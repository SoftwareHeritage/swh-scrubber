# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Reads all known corrupts objects from the swh-scrubber database,
and tries to recover them.

Currently, only recovery from Git origins is implemented"""

import dataclasses
import functools
import logging
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Dict, Optional, Type, Union

import dulwich
import dulwich.objects
import dulwich.repo
import psycopg2

from swh.journal.serializers import kafka_to_value, value_to_kafka
from swh.loader.git import converters
from swh.model.hashutil import hash_to_bytehex, hash_to_hex
from swh.model.model import BaseModel, Directory, Release, Revision, Snapshot
from swh.model.swhids import CoreSWHID, ObjectType

from .db import CorruptObject, FixedObject, ScrubberDb
from .utils import iter_corrupt_objects

logger = logging.getLogger(__name__)

ScrubbableObject = Union[Revision, Release, Snapshot, Directory]


def get_object_from_clone(
    clone_path: Path, swhid: CoreSWHID
) -> Union[None, bytes, dulwich.objects.ShaFile]:
    """Reads the original object matching the ``corrupt_object`` from the given clone
    if it exists, and returns a Dulwich object if possible, or a the raw manifest."""
    try:
        repo = dulwich.repo.Repo(str(clone_path))
    except dulwich.errors.NotGitRepository:
        return None

    with repo:  # needed to avoid packfile fd leaks
        try:
            return repo[hash_to_bytehex(swhid.object_id)]
        except KeyError:
            return None
        except dulwich.errors.ObjectFormatException:
            # fallback to git if dulwich can't parse it.
            # Unfortunately, Dulwich does not allow fetching an object without
            # parsing it into a ShaFile subclass, so we have to manually get it
            # by shelling out to git.
            object_type = (
                subprocess.check_output(
                    [
                        "git",
                        "-C",
                        clone_path,
                        "cat-file",
                        "-t",
                        hash_to_hex(swhid.object_id),
                    ]
                )
                .decode()
                .strip()
            )
            manifest = subprocess.check_output(
                [
                    "git",
                    "-C",
                    clone_path,
                    "cat-file",
                    object_type,
                    hash_to_hex(swhid.object_id),
                ]
            )
            manifest = f"{object_type} {len(manifest)}\x00".encode() + manifest
            logger.info("Dulwich failed to parse %r", manifest)
            return manifest


def get_fixed_object_from_clone(
    clone_path: Path, corrupt_object: CorruptObject
) -> Optional[FixedObject]:
    """Reads the original object matching the ``corrupt_object`` from the given clone
    if it exists, and returns a :class:`FixedObject` instance ready to be inserted
    in the database."""
    cloned_dulwich_obj_or_manifest = get_object_from_clone(
        clone_path, corrupt_object.id
    )
    if cloned_dulwich_obj_or_manifest is None:
        # Origin still exists, but object disappeared
        logger.info("%s not found in origin", corrupt_object.id)
        return None
    elif isinstance(cloned_dulwich_obj_or_manifest, bytes):
        # Dulwich could not parse it. Add as raw manifest to the existing object
        d = kafka_to_value(corrupt_object.object_)
        assert d.get("raw_manifest") is None, "Corrupt object has a raw_manifest"
        d["raw_manifest"] = cloned_dulwich_obj_or_manifest

        # Rebuild the object from the stored corrupt object + the raw manifest
        # just recovered; then checksum it.
        classes: Dict[ObjectType, Type[BaseModel]] = {
            ObjectType.REVISION: Revision,
            ObjectType.DIRECTORY: Directory,
            ObjectType.RELEASE: Release,
        }
        cls = classes[corrupt_object.id.object_type]
        recovered_obj = cls.from_dict(d)
        recovered_obj.check()

        return FixedObject(
            id=corrupt_object.id,
            object_=value_to_kafka(d),
            method="manifest_from_origin",
        )
    else:
        converter = {
            ObjectType.REVISION: converters.dulwich_commit_to_revision,
            ObjectType.DIRECTORY: converters.dulwich_tree_to_directory,
            ObjectType.RELEASE: converters.dulwich_tag_to_release,
        }[corrupt_object.id.object_type]
        cloned_obj = converter(cloned_dulwich_obj_or_manifest)

        # Check checksum, among others
        cloned_obj.check()

        return FixedObject(
            id=corrupt_object.id,
            object_=value_to_kafka(cloned_obj.to_dict()),
            method="from_origin",
        )


@dataclasses.dataclass
class Fixer:
    """Reads a chunk of corrupt objects in the swh-scrubber database, tries to recover
    them through various means (brute-forcing fields and re-downloading from the origin)
    recomputes checksums, and writes them back to the swh-scrubber database
    if successful.

    """

    db: ScrubberDb
    """Database to read from and write to."""
    start_object: CoreSWHID = CoreSWHID.from_string("swh:1:cnt:" + "00" * 20)
    """Minimum SWHID to check (in alphabetical order)"""
    end_object: CoreSWHID = CoreSWHID.from_string("swh:1:snp:" + "ff" * 20)
    """Maximum SWHID to check (in alphabetical order)"""

    def run(self):
        # TODO: currently only support re-downloading from the origin:
        # we should try brute-forcing for objects with no known origin (or when
        # all origins fail)
        after = ""
        while True:
            new_origins = self.db.object_origin_get(after=after)
            if not new_origins:
                break
            for origin_url in new_origins:
                self.recover_objects_from_origin(origin_url)
            after = new_origins[-1]

    def recover_objects_from_origin(self, origin_url):
        """Clones an origin, and cherry-picks original objects that are known to be
        corrupt in the database."""
        with tempfile.TemporaryDirectory(prefix=__name__ + ".") as tempdir:
            clone_path = Path(tempdir) / "repository.git"
            try:
                subprocess.run(
                    ["git", "clone", "--bare", origin_url, clone_path],
                    env={"PATH": os.environ["PATH"], "GIT_TERMINAL_PROMPT": "0"},
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    stdin=subprocess.DEVNULL,
                )
            except Exception:
                logger.exception("Failed to clone %s", origin_url)
                return

            iter_corrupt_objects(
                self.db,
                self.start_object,
                self.end_object,
                origin_url,
                functools.partial(self.recover_corrupt_object, clone_path=clone_path),
            )

    def recover_corrupt_object(
        self,
        corrupt_object: CorruptObject,
        cur: psycopg2.extensions.cursor,
        clone_path: Path,
    ) -> None:
        fixed_object = get_fixed_object_from_clone(clone_path, corrupt_object)

        if fixed_object is not None:
            self.db.fixed_object_add(cur, [fixed_object])
