# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Reads all objects in a swh-storage instance and recomputes their checksums."""

import collections
import contextlib
import dataclasses
import logging
from typing import Iterable, Union

from swh.core.statsd import Statsd
from swh.journal.serializers import value_to_kafka
from swh.model import swhids
from swh.model.model import (
    Content,
    Directory,
    ObjectType,
    Release,
    Revision,
    Snapshot,
    TargetType,
)
from swh.storage import backfill
from swh.storage.interface import StorageInterface
from swh.storage.postgresql.storage import Storage as PostgresqlStorage

from .db import Datastore, ScrubberDb

logger = logging.getLogger(__name__)

ScrubbableObject = Union[Revision, Release, Snapshot, Directory, Content]


@contextlib.contextmanager
def storage_db(storage):
    db = storage.get_db()
    try:
        yield db
    finally:
        storage.put_db(db)


@dataclasses.dataclass
class StorageChecker:
    """Reads a chunk of a swh-storage database, recomputes checksums, and
    reports errors in a separate database."""

    db: ScrubberDb
    storage: StorageInterface
    object_type: str
    """``directory``/``revision``/``release``/``snapshot``"""
    start_object: str
    """minimum value of the hexdigest of the object's sha1."""
    end_object: str
    """maximum value of the hexdigest of the object's sha1."""

    _datastore = None
    _statsd = None

    def datastore_info(self) -> Datastore:
        """Returns a :class:`Datastore` instance representing the swh-storage instance
        being checked."""
        if self._datastore is None:
            if isinstance(self.storage, PostgresqlStorage):
                with storage_db(self.storage) as db:
                    self._datastore = Datastore(
                        package="storage",
                        cls="postgresql",
                        instance=db.conn.dsn,
                    )
            else:
                raise NotImplementedError(
                    f"StorageChecker(storage={self.storage!r}).datastore()"
                )
        return self._datastore

    def statsd(self) -> Statsd:
        if self._statsd is None:
            self._statsd = Statsd(
                namespace="swh_scrubber",
                constant_tags={"object_type": self.object_type},
            )
        return self._statsd

    def run(self):
        """Runs on all objects of ``object_type`` and with id between
        ``start_object`` and ``end_object``.
        """
        if isinstance(self.storage, PostgresqlStorage):
            with storage_db(self.storage) as db:
                return self._check_postgresql(db)
        else:
            raise NotImplementedError(
                f"StorageChecker(storage={self.storage!r}).check_storage()"
            )

    def _check_postgresql(self, db):
        for range_start, range_end in backfill.RANGE_GENERATORS[self.object_type](
            self.start_object, self.end_object
        ):
            logger.debug(
                "Processing %s range %s to %s",
                self.object_type,
                backfill._format_range_bound(range_start),
                backfill._format_range_bound(range_end),
            )

            objects = backfill.fetch(
                db, self.object_type, start=range_start, end=range_end
            )
            objects = list(objects)

            with self.statsd().timed(
                "batch_duration_seconds", tags={"operation": "check_hashes"}
            ):
                self.check_object_hashes(objects)
            with self.statsd().timed(
                "batch_duration_seconds", tags={"operation": "check_references"}
            ):
                self.check_object_references(objects)

    def check_object_hashes(self, objects: Iterable[ScrubbableObject]):
        """Recomputes hashes, and reports mismatches."""
        count = 0
        for object_ in objects:
            if isinstance(object_, Content):
                # TODO
                continue
            real_id = object_.compute_hash()
            count += 1
            if object_.id != real_id:
                self.statsd().increment("hash_mismatch_total")
                self.db.corrupt_object_add(
                    object_.swhid(),
                    self.datastore_info(),
                    value_to_kafka(object_.to_dict()),
                )
        if count:
            self.statsd().increment("objects_hashed_total", count)

    def check_object_references(self, objects: Iterable[ScrubbableObject]):
        """Check all objects references by these objects exist."""
        cnt_references = collections.defaultdict(set)
        dir_references = collections.defaultdict(set)
        rev_references = collections.defaultdict(set)
        rel_references = collections.defaultdict(set)
        snp_references = collections.defaultdict(set)

        for object_ in objects:
            swhid = object_.swhid()

            if isinstance(object_, Content):
                pass
            elif isinstance(object_, Directory):
                for entry in object_.entries:
                    if entry.type == "file":
                        cnt_references[entry.target].add(swhid)
                    elif entry.type == "dir":
                        dir_references[entry.target].add(swhid)
                    elif entry.type == "rev":
                        # dir->rev holes are not considered a problem because they
                        # happen whenever git submodules point to repositories that
                        # were not loaded yet; ignore them
                        pass
                    else:
                        assert False, entry
            elif isinstance(object_, Revision):
                dir_references[object_.directory].add(swhid)
                for parent in object_.parents:
                    rev_references[parent].add(swhid)
            elif isinstance(object_, Release):
                if object_.target is None:
                    pass
                elif object_.target_type == ObjectType.CONTENT:
                    cnt_references[object_.target].add(swhid)
                elif object_.target_type == ObjectType.DIRECTORY:
                    dir_references[object_.target].add(swhid)
                elif object_.target_type == ObjectType.REVISION:
                    rev_references[object_.target].add(swhid)
                elif object_.target_type == ObjectType.RELEASE:
                    rel_references[object_.target].add(swhid)
                else:
                    assert False, object_
            elif isinstance(object_, Snapshot):
                for branch in object_.branches.values():
                    if branch is None:
                        pass
                    elif branch.target_type == TargetType.ALIAS:
                        pass
                    elif branch.target_type == TargetType.CONTENT:
                        cnt_references[branch.target].add(swhid)
                    elif branch.target_type == TargetType.DIRECTORY:
                        dir_references[branch.target].add(swhid)
                    elif branch.target_type == TargetType.REVISION:
                        rev_references[branch.target].add(swhid)
                    elif branch.target_type == TargetType.RELEASE:
                        rel_references[branch.target].add(swhid)
                    elif branch.target_type == TargetType.SNAPSHOT:
                        snp_references[branch.target].add(swhid)
                    else:
                        assert False, (str(object_.swhid()), branch)
            else:
                assert False, object_.swhid()

        missing_cnts = set(
            self.storage.content_missing_per_sha1_git(list(cnt_references))
        )
        missing_dirs = set(self.storage.directory_missing(list(dir_references)))
        missing_revs = set(self.storage.revision_missing(list(rev_references)))
        missing_rels = set(self.storage.release_missing(list(rel_references)))
        missing_snps = set(self.storage.snapshot_missing(list(snp_references)))

        self.statsd().increment(
            "missing_object_total",
            len(missing_cnts),
            tags={"target_object_type": "content"},
        )
        self.statsd().increment(
            "missing_object_total",
            len(missing_dirs),
            tags={"target_object_type": "directory"},
        )
        self.statsd().increment(
            "missing_object_total",
            len(missing_revs),
            tags={"target_object_type": "revision"},
        )
        self.statsd().increment(
            "missing_object_total",
            len(missing_rels),
            tags={"target_object_type": "release"},
        )
        self.statsd().increment(
            "missing_object_total",
            len(missing_snps),
            tags={"target_object_type": "snapshot"},
        )

        for missing_id in missing_cnts:
            missing_swhid = swhids.CoreSWHID(
                object_type=swhids.ObjectType.CONTENT, object_id=missing_id
            )
            self.db.missing_object_add(
                missing_swhid, cnt_references[missing_id], self.datastore_info()
            )

        for missing_id in missing_dirs:
            missing_swhid = swhids.CoreSWHID(
                object_type=swhids.ObjectType.DIRECTORY, object_id=missing_id
            )
            self.db.missing_object_add(
                missing_swhid, dir_references[missing_id], self.datastore_info()
            )

        for missing_id in missing_revs:
            missing_swhid = swhids.CoreSWHID(
                object_type=swhids.ObjectType.REVISION, object_id=missing_id
            )
            self.db.missing_object_add(
                missing_swhid, rev_references[missing_id], self.datastore_info()
            )

        for missing_id in missing_rels:
            missing_swhid = swhids.CoreSWHID(
                object_type=swhids.ObjectType.RELEASE, object_id=missing_id
            )
            self.db.missing_object_add(
                missing_swhid, rel_references[missing_id], self.datastore_info()
            )

        for missing_id in missing_snps:
            missing_swhid = swhids.CoreSWHID(
                object_type=swhids.ObjectType.SNAPSHOT, object_id=missing_id
            )
            self.db.missing_object_add(
                missing_swhid, snp_references[missing_id], self.datastore_info()
            )
