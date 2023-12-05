# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Lists corrupt objects in the scrubber database, and lists candidate origins
to recover them from."""

import dataclasses
import itertools
import logging
from typing import Iterable, Union

import psycopg2

from swh.core.utils import grouper
from swh.graph.http_client import GraphArgumentException, RemoteGraphClient
from swh.model.model import Directory, Release, Revision, Snapshot
from swh.model.swhids import CoreSWHID, ExtendedSWHID
from swh.storage.interface import StorageInterface

from .db import CorruptObject, ScrubberDb
from .utils import iter_corrupt_objects

logger = logging.getLogger(__name__)

ScrubbableObject = Union[Revision, Release, Snapshot, Directory]


def get_origins(
    graph: RemoteGraphClient, storage: StorageInterface, swhid: CoreSWHID
) -> Iterable[str]:
    try:
        origin_swhids = [
            ExtendedSWHID.from_string(line)
            for line in graph.leaves(str(swhid), direction="backward")
            if line.startswith("swh:1:ori:")
        ]
    except GraphArgumentException:
        return

    for origin_swhid_group in grouper(origin_swhids, 10):
        origin_swhid_group = list(origin_swhid_group)
        for origin, origin_swhid in zip(
            storage.origin_get_by_sha1(
                [origin_swhid.object_id for origin_swhid in origin_swhid_group]
            ),
            origin_swhid_group,
        ):
            if origin is None:
                logger.error("%s found in graph but missing from storage", origin_swhid)
            else:
                yield origin["url"]


@dataclasses.dataclass
class OriginLocator:
    """Reads a chunk of corrupt objects in the swh-scrubber database, then writes
    to the same database a list of origins they might be recovered from."""

    db: ScrubberDb
    """Database to read from and write to."""
    graph: RemoteGraphClient
    storage: StorageInterface
    """Used to resolve origin SHA1s to URLs."""

    start_object: CoreSWHID
    """Minimum SWHID to check (in alphabetical order)"""
    end_object: CoreSWHID
    """Maximum SWHID to check (in alphabetical order)"""

    def run(self):
        iter_corrupt_objects(
            self.db,
            self.start_object,
            self.end_object,
            None,
            self.handle_corrupt_object,
        )

    def handle_corrupt_object(
        self, corrupt_object: CorruptObject, cur: psycopg2.extensions.cursor
    ) -> None:
        origins = get_origins(self.graph, self.storage, corrupt_object.id)

        # Keep only 100 origins, to avoid flooding the DB.
        # It is very unlikely an object disappred from 100 somwhat-randomly sampled
        # origins.
        first_origins = list(itertools.islice(origins, 0, 100))

        self.db.object_origin_add(cur, corrupt_object.id, first_origins)
