# Copyright (C) 2021-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Reads all objects in a swh-storage instance and recomputes their checksums."""

import collections
import contextlib
import dataclasses
import datetime
import json
import logging
from typing import Iterable, Optional, Tuple, Union

import psycopg2
import tenacity

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
from swh.storage.algos.directory import (
    directory_get_many_with_possibly_duplicated_entries,
)
from swh.storage.algos.snapshot import snapshot_get_all_branches
from swh.storage.cassandra.storage import CassandraStorage
from swh.storage.interface import StorageInterface
from swh.storage.postgresql.storage import Storage as PostgresqlStorage

from .db import Datastore, ScrubberDb

logger = logging.getLogger(__name__)

ScrubbableObject = Union[Revision, Release, Snapshot, Directory, Content]


@contextlib.contextmanager
def postgresql_storage_db(storage):
    db = storage.get_db()
    try:
        yield db
    finally:
        storage.put_db(db)


def _get_inclusive_range_swhids(
    inclusive_range_start: Optional[bytes],
    exclusive_range_end: Optional[bytes],
    object_type: swhids.ObjectType,
) -> Tuple[swhids.CoreSWHID, swhids.CoreSWHID]:
    r"""
    Given a ``[range_start, range_end)`` right-open interval of id prefixes
    and an object type (as returned by :const:`swh.storage.backfill.RANGE_GENERATORS`),
    returns a ``[range_start_swhid, range_end_swhid]`` closed interval of SWHIDs
    suitable for the scrubber database.

    >>> _get_inclusive_range_swhids(b"\x42", None, swhids.ObjectType.SNAPSHOT)
    (CoreSWHID.from_string('swh:1:snp:4200000000000000000000000000000000000000'), CoreSWHID.from_string('swh:1:snp:ffffffffffffffffffffffffffffffffffffffff'))

    >>> _get_inclusive_range_swhids(b"\x00", b"\x12\x34", swhids.ObjectType.REVISION)
    (CoreSWHID.from_string('swh:1:rev:0000000000000000000000000000000000000000'), CoreSWHID.from_string('swh:1:rev:1233ffffffffffffffffffffffffffffffffffff'))

    """  # noqa
    range_start_swhid = swhids.CoreSWHID(
        object_type=object_type,
        object_id=(inclusive_range_start or b"").ljust(20, b"\00"),
    )
    if exclusive_range_end is None:
        inclusive_range_end = b"\xff" * 20
    else:
        # convert "1230000000..." to "122fffffff..."
        inclusive_range_end = (
            int.from_bytes(exclusive_range_end.ljust(20, b"\x00"), "big") - 1
        ).to_bytes(20, "big")
    range_end_swhid = swhids.CoreSWHID(
        object_type=object_type,
        object_id=inclusive_range_end,
    )

    return (range_start_swhid, range_end_swhid)


@dataclasses.dataclass
class StorageChecker:
    """Reads a chunk of a swh-storage database, recomputes checksums, and
    reports errors in a separate database."""

    db: ScrubberDb
    storage: StorageInterface
    object_type: str
    """``directory``/``revision``/``release``/``snapshot``"""
    nb_partitions: int
    """Number of partitions to split the whole set of objects into.
    Must be a power of 2."""
    start_partition_id: int
    """First partition id to check (inclusive). Must be in the range [0, nb_partitions).
    """
    end_partition_id: int
    """Last partition id to check (exclusive). Must be in the range
    (start_partition_id, nb_partitions]"""

    _datastore = None
    _statsd = None

    def datastore_info(self) -> Datastore:
        """Returns a :class:`Datastore` instance representing the swh-storage instance
        being checked."""
        if self._datastore is None:
            if isinstance(self.storage, PostgresqlStorage):
                with postgresql_storage_db(self.storage) as db:
                    self._datastore = Datastore(
                        package="storage",
                        cls="postgresql",
                        instance=db.conn.dsn,
                    )
            elif isinstance(self.storage, CassandraStorage):
                self._datastore = Datastore(
                    package="storage",
                    cls="cassandra",
                    instance=json.dumps(
                        {
                            "keyspace": self.storage.keyspace,
                            "hosts": self.storage.hosts,
                            "port": self.storage.port,
                        }
                    ),
                )
            else:
                raise NotImplementedError(
                    f"StorageChecker(storage={self.storage!r}).datastore()"
                )
        return self._datastore

    def statsd(self) -> Statsd:
        if self._statsd is None:
            datastore = self.datastore_info()
            self._statsd = Statsd(
                namespace="swh_scrubber",
                constant_tags={
                    "object_type": self.object_type,
                    "nb_partitions": self.nb_partitions,
                    "datastore_package": datastore.package,
                    "datastore_cls": datastore.cls,
                },
            )
        return self._statsd

    def run(self) -> None:
        """Runs on all objects of ``object_type`` in a partition between
        ``start_partition_id`` (inclusive) and ``end_partition_id`` (exclusive)
        """
        object_type = getattr(swhids.ObjectType, self.object_type.upper())
        for partition_id in range(self.start_partition_id, self.end_partition_id):

            start_time = datetime.datetime.now(tz=datetime.timezone.utc)

            # Currently, this matches range boundaries exactly, with no regard for
            # ranges that contain or are contained by it.
            last_check_time = self.db.checked_partition_get_last_date(
                self.datastore_info(), object_type, partition_id, self.nb_partitions
            )

            if last_check_time is not None:
                # TODO: re-check if 'last_check_time' was a long ago.
                logger.debug(
                    "Skipping processing of %s partition %d/%d: already done at %s",
                    self.object_type,
                    partition_id,
                    self.nb_partitions,
                    last_check_time,
                )
                continue

            logger.debug(
                "Processing %s partition %d/%d",
                self.object_type,
                partition_id,
                self.nb_partitions,
            )

            self._check_partition(object_type, partition_id)

            self.db.checked_partition_upsert(
                self.datastore_info(),
                object_type,
                partition_id,
                self.nb_partitions,
                start_time,
            )

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(psycopg2.OperationalError),
        wait=tenacity.wait_random_exponential(min=10, max=180),
    )
    def _check_partition(
        self, object_type: swhids.ObjectType, partition_id: int
    ) -> None:
        page_token = None
        while True:
            if object_type in (swhids.ObjectType.RELEASE, swhids.ObjectType.REVISION):
                method = getattr(self.storage, f"{self.object_type}_get_partition")
                page = method(partition_id, self.nb_partitions, page_token=page_token)
                objects = page.results
            elif object_type == swhids.ObjectType.DIRECTORY:
                page = self.storage.directory_get_id_partition(
                    partition_id, self.nb_partitions, page_token=page_token
                )
                directory_ids = page.results
                objects = []
                for (dir_id, item) in zip(
                    directory_ids,
                    directory_get_many_with_possibly_duplicated_entries(
                        self.storage, directory_ids
                    ),
                ):
                    assert item is not None, f"Directory {dir_id.hex()} disappeared"
                    (has_duplicate_entries, object_) = item
                    if has_duplicate_entries:
                        self.statsd().increment("duplicate_directory_entries_total")
                        self.db.corrupt_object_add(
                            object_.swhid(),
                            self.datastore_info(),
                            value_to_kafka(object_.to_dict()),
                        )
                    objects.append(object_)
            elif object_type == swhids.ObjectType.SNAPSHOT:
                page = self.storage.snapshot_get_id_partition(
                    partition_id, self.nb_partitions, page_token=page_token
                )
                snapshot_ids = page.results
                objects = [
                    snapshot_get_all_branches(self.storage, snapshot_id)
                    for snapshot_id in snapshot_ids
                ]
            else:
                assert False, f"Unexpected object type: {object_type}"

            with self.statsd().timed(
                "batch_duration_seconds", tags={"operation": "check_hashes"}
            ):
                self.check_object_hashes(objects)
            with self.statsd().timed(
                "batch_duration_seconds", tags={"operation": "check_references"}
            ):
                self.check_object_references(objects)

            page_token = page.next_page_token
            if page_token is None:
                break

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
