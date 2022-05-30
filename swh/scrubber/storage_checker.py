# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Reads all objects in a swh-storage instance and recomputes their checksums."""

import contextlib
import dataclasses
import logging
from typing import Iterable, Union

from swh.journal.serializers import value_to_kafka
from swh.model.model import Directory, Release, Revision, Snapshot
from swh.storage import backfill
from swh.storage.interface import StorageInterface
from swh.storage.postgresql.storage import Storage as PostgresqlStorage

from .db import Datastore, ScrubberDb

logger = logging.getLogger(__name__)

ScrubbableObject = Union[Revision, Release, Snapshot, Directory]


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
            logger.info(
                "Processing %s range %s to %s",
                self.object_type,
                backfill._format_range_bound(range_start),
                backfill._format_range_bound(range_end),
            )

            objects = backfill.fetch(
                db, self.object_type, start=range_start, end=range_end
            )
            objects = list(objects)

            self.process_objects(objects)

    def process_objects(self, objects: Iterable[ScrubbableObject]):
        for object_ in objects:
            real_id = object_.compute_hash()
            if object_.id != real_id:
                self.db.corrupt_object_add(
                    object_.swhid(),
                    self.datastore_info(),
                    value_to_kafka(object_.to_dict()),
                )
