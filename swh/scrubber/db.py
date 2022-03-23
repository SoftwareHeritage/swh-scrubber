# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import dataclasses
import datetime
import functools
from typing import Iterator, List

import psycopg2

from swh.core.db import BaseDb
from swh.model.swhids import CoreSWHID


@dataclasses.dataclass(frozen=True)
class Datastore:
    """Represents a datastore being scrubbed; eg. swh-storage or swh-journal."""

    package: str
    """'storage', 'journal', or 'objstorage'."""
    cls: str
    """'postgresql'/'cassandra' for storage, 'kafka' for journal,
    'pathslicer'/'winery'/... for objstorage."""
    instance: str
    """Human readable string."""


@dataclasses.dataclass(frozen=True)
class CorruptObject:
    id: CoreSWHID
    datastore: Datastore
    first_occurrence: datetime.datetime
    object_: bytes


class ScrubberDb(BaseDb):
    current_version = 1

    @functools.lru_cache(1000)
    def datastore_get_or_add(self, datastore: Datastore) -> int:
        """Creates a datastore if it does not exist, and returns its id."""
        cur = self.cursor()
        cur.execute(
            """
            INSERT INTO datastore (package, class, instance)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING id
            """,
            (datastore.package, datastore.cls, datastore.instance),
        )
        (id_,) = cur.fetchone()
        return id_

    def corrupt_object_add(
        self, id: CoreSWHID, datastore: Datastore, serialized_object: bytes,
    ) -> None:
        datastore_id = self.datastore_get_or_add(datastore)
        cur = self.cursor()
        cur.execute(
            """
            INSERT INTO corrupt_object (id, datastore, object)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (str(id), datastore_id, serialized_object),
        )

    def corrupt_object_iter(self) -> Iterator[CorruptObject]:
        """Yields all records in the 'corrupt_object' table."""
        cur = self.cursor()
        cur.execute(
            """
            SELECT
                co.id, co.first_occurrence, co.object,
                ds.package, ds.class, ds.instance
            FROM corrupt_object AS co
            INNER JOIN datastore AS ds ON (ds.id=co.datastore)
            """
        )

        for row in cur:
            (id, first_occurrence, object_, ds_package, ds_class, ds_instance) = row
            yield CorruptObject(
                id=CoreSWHID.from_string(id),
                first_occurrence=first_occurrence,
                object_=object_,
                datastore=Datastore(
                    package=ds_package, cls=ds_class, instance=ds_instance
                ),
            )

    def corrupt_object_grab(
        self,
        cur,
        start_id: CoreSWHID = None,
        end_id: CoreSWHID = None,
        limit: int = 100,
    ) -> List[CorruptObject]:
        """Yields a page of records in the 'corrupt_object' table."""
        cur.execute(
            """
            SELECT
                co.id, co.first_occurrence, co.object,
                ds.package, ds.class, ds.instance
            FROM corrupt_object AS co
            INNER JOIN datastore AS ds ON (ds.id=co.datastore)
            WHERE
                co.id >= %s
                AND co.id <= %s
            ORDER BY co.id
            LIMIT %s
            """,
            (str(start_id), str(end_id), limit),
        )

        results = []
        for row in cur:
            (id, first_occurrence, object_, ds_package, ds_class, ds_instance) = row
            results.append(
                CorruptObject(
                    id=CoreSWHID.from_string(id),
                    first_occurrence=first_occurrence,
                    object_=object_,
                    datastore=Datastore(
                        package=ds_package, cls=ds_class, instance=ds_instance
                    ),
                )
            )

        return results

    def object_origin_add(self, cur, swhid: CoreSWHID, origins: List[str]) -> None:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO object_origin (object_id, origin_url)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            [(str(swhid), origin_url) for origin_url in origins],
        )
