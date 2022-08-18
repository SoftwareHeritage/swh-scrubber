# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import dataclasses
import datetime
import functools
from typing import Iterable, Iterator, List, Optional

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


@dataclasses.dataclass(frozen=True)
class MissingObject:
    id: CoreSWHID
    datastore: Datastore
    first_occurrence: datetime.datetime


@dataclasses.dataclass(frozen=True)
class MissingObjectReference:
    missing_id: CoreSWHID
    reference_id: CoreSWHID
    datastore: Datastore
    first_occurrence: datetime.datetime


@dataclasses.dataclass(frozen=True)
class FixedObject:
    id: CoreSWHID
    object_: bytes
    method: str
    recovery_date: Optional[datetime.datetime] = None


class ScrubberDb(BaseDb):
    current_version = 3

    ####################################
    # Shared tables
    ####################################

    @functools.lru_cache(1000)
    def datastore_get_or_add(self, datastore: Datastore) -> int:
        """Creates a datastore if it does not exist, and returns its id."""
        with self.transaction() as cur:
            cur.execute(
                """
                WITH inserted AS (
                    INSERT INTO datastore (package, class, instance)
                    VALUES (%(package)s, %(cls)s, %(instance)s)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                )
                SELECT id
                FROM inserted
                UNION (
                    -- If the datastore already exists, we need to fetch its id
                    SELECT id
                    FROM datastore
                    WHERE
                        package=%(package)s
                        AND class=%(cls)s
                        AND instance=%(instance)s
                )
                LIMIT 1
                """,
                (dataclasses.asdict(datastore)),
            )
            res = cur.fetchone()
            assert res is not None
            (id_,) = res
            return id_

    ####################################
    # Inventory of objects with issues
    ####################################

    def corrupt_object_add(
        self,
        id: CoreSWHID,
        datastore: Datastore,
        serialized_object: bytes,
    ) -> None:
        datastore_id = self.datastore_get_or_add(datastore)
        with self.transaction() as cur:
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
        with self.transaction() as cur:
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

    def _corrupt_object_list_from_cursor(
        self, cur: psycopg2.extensions.cursor
    ) -> List[CorruptObject]:
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

    def corrupt_object_get(
        self,
        start_id: CoreSWHID,
        end_id: CoreSWHID,
        limit: int = 100,
    ) -> List[CorruptObject]:
        """Yields a page of records in the 'corrupt_object' table, ordered by id.

        Arguments:
            start_id: Only return objects after this id
            end_id: Only return objects before this id
            in_origin: An origin URL. If provided, only returns objects that may be
                found in the given origin
        """
        with self.transaction() as cur:
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
            return self._corrupt_object_list_from_cursor(cur)

    def corrupt_object_grab_by_id(
        self,
        cur: psycopg2.extensions.cursor,
        start_id: CoreSWHID,
        end_id: CoreSWHID,
        limit: int = 100,
    ) -> List[CorruptObject]:
        """Returns a page of records in the 'corrupt_object' table for a fixer,
        ordered by id

        These records are not already fixed (ie. do not have a corresponding entry
        in the 'fixed_object' table), and they are selected with an exclusive update
        lock.

        Arguments:
            start_id: Only return objects after this id
            end_id: Only return objects before this id
        """
        cur.execute(
            """
            SELECT
                co.id, co.first_occurrence, co.object,
                ds.package, ds.class, ds.instance
            FROM corrupt_object AS co
            INNER JOIN datastore AS ds ON (ds.id=co.datastore)
            WHERE
                co.id >= %(start_id)s
                AND co.id <= %(end_id)s
                AND NOT EXISTS (SELECT 1 FROM fixed_object WHERE fixed_object.id=co.id)
            ORDER BY co.id
            LIMIT %(limit)s
            FOR UPDATE SKIP LOCKED
            """,
            dict(
                start_id=str(start_id),
                end_id=str(end_id),
                limit=limit,
            ),
        )
        return self._corrupt_object_list_from_cursor(cur)

    def corrupt_object_grab_by_origin(
        self,
        cur: psycopg2.extensions.cursor,
        origin_url: str,
        start_id: Optional[CoreSWHID] = None,
        end_id: Optional[CoreSWHID] = None,
        limit: int = 100,
    ) -> List[CorruptObject]:
        """Returns a page of records in the 'corrupt_object' table for a fixer,
        ordered by id

        These records are not already fixed (ie. do not have a corresponding entry
        in the 'fixed_object' table), and they are selected with an exclusive update
        lock.

        Arguments:
            origin_url: only returns objects that may be found in the given origin
        """
        cur.execute(
            """
            SELECT
                co.id, co.first_occurrence, co.object,
                ds.package, ds.class, ds.instance
            FROM corrupt_object AS co
            INNER JOIN datastore AS ds ON (ds.id=co.datastore)
            INNER JOIN object_origin AS oo ON (oo.object_id=co.id)
            WHERE
                (co.id >= %(start_id)s OR %(start_id)s IS NULL)
                AND (co.id <= %(end_id)s OR %(end_id)s IS NULL)
                AND NOT EXISTS (SELECT 1 FROM fixed_object WHERE fixed_object.id=co.id)
                AND oo.origin_url=%(origin_url)s
            ORDER BY co.id
            LIMIT %(limit)s
            FOR UPDATE SKIP LOCKED
            """,
            dict(
                start_id=None if start_id is None else str(start_id),
                end_id=None if end_id is None else str(end_id),
                origin_url=origin_url,
                limit=limit,
            ),
        )
        return self._corrupt_object_list_from_cursor(cur)

    def missing_object_add(
        self,
        id: CoreSWHID,
        reference_ids: Iterable[CoreSWHID],
        datastore: Datastore,
    ) -> None:
        """
        Adds a "hole" to the inventory, ie. an object missing from a datastore
        that is referenced by an other object of the same datastore.

        If the missing object is already known to be missing by the scrubber database,
        this only records the reference (which can be useful to locate an origin
        to recover the object from).
        If that reference is already known too, this is a noop.

        Args:
            id: SWHID of the missing object (the hole)
            reference_id: SWHID of the object referencing the missing object
            datastore: representation of the swh-storage/swh-journal/... instance
              containing this hole
        """
        if not reference_ids:
            raise ValueError("reference_ids is empty")
        datastore_id = self.datastore_get_or_add(datastore)
        with self.transaction() as cur:
            cur.execute(
                """
                INSERT INTO missing_object (id, datastore)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                (str(id), datastore_id),
            )
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO missing_object_reference (missing_id, reference_id, datastore)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                [
                    (str(id), str(reference_id), datastore_id)
                    for reference_id in reference_ids
                ],
            )

    def missing_object_iter(self) -> Iterator[MissingObject]:
        """Yields all records in the 'missing_object' table."""
        with self.transaction() as cur:
            cur.execute(
                """
                SELECT
                    mo.id, mo.first_occurrence,
                    ds.package, ds.class, ds.instance
                FROM missing_object AS mo
                INNER JOIN datastore AS ds ON (ds.id=mo.datastore)
                """
            )

            for row in cur:
                (id, first_occurrence, ds_package, ds_class, ds_instance) = row
                yield MissingObject(
                    id=CoreSWHID.from_string(id),
                    first_occurrence=first_occurrence,
                    datastore=Datastore(
                        package=ds_package, cls=ds_class, instance=ds_instance
                    ),
                )

    def missing_object_reference_iter(
        self, missing_id: CoreSWHID
    ) -> Iterator[MissingObjectReference]:
        """Yields all records in the 'missing_object_reference' table."""
        with self.transaction() as cur:
            cur.execute(
                """
                SELECT
                    mor.reference_id, mor.first_occurrence,
                    ds.package, ds.class, ds.instance
                FROM missing_object_reference AS mor
                INNER JOIN datastore AS ds ON (ds.id=mor.datastore)
                WHERE mor.missing_id=%s
                """,
                (str(missing_id),),
            )

            for row in cur:
                (
                    reference_id,
                    first_occurrence,
                    ds_package,
                    ds_class,
                    ds_instance,
                ) = row
                yield MissingObjectReference(
                    missing_id=missing_id,
                    reference_id=CoreSWHID.from_string(reference_id),
                    first_occurrence=first_occurrence,
                    datastore=Datastore(
                        package=ds_package, cls=ds_class, instance=ds_instance
                    ),
                )

    ####################################
    # Issue resolution
    ####################################

    def object_origin_add(
        self, cur: psycopg2.extensions.cursor, swhid: CoreSWHID, origins: List[str]
    ) -> None:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO object_origin (object_id, origin_url)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            [(str(swhid), origin_url) for origin_url in origins],
        )

    def object_origin_get(self, after: str = "", limit: int = 1000) -> List[str]:
        """Returns origins with non-fixed corrupt objects, ordered by URL.

        Arguments:
            after: if given, only returns origins with an URL after this value
        """
        with self.transaction() as cur:
            cur.execute(
                """
                SELECT DISTINCT origin_url
                FROM object_origin
                WHERE
                    origin_url > %(after)s
                    AND object_id IN (
                        (SELECT id FROM corrupt_object)
                        EXCEPT (SELECT id FROM fixed_object)
                    )
                ORDER BY origin_url
                LIMIT %(limit)s
                """,
                dict(after=after, limit=limit),
            )

            return [origin_url for (origin_url,) in cur]

    def fixed_object_add(
        self, cur: psycopg2.extensions.cursor, fixed_objects: List[FixedObject]
    ) -> None:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO fixed_object (id, object, method)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            [
                (str(fixed_object.id), fixed_object.object_, fixed_object.method)
                for fixed_object in fixed_objects
            ],
        )

    def fixed_object_iter(self) -> Iterator[FixedObject]:
        with self.transaction() as cur:
            cur.execute("SELECT id, object, method, recovery_date FROM fixed_object")
            for (id, object_, method, recovery_date) in cur:
                yield FixedObject(
                    id=CoreSWHID.from_string(id),
                    object_=object_,
                    method=method,
                    recovery_date=recovery_date,
                )
