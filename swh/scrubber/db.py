# Copyright (C) 2022-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import dataclasses
import datetime
import functools
from typing import Iterable, Iterator, List, Optional, Tuple

import psycopg2

from swh.core.db import BaseDb
from swh.model.swhids import CoreSWHID, ObjectType


def now():
    return datetime.datetime.now(tz=datetime.timezone.utc)


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


@dataclasses.dataclass(frozen=True)
class ConfigEntry:
    """Represents a datastore being scrubbed; eg. swh-storage or swh-journal."""

    name: str
    datastore_id: int
    object_type: str
    nb_partitions: int


class ScrubberDb(BaseDb):
    current_version = 6

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

    @functools.lru_cache(1000)
    def datastore_get(self, datastore_id: int) -> Datastore:
        """Returns a datastore's id. Raises :exc:`ValueError` if it does not exist."""
        with self.transaction() as cur:
            cur.execute(
                """
                    SELECT package, class, instance
                    FROM datastore
                    WHERE id=%s
                """,
                (datastore_id,),
            )
            res = cur.fetchone()
            if res is None:
                raise ValueError(f"No datastore with id {datastore_id}")
            (package, cls, instance) = res
            return Datastore(package=package, cls=cls, instance=instance)

    def config_add(
        self,
        name: Optional[str],
        datastore: Datastore,
        object_type: ObjectType,
        nb_partitions: int,
    ) -> int:
        """Creates a configuration entry (and potentially a datastore);

        Will fail if a config with same (datastore. object_type, nb_paritions)
        already exists.
        """

        datastore_id = self.datastore_get_or_add(datastore)
        if not name:
            name = (
                f"check_{object_type.name.lower()}_{nb_partitions}_"
                f"{datastore.package}_{datastore.cls}"
            )
        args = {
            "name": name,
            "datastore_id": datastore_id,
            "object_type": object_type.name.lower(),
            "nb_partitions": nb_partitions,
        }
        with self.transaction() as cur:
            cur.execute(
                """
                WITH inserted AS (
                    INSERT INTO check_config (name, datastore, object_type, nb_partitions)
                    VALUES (%(name)s, %(datastore_id)s, %(object_type)s, %(nb_partitions)s)
                    RETURNING id
                )
                SELECT id
                FROM inserted;
                """,
                args,
            )
            res = cur.fetchone()
            if res is None:
                raise ValueError(f"No config matching {args}")
            (id_,) = res
            return id_

    @functools.lru_cache(1000)
    def config_get(self, config_id: int) -> ConfigEntry:
        with self.transaction() as cur:
            cur.execute(
                """
                    SELECT name, datastore, object_type, nb_partitions
                    FROM check_config
                    WHERE id=%(config_id)s
                """,
                {
                    "config_id": config_id,
                },
            )
            res = cur.fetchone()
            if res is None:
                raise ValueError(f"No config with id {config_id}")
            (name, datastore_id, object_type, nb_partitions) = res
            return ConfigEntry(
                name=name,
                datastore_id=datastore_id,
                object_type=object_type,
                nb_partitions=nb_partitions,
            )

    def config_get_by_name(
        self,
        name: str,
        datastore: Optional[int] = None,
    ) -> Optional[int]:
        """Get the configuration entry for given name, if any"""
        query_parts = ["SELECT id FROM check_config WHERE "]
        where_parts = [" name = %s "]
        query_params = [name]
        if datastore:
            where_parts.append(" datastore = %s ")
            query_params.append(str(datastore))

        query_parts.append(" AND ".join(where_parts))
        query = "\n".join(query_parts)
        with self.transaction() as cur:
            cur.execute(query, query_params)
            if cur.rowcount:
                res = cur.fetchone()
                if res:
                    (id_,) = res
                    return id_
            return None

    def config_iter(self) -> Iterator[Tuple[int, ConfigEntry]]:
        with self.transaction() as cur:
            cur.execute(
                """
                    SELECT id, name, datastore, object_type, nb_partitions
                    FROM check_config
                """,
            )
            for row in cur:
                assert row is not None
                (id_, name, datastore_id, object_type, nb_partitions) = row
                yield (
                    id_,
                    ConfigEntry(
                        name=name,
                        datastore_id=datastore_id,
                        object_type=object_type,
                        nb_partitions=nb_partitions,
                    ),
                )

    ####################################
    # Checkpointing/progress tracking
    ####################################

    def checked_partition_iter_next(
        self,
        config_id: int,
    ) -> Iterator[int]:
        """Generates partitions to be checked for the given configuration

        At each iteration, look for the next "free" partition in the
        checked_partition, for the given config_id, reserve it and return its
        id.

        Reserving the partition means make sure there is a row in the table for
        this partition id with the start_date column set.

        To chose a "free" partition is to select either the smaller partition
        is for which the start_date is NULL, or the first partition id not yet
        in the table.

        Stops the iteration when the number of partitions for the config id is
        reached.

        """
        while True:
            start_time = now()
            with self.transaction() as cur:
                cur.execute(
                    """
                    WITH next AS (
                     SELECT min(partition_id) as pid
                      FROM checked_partition
                      WHERE config_id=%(config_id)s and start_date is NULL
                     UNION
                     SELECT coalesce(max(partition_id) + 1, 0) as pid
                      FROM checked_partition
                      WHERE config_id=%(config_id)s
                    )
                    INSERT INTO checked_partition(config_id, partition_id, start_date)
                      select %(config_id)s, min(pid), %(start_date)s from next
                      where pid is not NULL
                    ON CONFLICT (config_id, partition_id)
                    DO UPDATE
                      SET start_date = GREATEST(
                        checked_partition.start_date, EXCLUDED.start_date
                      )
                    RETURNING partition_id;
                    """,
                    {"config_id": config_id, "start_date": start_time},
                )
                res = cur.fetchone()
                assert res is not None
                (partition_id,) = res
                if partition_id >= self.config_get(config_id).nb_partitions:
                    self.conn.rollback()
                    return
            yield partition_id

    def checked_partition_reset(self, config_id: int, partition_id: int) -> bool:
        """
        Reset the partition, aka clear start_date and end_date
        """
        with self.transaction() as cur:
            cur.execute(
                """
                UPDATE checked_partition
                SET start_date=NULL, end_date=NULL
                WHERE config_id=%(config_id)s AND partition_id=%(partition_id)s
                """,
                {"config_id": config_id, "partition_id": partition_id},
            )
            return bool(cur.rowcount)

    def checked_partition_upsert(
        self,
        config_id: int,
        partition_id: int,
        date: Optional[datetime.datetime] = None,
    ) -> None:
        """
        Records in the database the given partition was last checked at the given date.
        """
        if date is None:
            date = now()

        with self.transaction() as cur:
            cur.execute(
                """
                UPDATE checked_partition
                SET end_date = GREATEST(%(date)s, end_date)
                WHERE config_id=%(config_id)s AND partition_id=%(partition_id)s
                """,
                {
                    "config_id": config_id,
                    "partition_id": partition_id,
                    "date": date,
                },
            )

    def checked_partition_get_last_date(
        self,
        config_id: int,
        partition_id: int,
    ) -> Optional[datetime.datetime]:
        """
        Returns the last date the given partition was checked in the given datastore,
        or :const:`None` if it was never checked.

        Currently, this matches partition id and number exactly, with no regard for
        partitions that contain or are contained by it.
        """
        with self.transaction() as cur:
            cur.execute(
                """
                SELECT end_date
                FROM checked_partition
                WHERE config_id=%s AND partition_id=%s
                """,
                (config_id, partition_id),
            )

            res = cur.fetchone()
            if res is None:
                return None
            else:
                (date,) = res
                return date

    def checked_partition_get_running(
        self,
        config_id: int,
    ) -> Iterator[Tuple[int, datetime.datetime]]:
        """Yields the partitions which are currently being checked; i.e. which have a
        start_date but no end_date.
        """
        with self.transaction() as cur:
            cur.execute(
                """
                SELECT partition_id, start_date
                FROM checked_partition
                WHERE config_id=%s AND end_date is NULL
                """,
                (config_id,),
            )

            for partition_id, start_date in cur:
                yield (partition_id, start_date)

    def checked_partition_get_stuck(
        self,
        config_id: int,
        since: Optional[datetime.timedelta] = None,
    ) -> Iterator[Tuple[int, datetime.datetime]]:
        """Yields the partitions which are currently running for more than `since`; if
        not set, automatically guess a reasonable delay from completed partitions.
        If no such a delay can be extracted, fall back to 1 hour.

        The heuristic for the automatic delay is 2x max(end_date-start_date)
        for the last 10 partitions checked.

        """
        with self.transaction() as cur:
            if since is None:
                cur.execute(
                    """
                    WITH delays as
                    (
                    SELECT end_date - start_date as delay
                    FROM checked_partition
                    WHERE config_id=%s AND end_date is not NULL
                    ORDER BY start_date DESC
                    LIMIT 10
                    )
                    SELECT 2*max(delay) from delays
                    """,
                    (config_id,),
                )
                res = cur.fetchone()
                assert res is not None
                (since,) = res
            if since is None:
                since = datetime.timedelta(hours=1)

            cur.execute(
                """
                SELECT partition_id, start_date
                FROM checked_partition
                WHERE config_id=%s AND end_date is NULL AND start_date < %s
                """,
                (config_id, now() - since),
            )

            for partition_id, start_date in cur:
                yield (partition_id, start_date)

    def checked_partition_iter(
        self, config_id: int
    ) -> Iterator[Tuple[int, int, datetime.datetime, Optional[datetime.datetime]]]:
        """Yields tuples of ``(partition_id, nb_partitions, start_date, end_date)``"""
        with self.transaction() as cur:
            cur.execute(
                """
                SELECT CP.partition_id, CC.nb_partitions, CP.start_date, CP.end_date
                FROM checked_partition as CP
                INNER JOIN check_config AS CC on (CC.id=CP.config_id)
                WHERE CC.id=%s
                """,
                (config_id,),
            )

            for row in cur:
                yield tuple(row)  # type: ignore[misc]

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
