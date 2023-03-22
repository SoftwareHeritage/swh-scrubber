# Copyright (C) 2022-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

from swh.model.swhids import ObjectType
from swh.scrubber.db import Datastore, ScrubberDb

DATASTORE = Datastore(package="storage", cls="postgresql", instance="service=swh-test")
OBJECT_TYPE = ObjectType.DIRECTORY
PARTITION_ID = 10
NB_PARTITIONS = 64
DATE = datetime.datetime(2022, 10, 4, 12, 1, 23, tzinfo=datetime.timezone.utc)


def test_checked_partition_insert(scrubber_db: ScrubberDb):
    scrubber_db.checked_partition_upsert(
        DATASTORE, OBJECT_TYPE, PARTITION_ID, NB_PARTITIONS, DATE
    )

    assert list(scrubber_db.checked_partition_iter(DATASTORE)) == [
        (OBJECT_TYPE, PARTITION_ID, NB_PARTITIONS, DATE)
    ]


def test_checked_partition_insert_two(scrubber_db: ScrubberDb):
    scrubber_db.checked_partition_upsert(
        DATASTORE, OBJECT_TYPE, PARTITION_ID, NB_PARTITIONS, DATE
    )
    scrubber_db.checked_partition_upsert(
        DATASTORE, ObjectType.SNAPSHOT, PARTITION_ID, NB_PARTITIONS, DATE
    )

    assert set(scrubber_db.checked_partition_iter(DATASTORE)) == {
        (OBJECT_TYPE, PARTITION_ID, NB_PARTITIONS, DATE),
        (ObjectType.SNAPSHOT, PARTITION_ID, NB_PARTITIONS, DATE),
    }


def test_checked_partition_update(scrubber_db: ScrubberDb):
    scrubber_db.checked_partition_upsert(
        DATASTORE, OBJECT_TYPE, PARTITION_ID, NB_PARTITIONS, DATE
    )

    date2 = DATE + datetime.timedelta(days=1)
    scrubber_db.checked_partition_upsert(
        DATASTORE, OBJECT_TYPE, PARTITION_ID, NB_PARTITIONS, date2
    )

    assert list(scrubber_db.checked_partition_iter(DATASTORE)) == [
        (OBJECT_TYPE, PARTITION_ID, NB_PARTITIONS, date2)
    ]

    date3 = DATE + datetime.timedelta(days=-1)
    scrubber_db.checked_partition_upsert(
        DATASTORE, OBJECT_TYPE, PARTITION_ID, NB_PARTITIONS, date3
    )

    assert list(scrubber_db.checked_partition_iter(DATASTORE)) == [
        (OBJECT_TYPE, PARTITION_ID, NB_PARTITIONS, date2)  # newest date wins
    ]


def test_checked_partition_get(scrubber_db: ScrubberDb):
    assert (
        scrubber_db.checked_partition_get_last_date(
            DATASTORE, OBJECT_TYPE, PARTITION_ID, NB_PARTITIONS
        )
        is None
    )

    scrubber_db.checked_partition_upsert(
        DATASTORE, OBJECT_TYPE, PARTITION_ID, NB_PARTITIONS, DATE
    )

    assert (
        scrubber_db.checked_partition_get_last_date(
            DATASTORE, OBJECT_TYPE, PARTITION_ID, NB_PARTITIONS
        )
        == DATE
    )
