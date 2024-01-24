# Copyright (C) 2022-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from unittest.mock import patch

from psycopg2.errors import UniqueViolation
import pytest

from swh.model.swhids import ObjectType
from swh.scrubber.db import Datastore, ScrubberDb

from .conftest import NB_PARTITIONS, OBJECT_TYPE

DATE = datetime.datetime(2022, 10, 4, 12, 1, 23, tzinfo=datetime.timezone.utc)
ONE_MINUTE = datetime.timedelta(minutes=1)
ONE_DAY = datetime.timedelta(days=1)


def test_config_add(datastore: Datastore, scrubber_db: ScrubberDb, config_id: int):
    cfg_snp = scrubber_db.config_add("cfg snp", datastore, ObjectType.SNAPSHOT, 42)
    assert cfg_snp == 2

    cfg_snp2 = scrubber_db.config_add("cfg snp 2", datastore, ObjectType.SNAPSHOT, 43)
    assert cfg_snp2 == 3

    # if not given, a config name is computed
    cfg_snp3 = scrubber_db.config_add(None, datastore, ObjectType.SNAPSHOT, 44)
    assert cfg_snp3 == 4
    assert (
        scrubber_db.config_get(cfg_snp3).name == "check_snapshot_44_storage_postgresql"
    )

    # XXX this is debatable: is there a good reason to allow 2 configs for the
    # same datastore and object type but different partition number, but not
    # for the same number of partitions?
    with pytest.raises(UniqueViolation):
        scrubber_db.config_add("cfg4", datastore, OBJECT_TYPE, NB_PARTITIONS)


def test_config_add_flags(
    datastore: Datastore, scrubber_db: ScrubberDb, config_id: int
):
    id_cfg2 = scrubber_db.config_add("cfg snp", datastore, ObjectType.SNAPSHOT, 42)
    assert id_cfg2 == 2
    id_cfg3 = scrubber_db.config_add(
        "cfg3", datastore, ObjectType.SNAPSHOT, 43, check_hashes=False
    )
    assert id_cfg3 == 3
    id_cfg4 = scrubber_db.config_add(
        "cfg4", datastore, ObjectType.SNAPSHOT, 43, check_references=False
    )
    assert id_cfg4 == 4

    # but cannot add another config entry with the same name, ds, objtype and part
    # number but different flags
    with pytest.raises(UniqueViolation):
        scrubber_db.config_add(
            "cfg4", datastore, ObjectType.SNAPSHOT, 43, check_hashes=False
        )

    with pytest.raises(ValueError):
        scrubber_db.config_add(
            "cfg4",
            datastore,
            OBJECT_TYPE,
            NB_PARTITIONS,
            check_hashes=False,
            check_references=False,
        )


def test_config_get(datastore: Datastore, scrubber_db: ScrubberDb, config_id: int):
    id_cfg2 = scrubber_db.config_add("cfg2", datastore, ObjectType.SNAPSHOT, 42)
    id_cfg3 = scrubber_db.config_add(
        "cfg3", datastore, ObjectType.SNAPSHOT, 43, False, True
    )
    id_cfg4 = scrubber_db.config_add(
        "cfg4", datastore, ObjectType.SNAPSHOT, 43, True, False
    )

    cfg2 = scrubber_db.config_get(id_cfg2)
    assert cfg2
    assert cfg2.check_hashes is True
    assert cfg2.check_references is True
    cfg3 = scrubber_db.config_get(id_cfg3)
    assert cfg3
    assert cfg3.check_hashes is False
    assert cfg3.check_references is True
    cfg4 = scrubber_db.config_get(id_cfg4)
    assert cfg4
    assert cfg4.check_hashes is True
    assert cfg4.check_references is False

    with pytest.raises(ValueError):
        scrubber_db.config_get(id_cfg4 + 1)


@pytest.fixture
def datastore2():
    return Datastore(package="storage", cls="postgresql", instance="service=swh-test-2")


@pytest.fixture
def config_id2(scrubber_db, datastore2):
    return scrubber_db.config_add(
        f"cfg_{OBJECT_TYPE}_{NB_PARTITIONS}", datastore2, OBJECT_TYPE, NB_PARTITIONS
    )


def test_checked_config_get_by_name(
    datastore: Datastore,
    datastore2: Datastore,
    config_id: int,
    config_id2: int,
    scrubber_db: ScrubberDb,
):
    assert datastore == scrubber_db.datastore_get(config_id)
    assert datastore2 == scrubber_db.datastore_get(config_id2)

    cfg2 = scrubber_db.config_add("cfg2", datastore, ObjectType.SNAPSHOT, 42)
    cfg2prime = scrubber_db.config_add("cfg2", datastore2, ObjectType.SNAPSHOT, 42)
    cfg3 = scrubber_db.config_add("cfg3", datastore, ObjectType.SNAPSHOT, 43)

    assert scrubber_db.config_get_by_name("cfg2") == cfg2
    assert scrubber_db.config_get_by_name("cfg3") == cfg3

    # Check for unknown configuration
    assert scrubber_db.config_get_by_name("unknown config") is None

    # Check for duplicated configurations
    assert scrubber_db.config_get_by_name("cfg2", config_id) == cfg2
    assert scrubber_db.config_get_by_name("cfg2", config_id2) == cfg2prime


def test_datastore_get(datastore: Datastore, scrubber_db: ScrubberDb, config_id: int):
    assert scrubber_db.datastore_get(1) == datastore
    with pytest.raises(ValueError):
        scrubber_db.datastore_get(42)


def test_checked_partition_insert(
    datastore: Datastore, scrubber_db: ScrubberDb, config_id: int
):
    with patch("swh.scrubber.db.now", return_value=DATE):
        part_gen = scrubber_db.checked_partition_iter_next(config_id)
        partition_id = next(part_gen)
    scrubber_db.checked_partition_upsert(config_id, partition_id, DATE + ONE_MINUTE)

    assert list(scrubber_db.checked_partition_iter(config_id)) == [
        (partition_id, NB_PARTITIONS, DATE, DATE + ONE_MINUTE)
    ]


def test_checked_partition_insert_two(
    datastore: Datastore, scrubber_db: ScrubberDb, config_id: int
):
    with patch("swh.scrubber.db.now", return_value=DATE):
        part_gen = scrubber_db.checked_partition_iter_next(config_id)
        part_id = next(part_gen)
        scrubber_db.checked_partition_upsert(config_id, part_id, DATE + ONE_MINUTE)

        config_snp = scrubber_db.config_add("cfg2", datastore, ObjectType.SNAPSHOT, 42)
        snp_part_gen = scrubber_db.checked_partition_iter_next(config_snp)
        snp_part_id = next(snp_part_gen)
        scrubber_db.checked_partition_upsert(config_snp, snp_part_id, DATE + ONE_MINUTE)

    assert set(scrubber_db.checked_partition_iter(config_id)) == {
        (part_id, NB_PARTITIONS, DATE, DATE + ONE_MINUTE),
    }
    assert set(scrubber_db.checked_partition_iter(config_snp)) == {
        (snp_part_id, 42, DATE, DATE + ONE_MINUTE),
    }


def test_checked_partition_get_next(
    datastore: Datastore, scrubber_db: ScrubberDb, config_id: int
):
    config_snp = scrubber_db.config_add("cfg2", datastore, ObjectType.SNAPSHOT, 42)
    snp_part_gen = scrubber_db.checked_partition_iter_next(config_snp)
    dir_part_gen = scrubber_db.checked_partition_iter_next(config_id)

    assert next(dir_part_gen) == 0
    assert next(snp_part_gen) == 0
    assert next(dir_part_gen) == 1
    assert next(dir_part_gen) == 2
    assert next(snp_part_gen) == 1
    assert next(snp_part_gen) == 2
    assert next(dir_part_gen) == 3

    date = datetime.datetime.now(tz=datetime.timezone.utc)
    scrubber_db.checked_partition_upsert(config_snp, 0, date)
    scrubber_db.checked_partition_upsert(config_id, 2, date)

    assert next(snp_part_gen) == 3
    assert next(dir_part_gen) == 4

    # iterate on all 64 possible partitions for the config config_id (5 of them
    # are already affected, so 59 to go)
    for i in range(59):
        assert next(dir_part_gen) == (i + 5)
    # we should be at the end of the partitions now...
    with pytest.raises(StopIteration):
        next(dir_part_gen)
    # check we still do not get anything after 63 with a new iterator
    assert list(scrubber_db.checked_partition_iter_next(config_id)) == []
    # check the database is OK with that
    with scrubber_db.transaction() as cur:
        cur.execute(
            "select max(partition_id) from checked_partition where config_id=%s",
            (config_id,),
        )
        assert cur.fetchone() == (63,)


def test_checked_partition_get_next_with_hole(
    datastore: Datastore, scrubber_db: ScrubberDb, config_id: int
):
    dir_part_gen = scrubber_db.checked_partition_iter_next(config_id)

    # fill the checked_partition table
    list(zip(range(20), dir_part_gen))

    # one hole at a time
    for part_id in range(10):
        assert scrubber_db.checked_partition_reset(config_id, part_id)
        assert next(dir_part_gen) == part_id

    # a series of holes
    for part_id in range(0, 10, 2):
        assert scrubber_db.checked_partition_reset(config_id, part_id)

    for i in range(5):
        assert next(dir_part_gen) == 2 * i

    # all the holes are filled, next partition is 20
    assert next(dir_part_gen) == 20


def test_checked_partition_update(
    datastore: Datastore, scrubber_db: ScrubberDb, config_id: int
):
    with patch("swh.scrubber.db.now", return_value=DATE):
        dir_part_gen = scrubber_db.checked_partition_iter_next(config_id)
        part_id = next(dir_part_gen)
    scrubber_db.checked_partition_upsert(config_id, part_id, DATE + ONE_MINUTE)

    date2 = DATE + 2 * ONE_MINUTE
    scrubber_db.checked_partition_upsert(config_id, part_id, date2)

    assert list(scrubber_db.checked_partition_iter(config_id)) == [
        (part_id, NB_PARTITIONS, DATE, date2)
    ]

    date3 = DATE - ONE_MINUTE
    scrubber_db.checked_partition_upsert(config_id, part_id, date3)

    assert list(scrubber_db.checked_partition_iter(config_id)) == [
        (part_id, NB_PARTITIONS, DATE, date2)  # newest date wins
    ]


def test_checked_partition_get(
    datastore: Datastore, scrubber_db: ScrubberDb, config_id: int
):
    with patch("swh.scrubber.db.now", return_value=DATE):
        dir_part_gen = scrubber_db.checked_partition_iter_next(config_id)
        part_id = next(dir_part_gen)
    assert scrubber_db.checked_partition_get_last_date(config_id, part_id) is None

    scrubber_db.checked_partition_upsert(config_id, part_id, DATE)

    assert scrubber_db.checked_partition_get_last_date(config_id, part_id) == DATE


def test_checked_partition_get_running(
    datastore: Datastore, scrubber_db: ScrubberDb, config_id: int
):
    assert list(scrubber_db.checked_partition_get_running(config_id)) == []
    with patch("swh.scrubber.db.now", return_value=DATE):
        dir_part_gen = scrubber_db.checked_partition_iter_next(config_id)
        part_id1 = next(dir_part_gen)
        part_id2 = next(dir_part_gen)
        part_id3 = next(dir_part_gen)

    assert scrubber_db.checked_partition_get_last_date(config_id, part_id1) is None
    assert scrubber_db.checked_partition_get_last_date(config_id, part_id2) is None
    assert scrubber_db.checked_partition_get_last_date(config_id, part_id3) is None

    assert list(scrubber_db.checked_partition_get_running(config_id)) == [
        (part_id1, DATE),
        (part_id2, DATE),
        (part_id3, DATE),
    ]

    scrubber_db.checked_partition_upsert(config_id, part_id2, DATE + ONE_MINUTE)
    assert list(scrubber_db.checked_partition_get_running(config_id)) == [
        (part_id1, DATE),
        (part_id3, DATE),
    ]

    scrubber_db.checked_partition_upsert(config_id, part_id1, DATE + ONE_MINUTE)
    assert list(scrubber_db.checked_partition_get_running(config_id)) == [
        (part_id3, DATE),
    ]

    scrubber_db.checked_partition_upsert(config_id, part_id3, DATE + ONE_MINUTE)
    assert list(scrubber_db.checked_partition_get_running(config_id)) == []
