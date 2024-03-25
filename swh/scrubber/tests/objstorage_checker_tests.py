# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timedelta, timezone
import json

import attr
import pytest

from swh.journal.serializers import kafka_to_value
from swh.model.swhids import CoreSWHID, ObjectType
from swh.model.tests import swh_model_data
from swh.scrubber.objstorage_checker import (
    ObjectStorageCheckerFromJournal,
    ObjectStorageCheckerFromStoragePartition,
    get_objstorage_datastore,
)

from .storage_checker_tests import assert_checked_ranges

EXPECTED_PARTITIONS = {
    (ObjectType.CONTENT, 0, 4),
    (ObjectType.CONTENT, 1, 4),
    (ObjectType.CONTENT, 2, 4),
    (ObjectType.CONTENT, 3, 4),
}


@pytest.fixture
def datastore(swh_objstorage_config):
    return get_objstorage_datastore(swh_objstorage_config)


@pytest.fixture
def objstorage_checker_partition(swh_storage, swh_objstorage, scrubber_db, datastore):
    nb_partitions = len(EXPECTED_PARTITIONS)
    config_id = scrubber_db.config_add(
        "cfg_objstorage_checker_partition", datastore, ObjectType.CONTENT, nb_partitions
    )
    return ObjectStorageCheckerFromStoragePartition(
        scrubber_db, config_id, swh_storage, swh_objstorage
    )


@pytest.fixture
def objstorage_checker_journal(
    journal_client_config, swh_objstorage, scrubber_db, datastore
):
    config_id = scrubber_db.config_add(
        "cfg_objstorage_checker_journal", datastore, ObjectType.CONTENT, nb_partitions=1
    )
    return ObjectStorageCheckerFromJournal(
        scrubber_db, config_id, journal_client_config, swh_objstorage
    )


def test_objstorage_checker_partition_no_corruption(
    swh_storage, swh_objstorage, objstorage_checker_partition
):
    swh_storage.content_add(swh_model_data.CONTENTS)
    swh_objstorage.add_batch({c.sha1: c.data for c in swh_model_data.CONTENTS})

    objstorage_checker_partition.run()

    scrubber_db = objstorage_checker_partition.db
    assert list(scrubber_db.corrupt_object_iter()) == []

    assert_checked_ranges(
        scrubber_db,
        [(ObjectType.CONTENT, objstorage_checker_partition.config_id)],
        EXPECTED_PARTITIONS,
    )


@pytest.mark.parametrize("missing_idx", range(0, len(swh_model_data.CONTENTS), 5))
def test_objstorage_checker_partition_missing_content(
    swh_storage, swh_objstorage, objstorage_checker_partition, missing_idx
):
    contents = list(swh_model_data.CONTENTS)
    swh_storage.content_add(contents)
    swh_objstorage.add_batch(
        {c.sha1: c.data for i, c in enumerate(contents) if i != missing_idx}
    )

    before_date = datetime.now(tz=timezone.utc)
    objstorage_checker_partition.run()
    after_date = datetime.now(tz=timezone.utc)

    scrubber_db = objstorage_checker_partition.db

    missing_objects = list(scrubber_db.missing_object_iter())
    assert len(missing_objects) == 1
    assert missing_objects[0].id == contents[missing_idx].swhid()
    assert missing_objects[0].config.datastore == objstorage_checker_partition.datastore
    assert (
        before_date - timedelta(seconds=5)
        <= missing_objects[0].first_occurrence
        <= after_date + timedelta(seconds=5)
    )

    assert_checked_ranges(
        scrubber_db,
        [(ObjectType.CONTENT, objstorage_checker_partition.config_id)],
        EXPECTED_PARTITIONS,
        before_date,
        after_date,
    )


@pytest.mark.parametrize("corrupt_idx", range(0, len(swh_model_data.CONTENTS), 5))
def test_objstorage_checker_partition_corrupt_content(
    swh_storage, swh_objstorage, objstorage_checker_partition, corrupt_idx
):
    contents = list(swh_model_data.CONTENTS)
    contents[corrupt_idx] = attr.evolve(contents[corrupt_idx], sha1_git=b"\x00" * 20)
    swh_storage.content_add(contents)
    swh_objstorage.add_batch({c.sha1: c.data for c in contents})

    before_date = datetime.now(tz=timezone.utc)
    objstorage_checker_partition.run()
    after_date = datetime.now(tz=timezone.utc)

    scrubber_db = objstorage_checker_partition.db

    corrupt_objects = list(scrubber_db.corrupt_object_iter())
    assert len(corrupt_objects) == 1
    assert corrupt_objects[0].id == CoreSWHID.from_string(
        "swh:1:cnt:0000000000000000000000000000000000000000"
    )
    assert corrupt_objects[0].config.datastore == objstorage_checker_partition.datastore
    assert (
        before_date - timedelta(seconds=5)
        <= corrupt_objects[0].first_occurrence
        <= after_date + timedelta(seconds=5)
    )

    corrupted_content = contents[corrupt_idx].to_dict()
    corrupted_content.pop("data")
    assert kafka_to_value(corrupt_objects[0].object_) == corrupted_content

    assert_checked_ranges(
        scrubber_db,
        [(ObjectType.CONTENT, objstorage_checker_partition.config_id)],
        EXPECTED_PARTITIONS,
        before_date,
        after_date,
    )


def test_objstorage_checker_journal_contents_no_corruption(
    scrubber_db,
    journal_writer,
    journal_client_config,
    objstorage_checker_journal,
):
    journal_writer.write_additions("content", swh_model_data.CONTENTS)

    gid = journal_client_config["group_id"] + "_"

    object_type = "content"
    journal_client_config["group_id"] = gid + object_type

    objstorage_checker_journal.objstorage.add_batch(
        {c.sha1: c.data for c in swh_model_data.CONTENTS}
    )
    objstorage_checker_journal.run()
    objstorage_checker_journal.journal_client.close()

    assert list(scrubber_db.corrupt_object_iter()) == []


@pytest.mark.parametrize("corrupt_idx", range(0, len(swh_model_data.CONTENTS), 5))
def test_objstorage_checker_journal_corrupt_content(
    scrubber_db,
    journal_writer,
    objstorage_checker_journal,
    swh_objstorage_config,
    corrupt_idx,
):
    contents = list(swh_model_data.CONTENTS)
    contents[corrupt_idx] = attr.evolve(contents[corrupt_idx], sha1_git=b"\x00" * 20)

    journal_writer.write_additions("content", contents)

    before_date = datetime.now(tz=timezone.utc)

    objstorage_checker_journal.objstorage.add_batch({c.sha1: c.data for c in contents})
    objstorage_checker_journal.run()
    after_date = datetime.now(tz=timezone.utc)

    corrupt_objects = list(scrubber_db.corrupt_object_iter())
    assert len(corrupt_objects) == 1
    assert corrupt_objects[0].id == CoreSWHID.from_string(
        "swh:1:cnt:0000000000000000000000000000000000000000"
    )
    assert corrupt_objects[0].config.datastore.package == "objstorage"
    assert corrupt_objects[0].config.datastore.cls == swh_objstorage_config.pop("cls")
    assert corrupt_objects[0].config.datastore.instance == json.dumps(
        swh_objstorage_config
    )
    assert (
        before_date - timedelta(seconds=5)
        <= corrupt_objects[0].first_occurrence
        <= after_date + timedelta(seconds=5)
    )
    corrupted_content = contents[corrupt_idx].to_dict()
    corrupted_content.pop("data")
    assert kafka_to_value(corrupt_objects[0].object_) == corrupted_content


@pytest.mark.parametrize("missing_idx", range(0, len(swh_model_data.CONTENTS), 5))
def test_objstorage_checker_journal_missing_content(
    scrubber_db,
    journal_writer,
    objstorage_checker_journal,
    missing_idx,
):
    contents = list(swh_model_data.CONTENTS)

    journal_writer.write_additions("content", contents)

    before_date = datetime.now(tz=timezone.utc)

    objstorage_checker_journal.objstorage.add_batch(
        {c.sha1: c.data for i, c in enumerate(contents) if i != missing_idx}
    )
    objstorage_checker_journal.run()
    after_date = datetime.now(tz=timezone.utc)

    missing_objects = list(scrubber_db.missing_object_iter())
    assert len(missing_objects) == 1
    assert missing_objects[0].id == contents[missing_idx].swhid()
    assert missing_objects[0].config.datastore == objstorage_checker_journal.datastore
    assert (
        before_date - timedelta(seconds=5)
        <= missing_objects[0].first_occurrence
        <= after_date + timedelta(seconds=5)
    )
