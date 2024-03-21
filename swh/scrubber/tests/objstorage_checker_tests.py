# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime, timedelta, timezone

import attr
import pytest

from swh.journal.serializers import kafka_to_value
from swh.model.swhids import CoreSWHID, ObjectType
from swh.model.tests import swh_model_data
from swh.scrubber.objstorage_checker import (
    ObjectStorageChecker,
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
def objstorage_checker(swh_storage, swh_objstorage, scrubber_db, datastore):
    nb_partitions = len(EXPECTED_PARTITIONS)
    config_id = scrubber_db.config_add(
        "cfg_objstorage_checker", datastore, ObjectType.CONTENT, nb_partitions
    )
    return ObjectStorageChecker(scrubber_db, config_id, swh_storage, swh_objstorage)


def test_objstorage_checker_no_corruption(
    swh_storage, swh_objstorage, objstorage_checker
):
    swh_storage.content_add(swh_model_data.CONTENTS)
    swh_objstorage.add_batch({c.sha1: c.data for c in swh_model_data.CONTENTS})

    objstorage_checker.run()

    scrubber_db = objstorage_checker.db
    assert list(scrubber_db.corrupt_object_iter()) == []

    assert_checked_ranges(
        scrubber_db,
        [(ObjectType.CONTENT, objstorage_checker.config_id)],
        EXPECTED_PARTITIONS,
    )


@pytest.mark.parametrize("missing_idx", range(0, len(swh_model_data.CONTENTS), 5))
def test_objstorage_checker_missing_content(
    swh_storage, swh_objstorage, objstorage_checker, missing_idx
):
    contents = list(swh_model_data.CONTENTS)
    swh_storage.content_add(contents)
    swh_objstorage.add_batch(
        {c.sha1: c.data for i, c in enumerate(contents) if i != missing_idx}
    )

    before_date = datetime.now(tz=timezone.utc)
    objstorage_checker.run()
    after_date = datetime.now(tz=timezone.utc)

    scrubber_db = objstorage_checker.db

    missing_objects = list(scrubber_db.missing_object_iter())
    assert len(missing_objects) == 1
    assert missing_objects[0].id == contents[missing_idx].swhid()
    assert missing_objects[0].config.datastore == objstorage_checker.datastore
    assert (
        before_date - timedelta(seconds=5)
        <= missing_objects[0].first_occurrence
        <= after_date + timedelta(seconds=5)
    )

    assert_checked_ranges(
        scrubber_db,
        [(ObjectType.CONTENT, objstorage_checker.config_id)],
        EXPECTED_PARTITIONS,
        before_date,
        after_date,
    )


@pytest.mark.parametrize("corrupt_idx", range(0, len(swh_model_data.CONTENTS), 5))
def test_objstorage_checker_corrupt_content(
    swh_storage, swh_objstorage, objstorage_checker, corrupt_idx
):
    contents = list(swh_model_data.CONTENTS)
    contents[corrupt_idx] = attr.evolve(contents[corrupt_idx], sha1_git=b"\x00" * 20)
    swh_storage.content_add(contents)
    swh_objstorage.add_batch({c.sha1: c.data for c in contents})

    before_date = datetime.now(tz=timezone.utc)
    objstorage_checker.run()
    after_date = datetime.now(tz=timezone.utc)

    scrubber_db = objstorage_checker.db

    corrupt_objects = list(scrubber_db.corrupt_object_iter())
    assert len(corrupt_objects) == 1
    assert corrupt_objects[0].id == CoreSWHID.from_string(
        "swh:1:cnt:0000000000000000000000000000000000000000"
    )
    assert corrupt_objects[0].config.datastore == objstorage_checker.datastore
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
        [(ObjectType.CONTENT, objstorage_checker.config_id)],
        EXPECTED_PARTITIONS,
        before_date,
        after_date,
    )
