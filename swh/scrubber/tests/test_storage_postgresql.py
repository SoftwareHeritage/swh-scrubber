# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import unittest.mock

import attr
import pytest

from swh.journal.serializers import kafka_to_value
from swh.model import swhids
from swh.model.tests import swh_model_data
from swh.scrubber.storage_checker import StorageChecker
from swh.storage.backfill import byte_ranges

# decorator to make swh.storage.backfill use less ranges, so tests run faster
patch_byte_ranges = unittest.mock.patch(
    "swh.storage.backfill.byte_ranges",
    lambda numbits, start, end: byte_ranges(numbits // 8, start, end),
)


@patch_byte_ranges
def test_no_corruption(scrubber_db, swh_storage):
    swh_storage.directory_add(swh_model_data.DIRECTORIES)
    swh_storage.revision_add(swh_model_data.REVISIONS)
    swh_storage.release_add(swh_model_data.RELEASES)
    swh_storage.snapshot_add(swh_model_data.SNAPSHOTS)

    for object_type in ("snapshot", "release", "revision", "directory"):
        StorageChecker(
            db=scrubber_db,
            storage=swh_storage,
            object_type=object_type,
            start_object="00" * 20,
            end_object="ff" * 20,
        ).run()

    assert list(scrubber_db.corrupt_object_iter()) == []


@pytest.mark.parametrize("corrupt_idx", range(len(swh_model_data.SNAPSHOTS)))
@patch_byte_ranges
def test_corrupt_snapshot(scrubber_db, swh_storage, corrupt_idx):
    snapshots = list(swh_model_data.SNAPSHOTS)
    snapshots[corrupt_idx] = attr.evolve(snapshots[corrupt_idx], id=b"\x00" * 20)
    swh_storage.snapshot_add(snapshots)

    before_date = datetime.datetime.now(tz=datetime.timezone.utc)
    for object_type in ("snapshot", "release", "revision", "directory"):
        StorageChecker(
            db=scrubber_db,
            storage=swh_storage,
            object_type=object_type,
            start_object="00" * 20,
            end_object="ff" * 20,
        ).run()
    after_date = datetime.datetime.now(tz=datetime.timezone.utc)

    corrupt_objects = list(scrubber_db.corrupt_object_iter())
    assert len(corrupt_objects) == 1
    assert corrupt_objects[0].id == swhids.CoreSWHID.from_string(
        "swh:1:snp:0000000000000000000000000000000000000000"
    )
    assert corrupt_objects[0].datastore.package == "storage"
    assert corrupt_objects[0].datastore.cls == "postgresql"
    assert corrupt_objects[0].datastore.instance.startswith(
        "user=postgres password=xxx dbname=storage host="
    )
    assert (
        before_date - datetime.timedelta(seconds=5)
        <= corrupt_objects[0].first_occurrence
        <= after_date + datetime.timedelta(seconds=5)
    )
    assert (
        kafka_to_value(corrupt_objects[0].object_) == snapshots[corrupt_idx].to_dict()
    )


@patch_byte_ranges
def test_corrupt_snapshots_same_batch(scrubber_db, swh_storage):
    snapshots = list(swh_model_data.SNAPSHOTS)
    for i in (0, 1):
        snapshots[i] = attr.evolve(snapshots[i], id=bytes([i]) * 20)
    swh_storage.snapshot_add(snapshots)

    StorageChecker(
        db=scrubber_db,
        storage=swh_storage,
        object_type="snapshot",
        start_object="00" * 20,
        end_object="ff" * 20,
    ).run()

    corrupt_objects = list(scrubber_db.corrupt_object_iter())
    assert len(corrupt_objects) == 2
    assert {co.id for co in corrupt_objects} == {
        swhids.CoreSWHID.from_string(swhid)
        for swhid in [
            "swh:1:snp:0000000000000000000000000000000000000000",
            "swh:1:snp:0101010101010101010101010101010101010101",
        ]
    }


@patch_byte_ranges
def test_corrupt_snapshots_different_batches(scrubber_db, swh_storage):
    snapshots = list(swh_model_data.SNAPSHOTS)
    for i in (0, 1):
        snapshots[i] = attr.evolve(snapshots[i], id=bytes([i * 255]) * 20)
    swh_storage.snapshot_add(snapshots)

    StorageChecker(
        db=scrubber_db,
        storage=swh_storage,
        object_type="snapshot",
        start_object="00" * 20,
        end_object="87" * 20,
    ).run()

    corrupt_objects = list(scrubber_db.corrupt_object_iter())
    assert len(corrupt_objects) == 1

    # Simulates resuming from a different process, with an empty lru_cache
    scrubber_db.datastore_get_or_add.cache_clear()

    StorageChecker(
        db=scrubber_db,
        storage=swh_storage,
        object_type="snapshot",
        start_object="88" * 20,
        end_object="ff" * 20,
    ).run()

    corrupt_objects = list(scrubber_db.corrupt_object_iter())
    assert len(corrupt_objects) == 2
    assert {co.id for co in corrupt_objects} == {
        swhids.CoreSWHID.from_string(swhid)
        for swhid in [
            "swh:1:snp:0000000000000000000000000000000000000000",
            "swh:1:snp:ffffffffffffffffffffffffffffffffffffffff",
        ]
    }
