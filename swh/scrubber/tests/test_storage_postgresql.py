# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import unittest.mock

import attr
import pytest

from swh.journal.serializers import kafka_to_value
from swh.model import model, swhids
from swh.model.tests import swh_model_data
from swh.scrubber.db import Datastore
from swh.scrubber.storage_checker import StorageChecker, storage_db
from swh.storage.backfill import byte_ranges

CONTENT1 = model.Content.from_data(b"foo")
DIRECTORY1 = model.Directory(
    entries=(
        model.DirectoryEntry(
            target=CONTENT1.sha1_git, type="file", name=b"file1", perms=0o1
        ),
    )
)
DIRECTORY2 = model.Directory(
    entries=(
        model.DirectoryEntry(
            target=CONTENT1.sha1_git, type="file", name=b"file2", perms=0o1
        ),
        model.DirectoryEntry(target=DIRECTORY1.id, type="dir", name=b"dir1", perms=0o1),
        model.DirectoryEntry(target=b"\x00" * 20, type="rev", name=b"rev1", perms=0o1),
    )
)
REVISION1 = model.Revision(
    message=b"blah",
    directory=DIRECTORY2.id,
    author=None,
    committer=None,
    date=None,
    committer_date=None,
    type=model.RevisionType.GIT,
    synthetic=True,
)
RELEASE1 = model.Release(
    message=b"blih",
    name=b"bluh",
    target_type=model.ObjectType.REVISION,
    target=REVISION1.id,
    synthetic=True,
)
SNAPSHOT1 = model.Snapshot(
    branches={
        b"rel1": model.SnapshotBranch(
            target_type=model.TargetType.RELEASE, target=RELEASE1.id
        ),
    }
)


@pytest.fixture
def datastore(swh_storage):
    with storage_db(swh_storage) as db:
        return Datastore(
            package="storage",
            cls="postgresql",
            instance=db.conn.dsn,
        )


# decorator to make swh.storage.backfill use fewer ranges, so tests run faster
patch_byte_ranges = unittest.mock.patch(
    "swh.storage.backfill.byte_ranges",
    lambda numbits, start, end: byte_ranges(numbits // 8, start, end),
)


def _short_ranges(type_):
    return [
        (
            f"swh:1:{type_}:0000000000000000000000000000000000000000",
            f"swh:1:{type_}:1fffffffffffffffffffffffffffffffffffffff",
        ),
        (
            f"swh:1:{type_}:2000000000000000000000000000000000000000",
            f"swh:1:{type_}:3fffffffffffffffffffffffffffffffffffffff",
        ),
        (
            f"swh:1:{type_}:4000000000000000000000000000000000000000",
            f"swh:1:{type_}:5fffffffffffffffffffffffffffffffffffffff",
        ),
        (
            f"swh:1:{type_}:6000000000000000000000000000000000000000",
            f"swh:1:{type_}:7fffffffffffffffffffffffffffffffffffffff",
        ),
        (
            f"swh:1:{type_}:8000000000000000000000000000000000000000",
            f"swh:1:{type_}:9fffffffffffffffffffffffffffffffffffffff",
        ),
        (
            f"swh:1:{type_}:a000000000000000000000000000000000000000",
            f"swh:1:{type_}:bfffffffffffffffffffffffffffffffffffffff",
        ),
        (
            f"swh:1:{type_}:c000000000000000000000000000000000000000",
            f"swh:1:{type_}:dfffffffffffffffffffffffffffffffffffffff",
        ),
        (
            f"swh:1:{type_}:e000000000000000000000000000000000000000",
            f"swh:1:{type_}:ffffffffffffffffffffffffffffffffffffffff",
        ),
    ]


def _long_ranges(type_):
    return [
        (
            f"swh:1:{type_}:0000000000000000000000000000000000000000",
            f"swh:1:{type_}:3fffffffffffffffffffffffffffffffffffffff",
        ),
        (
            f"swh:1:{type_}:4000000000000000000000000000000000000000",
            f"swh:1:{type_}:7fffffffffffffffffffffffffffffffffffffff",
        ),
        (
            f"swh:1:{type_}:8000000000000000000000000000000000000000",
            f"swh:1:{type_}:bfffffffffffffffffffffffffffffffffffffff",
        ),
        (
            f"swh:1:{type_}:c000000000000000000000000000000000000000",
            f"swh:1:{type_}:ffffffffffffffffffffffffffffffffffffffff",
        ),
    ]


EXPECTED_RANGES = [
    *_short_ranges("dir"),
    *_long_ranges("rel"),
    *_short_ranges("rev"),
    *_long_ranges("snp"),
]


def assert_checked_ranges(
    scrubber_db, datastore, expected_ranges, before_date=None, after_date=None
):
    if before_date is not None:
        assert all(
            before_date < date < after_date
            for (_, _, date) in scrubber_db.checked_range_iter(datastore)
        )

    checked_ranges = [
        (str(start), str(end))
        for (start, end, date) in scrubber_db.checked_range_iter(datastore)
    ]
    checked_ranges.sort(key=str)

    assert checked_ranges == expected_ranges


@patch_byte_ranges
def test_no_corruption(scrubber_db, datastore, swh_storage):
    swh_storage.directory_add(swh_model_data.DIRECTORIES)
    swh_storage.revision_add(swh_model_data.REVISIONS)
    swh_storage.release_add(swh_model_data.RELEASES)
    swh_storage.snapshot_add(swh_model_data.SNAPSHOTS)

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

    assert list(scrubber_db.corrupt_object_iter()) == []

    assert_checked_ranges(
        scrubber_db, datastore, EXPECTED_RANGES, before_date, after_date
    )


@pytest.mark.parametrize("corrupt_idx", range(len(swh_model_data.SNAPSHOTS)))
@patch_byte_ranges
def test_corrupt_snapshot(scrubber_db, datastore, swh_storage, corrupt_idx):
    storage_dsn = swh_storage.get_db().conn.dsn
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
    assert corrupt_objects[0].datastore.instance.startswith(storage_dsn)
    assert (
        before_date - datetime.timedelta(seconds=5)
        <= corrupt_objects[0].first_occurrence
        <= after_date + datetime.timedelta(seconds=5)
    )
    assert (
        kafka_to_value(corrupt_objects[0].object_) == snapshots[corrupt_idx].to_dict()
    )

    assert_checked_ranges(
        scrubber_db, datastore, EXPECTED_RANGES, before_date, after_date
    )


@patch_byte_ranges
def test_corrupt_snapshots_same_batch(scrubber_db, datastore, swh_storage):
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

    assert_checked_ranges(scrubber_db, datastore, _long_ranges("snp"))


@patch_byte_ranges
def test_corrupt_snapshots_different_batches(scrubber_db, datastore, swh_storage):
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

    assert_checked_ranges(scrubber_db, datastore, _long_ranges("snp"))


@patch_byte_ranges
def test_no_recheck(scrubber_db, datastore, swh_storage):
    """
    Tests that objects that were already checked are not checked again on
    the next run.
    """
    # Corrupt two snapshots
    snapshots = list(swh_model_data.SNAPSHOTS)
    for i in (0, 1):
        snapshots[i] = attr.evolve(snapshots[i], id=bytes([i]) * 20)
    swh_storage.snapshot_add(snapshots)

    # Mark ranges as already checked
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    for (range_start, range_end) in EXPECTED_RANGES:
        scrubber_db.checked_range_upsert(datastore, range_start, range_end, now)

    StorageChecker(
        db=scrubber_db,
        storage=swh_storage,
        object_type="snapshot",
        start_object="00" * 20,
        end_object="ff" * 20,
    ).run()

    corrupt_objects = list(scrubber_db.corrupt_object_iter())
    assert (
        corrupt_objects == []
    ), "Detected corrupt objects in ranges that should have been skipped."

    # Make sure the DB was not changed (in particular, that timestamps were not bumped)
    ranges = [
        (str(range_start), str(range_end), date)
        for (range_start, range_end, date) in scrubber_db.checked_range_iter(datastore)
    ]
    ranges.sort(key=str)
    assert ranges == [
        (range_start, range_end, now) for (range_start, range_end) in EXPECTED_RANGES
    ]


@patch_byte_ranges
def test_no_hole(scrubber_db, datastore, swh_storage):
    swh_storage.content_add([CONTENT1])
    swh_storage.directory_add([DIRECTORY1, DIRECTORY2])
    swh_storage.revision_add([REVISION1])
    swh_storage.release_add([RELEASE1])
    swh_storage.snapshot_add([SNAPSHOT1])

    for object_type in ("snapshot", "release", "revision", "directory"):
        StorageChecker(
            db=scrubber_db,
            storage=swh_storage,
            object_type=object_type,
            start_object="00" * 20,
            end_object="ff" * 20,
        ).run()

    assert list(scrubber_db.missing_object_iter()) == []

    assert_checked_ranges(scrubber_db, datastore, EXPECTED_RANGES)


@pytest.mark.parametrize(
    "missing_object",
    ["content1", "directory1", "directory2", "revision1", "release1"],
)
@patch_byte_ranges
def test_one_hole(scrubber_db, datastore, swh_storage, missing_object):
    if missing_object == "content1":
        missing_swhid = CONTENT1.swhid()
        reference_swhids = [DIRECTORY1.swhid(), DIRECTORY2.swhid()]
    else:
        swh_storage.content_add([CONTENT1])

    if missing_object == "directory1":
        missing_swhid = DIRECTORY1.swhid()
        reference_swhids = [DIRECTORY2.swhid()]
    else:
        swh_storage.directory_add([DIRECTORY1])

    if missing_object == "directory2":
        missing_swhid = DIRECTORY2.swhid()
        reference_swhids = [REVISION1.swhid()]
    else:
        swh_storage.directory_add([DIRECTORY2])

    if missing_object == "revision1":
        missing_swhid = REVISION1.swhid()
        reference_swhids = [RELEASE1.swhid()]
    else:
        swh_storage.revision_add([REVISION1])

    if missing_object == "release1":
        missing_swhid = RELEASE1.swhid()
        reference_swhids = [SNAPSHOT1.swhid()]
    else:
        swh_storage.release_add([RELEASE1])

    swh_storage.snapshot_add([SNAPSHOT1])

    for object_type in ("snapshot", "release", "revision", "directory"):
        StorageChecker(
            db=scrubber_db,
            storage=swh_storage,
            object_type=object_type,
            start_object="00" * 20,
            end_object="ff" * 20,
        ).run()

    assert [mo.id for mo in scrubber_db.missing_object_iter()] == [missing_swhid]
    assert {
        (mor.missing_id, mor.reference_id)
        for mor in scrubber_db.missing_object_reference_iter(missing_swhid)
    } == {(missing_swhid, reference_swhid) for reference_swhid in reference_swhids}

    assert_checked_ranges(scrubber_db, datastore, EXPECTED_RANGES)


@patch_byte_ranges
def test_two_holes(scrubber_db, datastore, swh_storage):
    # missing content and revision
    swh_storage.directory_add([DIRECTORY1, DIRECTORY2])
    swh_storage.release_add([RELEASE1])
    swh_storage.snapshot_add([SNAPSHOT1])

    for object_type in ("snapshot", "release", "revision", "directory"):
        StorageChecker(
            db=scrubber_db,
            storage=swh_storage,
            object_type=object_type,
            start_object="00" * 20,
            end_object="ff" * 20,
        ).run()

    assert {mo.id for mo in scrubber_db.missing_object_iter()} == {
        CONTENT1.swhid(),
        REVISION1.swhid(),
    }
    assert {
        mor.reference_id
        for mor in scrubber_db.missing_object_reference_iter(CONTENT1.swhid())
    } == {DIRECTORY1.swhid(), DIRECTORY2.swhid()}
    assert {
        mor.reference_id
        for mor in scrubber_db.missing_object_reference_iter(REVISION1.swhid())
    } == {RELEASE1.swhid()}

    assert_checked_ranges(scrubber_db, datastore, EXPECTED_RANGES)
