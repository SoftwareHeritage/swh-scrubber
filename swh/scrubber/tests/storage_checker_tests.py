# Copyright (C) 2022-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

import attr
import msgpack
import pytest

from swh.journal.serializers import kafka_to_value
from swh.model import model, swhids
from swh.model.tests import swh_model_data
from swh.scrubber.storage_checker import StorageChecker

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
    target_type=model.ReleaseTargetType.REVISION,
    target=REVISION1.id,
    synthetic=True,
)
SNAPSHOT1 = model.Snapshot(
    branches={
        b"rel1": model.SnapshotBranch(
            target_type=model.SnapshotTargetType.RELEASE, target=RELEASE1.id
        ),
    }
)


EXPECTED_PARTITIONS = {
    (swhids.ObjectType.SNAPSHOT, 0, 1),
    (swhids.ObjectType.DIRECTORY, 0, 1),
    (swhids.ObjectType.REVISION, 0, 1),
    (swhids.ObjectType.RELEASE, 0, 1),
}

OBJECT_TYPES = (
    swhids.ObjectType.SNAPSHOT,
    swhids.ObjectType.DIRECTORY,
    swhids.ObjectType.REVISION,
    swhids.ObjectType.RELEASE,
)


def assert_checked_ranges(
    scrubber_db, config, expected_ranges, before_date=None, after_date=None
):
    checked_ranges = set()
    for object_type, config_id in config:
        if before_date is not None:
            assert all(
                before_date < date < after_date
                for (_, _, date, _) in scrubber_db.checked_partition_iter(config_id)
            )

        checked_ranges.update(
            {
                (object_type, partition, nb_partitions)
                for (
                    partition,
                    nb_partitions,
                    start_date,
                    end_date,
                ) in scrubber_db.checked_partition_iter(config_id)
            }
        )
    assert checked_ranges == expected_ranges


def test_no_corruption(scrubber_db, datastore, swh_storage):
    swh_storage.directory_add(swh_model_data.DIRECTORIES)
    swh_storage.revision_add(swh_model_data.REVISIONS)
    swh_storage.release_add(swh_model_data.RELEASES)
    swh_storage.snapshot_add(swh_model_data.SNAPSHOTS)

    before_date = datetime.datetime.now(tz=datetime.timezone.utc)
    config = []
    for object_type in OBJECT_TYPES:
        config_id = scrubber_db.config_add(
            f"cfg_{object_type.name}", datastore, object_type, 1
        )
        config.append((object_type, config_id))
        StorageChecker(
            db=scrubber_db,
            storage=swh_storage,
            config_id=config_id,
        ).run()
    after_date = datetime.datetime.now(tz=datetime.timezone.utc)

    assert list(scrubber_db.corrupt_object_iter()) == []

    assert_checked_ranges(
        scrubber_db, config, EXPECTED_PARTITIONS, before_date, after_date
    )


@pytest.mark.parametrize("corrupt_idx", range(len(swh_model_data.SNAPSHOTS)))
def test_corrupt_snapshot(scrubber_db, datastore, swh_storage, corrupt_idx):
    snapshots = list(swh_model_data.SNAPSHOTS)
    snapshots[corrupt_idx] = attr.evolve(snapshots[corrupt_idx], id=b"\x00" * 20)
    swh_storage.snapshot_add(snapshots)

    before_date = datetime.datetime.now(tz=datetime.timezone.utc)
    config = []
    for object_type in OBJECT_TYPES:
        config_id = scrubber_db.config_add(
            f"cfg_{object_type.name}", datastore, object_type, 1
        )
        config.append((object_type, config_id))
        StorageChecker(
            db=scrubber_db,
            storage=swh_storage,
            config_id=config_id,
        ).run()
    after_date = datetime.datetime.now(tz=datetime.timezone.utc)

    corrupt_objects = list(scrubber_db.corrupt_object_iter())
    assert len(corrupt_objects) == 1
    assert corrupt_objects[0].id == swhids.CoreSWHID.from_string(
        "swh:1:snp:0000000000000000000000000000000000000000"
    )
    assert corrupt_objects[0].config.datastore == datastore
    assert (
        before_date - datetime.timedelta(seconds=5)
        <= corrupt_objects[0].first_occurrence
        <= after_date + datetime.timedelta(seconds=5)
    )
    assert (
        kafka_to_value(corrupt_objects[0].object_) == snapshots[corrupt_idx].to_dict()
    )

    assert_checked_ranges(
        scrubber_db, config, EXPECTED_PARTITIONS, before_date, after_date
    )


def test_corrupt_snapshots_same_batch(scrubber_db, datastore, swh_storage):
    snapshots = list(swh_model_data.SNAPSHOTS)
    for i in (0, 1):
        snapshots[i] = attr.evolve(snapshots[i], id=bytes([i]) * 20)
    swh_storage.snapshot_add(snapshots)

    config_id = scrubber_db.config_add("cfg1", datastore, swhids.ObjectType.SNAPSHOT, 1)
    StorageChecker(
        db=scrubber_db,
        storage=swh_storage,
        config_id=config_id,
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

    assert_checked_ranges(
        scrubber_db,
        [(swhids.ObjectType.SNAPSHOT, config_id)],
        {(swhids.ObjectType.SNAPSHOT, 0, 1)},
    )


def test_corrupt_snapshots_different_batches(scrubber_db, datastore, swh_storage):
    # FIXME: this is brittle, because it relies on objects being on different
    # partitions. In particular on Cassandra, it will break if the hashing scheme
    # or hash algorithm changes.
    snapshots = list(swh_model_data.SNAPSHOTS)
    snapshots.extend(
        [attr.evolve(snapshots[0], id=bytes([17 * i]) * 20) for i in range(16)]
    )
    swh_storage.snapshot_add(snapshots)

    config_id = scrubber_db.config_add(
        "cfg1", datastore, swhids.ObjectType.SNAPSHOT, 16
    )
    StorageChecker(
        db=scrubber_db,
        storage=swh_storage,
        config_id=config_id,
        limit=8,
    ).run()

    corrupt_objects = list(scrubber_db.corrupt_object_iter())
    assert len(corrupt_objects) == 8

    # Simulates resuming from a different process
    scrubber_db.datastore_get_or_add.cache_clear()

    StorageChecker(
        db=scrubber_db,
        storage=swh_storage,
        config_id=config_id,
        limit=8,
    ).run()

    corrupt_objects = list(scrubber_db.corrupt_object_iter())
    assert len(corrupt_objects) == 16
    assert {co.id for co in corrupt_objects} == {
        swhids.CoreSWHID.from_string(swhid)
        for swhid in ["swh:1:snp:" + f"{i:x}" * 40 for i in range(16)]
    }

    assert_checked_ranges(
        scrubber_db,
        [(swhids.ObjectType.SNAPSHOT, config_id)],
        {(swhids.ObjectType.SNAPSHOT, i, 16) for i in range(16)},
    )


def test_directory_duplicate_entries(scrubber_db, datastore, swh_storage):
    run_validators = attr.get_run_validators()
    attr.set_run_validators(False)
    try:
        invalid_directory = model.Directory(
            entries=(
                model.DirectoryEntry(
                    name=b"foo", type="dir", target=b"\x01" * 20, perms=1
                ),
                model.DirectoryEntry(
                    name=b"foo", type="file", target=b"\x00" * 20, perms=0
                ),
            )
        )
    finally:
        attr.set_run_validators(run_validators)
    swh_storage.directory_add([invalid_directory])

    deduplicated_directory = model.Directory(
        id=invalid_directory.id,
        entries=(
            model.DirectoryEntry(name=b"foo", type="dir", target=b"\x01" * 20, perms=1),
            model.DirectoryEntry(
                name=b"foo_0000000000", type="file", target=b"\x00" * 20, perms=0
            ),
        ),
        raw_manifest=(
            # fmt: off
            b"tree 52\x00"
            + b"0 foo\x00" + b"\x00" * 20
            + b"1 foo\x00" + b"\x01" * 20
            # fmt: on
        ),
    )

    before_date = datetime.datetime.now(tz=datetime.timezone.utc)
    config = []
    for object_type in OBJECT_TYPES:
        config_id = scrubber_db.config_add("cfg2", datastore, object_type, 1)
        config.append((object_type, config_id))
        StorageChecker(
            db=scrubber_db,
            storage=swh_storage,
            config_id=config_id,
        ).run()
    after_date = datetime.datetime.now(tz=datetime.timezone.utc)

    corrupt_objects = list(scrubber_db.corrupt_object_iter())
    assert len(corrupt_objects) == 1
    assert corrupt_objects[0].id == invalid_directory.swhid()
    assert corrupt_objects[0].config.datastore == datastore
    assert (
        before_date - datetime.timedelta(seconds=5)
        <= corrupt_objects[0].first_occurrence
        <= after_date + datetime.timedelta(seconds=5)
    )
    assert kafka_to_value(corrupt_objects[0].object_) == msgpack.loads(
        msgpack.dumps(deduplicated_directory.to_dict())  # turn entry list into tuple
    )

    assert_checked_ranges(
        scrubber_db, config, EXPECTED_PARTITIONS, before_date, after_date
    )


def test_no_recheck(scrubber_db, datastore, swh_storage):
    """
    Tests that objects that were already checked are not checked again on
    the next run.
    """
    # check the whole (empty) storage with a given config
    config_id = scrubber_db.config_add("cfg2", datastore, swhids.ObjectType.SNAPSHOT, 1)
    StorageChecker(
        db=scrubber_db,
        storage=swh_storage,
        config_id=config_id,
    ).run()

    # Corrupt two snapshots and add them to the storage
    snapshots = list(swh_model_data.SNAPSHOTS)
    for i in (0, 1):
        snapshots[i] = attr.evolve(snapshots[i], id=bytes([i]) * 20)
    swh_storage.snapshot_add(snapshots)

    previous_partitions = set(scrubber_db.checked_partition_iter(config_id))

    # rerun a checker for the same config, it should be a noop
    StorageChecker(
        db=scrubber_db,
        storage=swh_storage,
        config_id=config_id,
    ).run()

    corrupt_objects = list(scrubber_db.corrupt_object_iter())
    assert (
        corrupt_objects == []
    ), "Detected corrupt objects in ranges that should have been skipped."

    # Make sure the DB was not changed (in particular, that timestamps were not bumped)
    assert set(scrubber_db.checked_partition_iter(config_id)) == previous_partitions


def test_no_hole(scrubber_db, datastore, swh_storage):
    swh_storage.content_add([CONTENT1])
    swh_storage.directory_add([DIRECTORY1, DIRECTORY2])
    swh_storage.revision_add([REVISION1])
    swh_storage.release_add([RELEASE1])
    swh_storage.snapshot_add([SNAPSHOT1])

    config = []
    for object_type in OBJECT_TYPES:
        config_id = scrubber_db.config_add("cfg2", datastore, object_type, 1)
        config.append((object_type, config_id))
        StorageChecker(
            db=scrubber_db,
            storage=swh_storage,
            config_id=config_id,
        ).run()

    assert list(scrubber_db.missing_object_iter()) == []

    assert_checked_ranges(scrubber_db, config, EXPECTED_PARTITIONS)


@pytest.mark.parametrize(
    "missing_object",
    ["content1", "directory1", "directory2", "revision1", "release1"],
)
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

    config = []
    for object_type in OBJECT_TYPES:
        config_id = scrubber_db.config_add(
            f"cfg_{object_type.name}", datastore, object_type, 1
        )
        config.append((object_type, config_id))
        StorageChecker(
            db=scrubber_db,
            storage=swh_storage,
            config_id=config_id,
        ).run()

    assert [mo.id for mo in scrubber_db.missing_object_iter()] == [missing_swhid]
    assert {
        (mor.missing_id, mor.reference_id)
        for mor in scrubber_db.missing_object_reference_iter(missing_swhid)
    } == {(missing_swhid, reference_swhid) for reference_swhid in reference_swhids}

    assert_checked_ranges(scrubber_db, config, EXPECTED_PARTITIONS)


def test_two_holes(scrubber_db, datastore, swh_storage):
    # missing content and revision
    swh_storage.directory_add([DIRECTORY1, DIRECTORY2])
    swh_storage.release_add([RELEASE1])
    swh_storage.snapshot_add([SNAPSHOT1])

    config = []
    for object_type in OBJECT_TYPES:
        config_id = scrubber_db.config_add(
            f"cfg_{object_type.name}", datastore, object_type, 1
        )
        config.append((object_type, config_id))
        StorageChecker(
            db=scrubber_db,
            storage=swh_storage,
            config_id=config_id,
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

    assert_checked_ranges(scrubber_db, config, EXPECTED_PARTITIONS)
