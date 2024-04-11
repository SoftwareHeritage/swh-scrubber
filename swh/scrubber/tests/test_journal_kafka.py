# Copyright (C) 2022-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import hashlib

import attr
import pytest

from swh.journal.serializers import kafka_to_value
from swh.model import model, swhids
from swh.model.swhids import ObjectType
from swh.model.tests import swh_model_data
from swh.scrubber.db import Datastore
from swh.scrubber.journal_checker import JournalChecker, get_datastore


@pytest.fixture
def datastore(journal_client_config) -> Datastore:
    return get_datastore(journal_client_config)


def test_no_corruption(
    scrubber_db,
    datastore,
    journal_writer,
    journal_client_config,
):
    journal_writer.write_additions("directory", swh_model_data.DIRECTORIES)
    journal_writer.write_additions("revision", swh_model_data.REVISIONS)
    journal_writer.write_additions("release", swh_model_data.RELEASES)
    journal_writer.write_additions("snapshot", swh_model_data.SNAPSHOTS)

    gid = journal_client_config["group_id"] + "_"

    for object_type in ("directory", "revision", "release", "snapshot"):
        journal_client_config["group_id"] = gid + object_type
        config_id = scrubber_db.config_add(
            name=f"cfg_{object_type}",
            datastore=datastore,
            object_type=getattr(ObjectType, object_type.upper()),
            nb_partitions=1,
            check_references=False,
        )
        jc = JournalChecker(
            db=scrubber_db,
            config_id=config_id,
            journal_client_config=journal_client_config,
        )
        jc.run()
        jc.journal_client.close()

    assert list(scrubber_db.corrupt_object_iter()) == []


@pytest.mark.parametrize("corrupt_idx", range(len(swh_model_data.SNAPSHOTS)))
def test_corrupt_snapshot(
    scrubber_db,
    datastore,
    journal_writer,
    journal_client_config,
    corrupt_idx,
):
    config_id = scrubber_db.config_add(
        name="cfg_snapshot",
        datastore=datastore,
        object_type=ObjectType.SNAPSHOT,
        nb_partitions=1,
        check_references=False,
    )
    snapshots = list(swh_model_data.SNAPSHOTS)
    snapshots[corrupt_idx] = attr.evolve(snapshots[corrupt_idx], id=b"\x00" * 20)

    journal_writer.write_additions("snapshot", snapshots)

    before_date = datetime.datetime.now(tz=datetime.timezone.utc)
    JournalChecker(
        db=scrubber_db,
        config_id=config_id,
        journal_client_config=journal_client_config,
    ).run()
    after_date = datetime.datetime.now(tz=datetime.timezone.utc)

    corrupt_objects = list(scrubber_db.corrupt_object_iter())
    assert len(corrupt_objects) == 1
    assert corrupt_objects[0].id == swhids.CoreSWHID.from_string(
        "swh:1:snp:0000000000000000000000000000000000000000"
    )
    assert corrupt_objects[0].config.datastore.package == "journal"
    assert corrupt_objects[0].config.datastore.cls == "kafka"
    assert (
        before_date - datetime.timedelta(seconds=5)
        <= corrupt_objects[0].first_occurrence
        <= after_date + datetime.timedelta(seconds=5)
    )
    assert (
        kafka_to_value(corrupt_objects[0].object_) == snapshots[corrupt_idx].to_dict()
    )


def test_corrupt_snapshots(
    scrubber_db,
    datastore,
    journal_writer,
    journal_client_config,
):
    config_id = scrubber_db.config_add(
        name="cfg_snapshot",
        datastore=datastore,
        object_type=ObjectType.SNAPSHOT,
        nb_partitions=1,
        check_references=False,
    )
    snapshots = list(swh_model_data.SNAPSHOTS)
    for i in (0, 1):
        snapshots[i] = attr.evolve(snapshots[i], id=bytes([i]) * 20)

    journal_writer.write_additions("snapshot", snapshots)

    JournalChecker(
        db=scrubber_db,
        config_id=config_id,
        journal_client_config=journal_client_config,
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


def test_duplicate_directory_entries(
    scrubber_db,
    datastore,
    journal_writer,
    kafka_prefix,
    journal_client_config,
):
    config_id = scrubber_db.config_add(
        name="cfg_directory",
        datastore=datastore,
        object_type=ObjectType.DIRECTORY,
        nb_partitions=1,
        check_references=False,
    )
    directory = model.Directory(
        entries=(
            model.DirectoryEntry(
                name=b"filename", type="file", target=b"\x01" * 20, perms=0
            ),
        )
    )

    # has duplicate entries and wrong hash
    corrupt_directory = {
        "id": b"\x00" * 20,
        "entries": [
            {"name": b"filename", "type": "file", "target": b"\x01" * 20, "perms": 0},
            {"name": b"filename", "type": "file", "target": b"\x02" * 20, "perms": 0},
        ],
    }

    # has duplicate entries but correct hash
    raw_manifest = (
        b"tree 62\x00"
        + b"0 filename\x00"
        + b"\x01" * 20
        + b"0 filename\x00"
        + b"\x02" * 20
    )
    dup_directory = {
        "id": hashlib.sha1(raw_manifest).digest(),
        "entries": corrupt_directory["entries"],
        "raw_manifest": raw_manifest,
    }

    journal_writer.send(f"{kafka_prefix}.directory", directory.id, directory.to_dict())
    journal_writer.send(
        f"{kafka_prefix}.directory", corrupt_directory["id"], corrupt_directory
    )
    journal_writer.send(f"{kafka_prefix}.directory", dup_directory["id"], dup_directory)

    JournalChecker(
        db=scrubber_db,
        config_id=config_id,
        journal_client_config=journal_client_config,
    ).run()

    corrupt_objects = list(scrubber_db.corrupt_object_iter())
    assert len(corrupt_objects) == 2
    assert {co.id for co in corrupt_objects} == {
        swhids.CoreSWHID.from_string(swhid)
        for swhid in [
            "swh:1:dir:0000000000000000000000000000000000000000",
            f"swh:1:dir:{dup_directory['id'].hex()}",
        ]
    }


def test_check_references_raises(
    scrubber_db,
    datastore,
    journal_client_config,
):
    config_id = scrubber_db.config_add(
        name="cfg_snapshot",
        datastore=datastore,
        object_type=ObjectType.SNAPSHOT,
        nb_partitions=1,
        check_references=True,
    )

    with pytest.raises(ValueError):
        JournalChecker(
            db=scrubber_db,
            config_id=config_id,
            journal_client_config=journal_client_config,
        )
