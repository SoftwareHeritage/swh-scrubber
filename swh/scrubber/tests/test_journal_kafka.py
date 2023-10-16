# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime

import attr
import pytest

from swh.journal.serializers import kafka_to_value
from swh.journal.writer import get_journal_writer
from swh.model import swhids
from swh.model.swhids import ObjectType
from swh.model.tests import swh_model_data
from swh.scrubber.db import Datastore
from swh.scrubber.journal_checker import JournalChecker, get_datastore


def journal_client_config(
    kafka_server: str, kafka_prefix: str, kafka_consumer_group: str
):
    return dict(
        cls="kafka",
        brokers=kafka_server,
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        on_eof="stop",
    )


def journal_writer(kafka_server: str, kafka_prefix: str):
    return get_journal_writer(
        cls="kafka",
        brokers=[kafka_server],
        client_id="kafka_writer",
        prefix=kafka_prefix,
        anonymize=False,
    )


@pytest.fixture
def datastore(
    kafka_server: str, kafka_prefix: str, kafka_consumer_group: str
) -> Datastore:
    journal_config = journal_client_config(
        kafka_server, kafka_prefix, kafka_consumer_group
    )
    datastore = get_datastore(journal_config)
    return datastore


def test_no_corruption(
    scrubber_db, kafka_server, kafka_prefix, kafka_consumer_group, datastore
):
    writer = journal_writer(kafka_server, kafka_prefix)
    writer.write_additions("directory", swh_model_data.DIRECTORIES)
    writer.write_additions("revision", swh_model_data.REVISIONS)
    writer.write_additions("release", swh_model_data.RELEASES)
    writer.write_additions("snapshot", swh_model_data.SNAPSHOTS)

    journal_cfg = journal_client_config(
        kafka_server, kafka_prefix, kafka_consumer_group
    )
    gid = journal_cfg["group_id"] + "_"

    for object_type in ("directory", "revision", "release", "snapshot"):
        journal_cfg["group_id"] = gid + object_type
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
            journal=journal_cfg,
        )
        jc.run()
        jc.journal_client.close()

    assert list(scrubber_db.corrupt_object_iter()) == []


@pytest.mark.parametrize("corrupt_idx", range(len(swh_model_data.SNAPSHOTS)))
def test_corrupt_snapshot(
    scrubber_db,
    kafka_server,
    kafka_prefix,
    kafka_consumer_group,
    datastore,
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

    writer = journal_writer(kafka_server, kafka_prefix)
    writer.write_additions("snapshot", snapshots)

    before_date = datetime.datetime.now(tz=datetime.timezone.utc)
    JournalChecker(
        db=scrubber_db,
        config_id=config_id,
        journal=journal_client_config(kafka_server, kafka_prefix, kafka_consumer_group),
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
    kafka_server,
    kafka_prefix,
    kafka_consumer_group,
    datastore,
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

    writer = journal_writer(kafka_server, kafka_prefix)
    writer.write_additions("snapshot", snapshots)

    JournalChecker(
        db=scrubber_db,
        config_id=config_id,
        journal=journal_client_config(kafka_server, kafka_prefix, kafka_consumer_group),
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


def test_check_references_raises(
    scrubber_db,
    kafka_server,
    kafka_prefix,
    kafka_consumer_group,
    datastore,
):
    config_id = scrubber_db.config_add(
        name="cfg_snapshot",
        datastore=datastore,
        object_type=ObjectType.SNAPSHOT,
        nb_partitions=1,
        check_references=True,
    )
    journal_config = journal_client_config(
        kafka_server, kafka_prefix, kafka_consumer_group
    )
    with pytest.raises(ValueError):
        JournalChecker(
            db=scrubber_db,
            config_id=config_id,
            journal=journal_config,
        )
