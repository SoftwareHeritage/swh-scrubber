# Copyright (C) 2020-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import tempfile
from unittest.mock import MagicMock, call

from click.testing import CliRunner
import yaml

from swh.model.swhids import CoreSWHID
from swh.scrubber.cli import scrubber_cli_group
from swh.scrubber.storage_checker import storage_db


def invoke(
    scrubber_db,
    args,
    storage=None,
    kafka_server=None,
    kafka_prefix=None,
    kafka_consumer_group=None,
):
    runner = CliRunner()

    config = {
        "scrubber_db": {"cls": "local", "db": scrubber_db.conn.dsn},
        "graph": {"url": "http://graph.example.org:5009/"},
    }
    if storage:
        with storage_db(storage) as db:
            config["storage"] = {
                "cls": "postgresql",
                "db": db.conn.dsn,
                "objstorage": {"cls": "memory"},
            }

    assert (
        (kafka_server is None)
        == (kafka_prefix is None)
        == (kafka_consumer_group is None)
    )
    if kafka_server:
        config["journal_client"] = dict(
            cls="kafka",
            brokers=kafka_server,
            group_id=kafka_consumer_group,
            prefix=kafka_prefix,
            stop_on_eof=True,
        )

    with tempfile.NamedTemporaryFile("a", suffix=".yml") as config_fd:
        yaml.dump(config, config_fd)
        config_fd.seek(0)
        args = ["-C" + config_fd.name] + list(args)
        result = runner.invoke(scrubber_cli_group, args, catch_exceptions=False)
    return result


def test_check_storage(mocker, scrubber_db, swh_storage):
    storage_checker = MagicMock()
    StorageChecker = mocker.patch(
        "swh.scrubber.storage_checker.StorageChecker", return_value=storage_checker
    )
    get_scrubber_db = mocker.patch(
        "swh.scrubber.get_scrubber_db", return_value=scrubber_db
    )
    result = invoke(
        scrubber_db, ["check", "storage", "--object-type=snapshot"], storage=swh_storage
    )
    assert result.exit_code == 0, result.output
    assert result.output == ""

    get_scrubber_db.assert_called_once_with(cls="local", db=scrubber_db.conn.dsn)
    StorageChecker.assert_called_once_with(
        db=scrubber_db,
        storage=StorageChecker.mock_calls[0][2]["storage"],
        object_type="snapshot",
        start_object="0" * 40,
        end_object="f" * 40,
    )
    assert storage_checker.method_calls == [call.run()]


def test_check_journal(
    mocker, scrubber_db, kafka_server, kafka_prefix, kafka_consumer_group
):
    journal_checker = MagicMock()
    JournalChecker = mocker.patch(
        "swh.scrubber.journal_checker.JournalChecker", return_value=journal_checker
    )
    get_scrubber_db = mocker.patch(
        "swh.scrubber.get_scrubber_db", return_value=scrubber_db
    )
    result = invoke(
        scrubber_db,
        ["check", "journal"],
        kafka_server=kafka_server,
        kafka_prefix=kafka_prefix,
        kafka_consumer_group=kafka_consumer_group,
    )
    assert result.exit_code == 0, result.output
    assert result.output == ""

    get_scrubber_db.assert_called_once_with(cls="local", db=scrubber_db.conn.dsn)
    JournalChecker.assert_called_once_with(
        db=scrubber_db,
        journal_client={
            "brokers": kafka_server,
            "cls": "kafka",
            "group_id": kafka_consumer_group,
            "prefix": kafka_prefix,
            "stop_on_eof": True,
        },
    )
    assert journal_checker.method_calls == [call.run()]


def test_locate_origins(mocker, scrubber_db, swh_storage):
    origin_locator = MagicMock()
    OriginLocator = mocker.patch(
        "swh.scrubber.origin_locator.OriginLocator", return_value=origin_locator
    )
    get_scrubber_db = mocker.patch(
        "swh.scrubber.get_scrubber_db", return_value=scrubber_db
    )
    result = invoke(scrubber_db, ["locate"], storage=swh_storage)
    assert result.exit_code == 0, result.output
    assert result.output == ""

    get_scrubber_db.assert_called_once_with(cls="local", db=scrubber_db.conn.dsn)
    OriginLocator.assert_called_once_with(
        db=scrubber_db,
        storage=OriginLocator.mock_calls[0][2]["storage"],
        graph=OriginLocator.mock_calls[0][2]["graph"],
        start_object=CoreSWHID.from_string("swh:1:cnt:" + "00" * 20),
        end_object=CoreSWHID.from_string("swh:1:snp:" + "ff" * 20),
    )
    assert origin_locator.method_calls == [call.run()]


def test_fix_objects(mocker, scrubber_db):
    fixer = MagicMock()
    Fixer = mocker.patch("swh.scrubber.fixer.Fixer", return_value=fixer)
    get_scrubber_db = mocker.patch(
        "swh.scrubber.get_scrubber_db", return_value=scrubber_db
    )
    result = invoke(scrubber_db, ["fix"])
    assert result.exit_code == 0, result.output
    assert result.output == ""

    get_scrubber_db.assert_called_once_with(cls="local", db=scrubber_db.conn.dsn)
    Fixer.assert_called_once_with(
        db=scrubber_db,
        start_object=CoreSWHID.from_string("swh:1:cnt:" + "00" * 20),
        end_object=CoreSWHID.from_string("swh:1:snp:" + "ff" * 20),
    )
    assert fixer.method_calls == [call.run()]
