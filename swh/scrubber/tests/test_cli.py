# Copyright (C) 2020-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import tempfile
from unittest.mock import MagicMock

from click.testing import CliRunner
import yaml

from swh.scrubber.check_storage import storage_db
from swh.scrubber.cli import scrubber_cli_group


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
        "swh.scrubber.check_storage.StorageChecker", return_value=storage_checker
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


def test_check_journal(
    mocker, scrubber_db, kafka_server, kafka_prefix, kafka_consumer_group
):
    journal_checker = MagicMock()
    JournalChecker = mocker.patch(
        "swh.scrubber.check_journal.JournalChecker", return_value=journal_checker
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
