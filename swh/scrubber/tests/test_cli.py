# Copyright (C) 2020-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import tempfile
import traceback
from unittest.mock import MagicMock, call

from click.testing import CliRunner
import yaml

from swh.model.swhids import CoreSWHID
from swh.scrubber.cli import scrubber_cli_group
from swh.scrubber.storage_checker import postgresql_storage_db


def assert_result(result):
    if result.exception:
        assert result.exit_code == 0, (
            "Unexpected exception: "
            f"{''.join(traceback.format_tb(result.exc_info[2]))}"
            f"\noutput: {result.output}"
        )
    else:
        assert result.exit_code == 0, f"Unexpected output: {result.output}"


def invoke(
    scrubber_db,
    args,
    storage=None,
    objstorage=None,
    kafka_server=None,
    kafka_prefix=None,
    kafka_consumer_group=None,
):
    runner = CliRunner()

    config = {
        "scrubber": {"cls": "postgresql", "db": scrubber_db.conn.dsn},
        "graph": {"url": "http://graph.example.org:5009/"},
    }
    if storage:
        with postgresql_storage_db(storage) as db:
            config["storage"] = {
                "cls": "postgresql",
                "db": db.conn.dsn,
                "objstorage": {"cls": "memory"},
            }
    if objstorage:
        config["objstorage"] = {"cls": "memory"}

    assert (
        (kafka_server is None)
        == (kafka_prefix is None)
        == (kafka_consumer_group is None)
    )
    if kafka_server:
        config["journal"] = dict(
            cls="kafka",
            brokers=kafka_server,
            group_id=kafka_consumer_group,
            prefix=kafka_prefix,
            on_eof="stop",
        )

    with tempfile.NamedTemporaryFile("a", suffix=".yml") as config_fd:
        yaml.dump(config, config_fd)
        config_fd.seek(0)
        args = ["-C" + config_fd.name] + list(args)
        result = runner.invoke(scrubber_cli_group, args, catch_exceptions=False)
    return result


def test_help_main(mocker, scrubber_db, swh_storage):
    result = invoke(
        scrubber_db,
        [
            "--help",
        ],
    )
    assert_result(result)
    output = result.output.splitlines(keepends=False)
    msg = "Usage: scrubber [OPTIONS] COMMAND [ARGS]..."
    assert output[0] == msg
    assert "Commands:" in output
    commands = [cmd.split()[0] for cmd in output[output.index("Commands:") + 1 :]]
    assert commands == ["check", "fix", "locate"]


def test_help_check(mocker, scrubber_db, swh_storage):
    result = invoke(
        scrubber_db,
        [
            "check",
            "--help",
        ],
    )
    assert_result(result)
    output = result.output.splitlines(keepends=False)
    msg = "Usage: scrubber check [OPTIONS] COMMAND [ARGS]..."
    assert output[0] == msg
    assert "Commands:" in output
    commands = [cmd.split()[0] for cmd in output[output.index("Commands:") + 1 :]]
    # With older click version (e.g. 7.0-1), the text wrapping can be different,
    # resulting in some docstring text included in this command list, so check we find
    # the expected commands instead
    for command in ["init", "list", "run", "stalled"]:
        assert command in commands
    for command in ["storage", "journal"]:
        assert command not in commands

    # without a config file, --help should still work
    result = CliRunner().invoke(
        scrubber_cli_group, ["check", "--help"], catch_exceptions=False
    )
    output = result.output.splitlines(keepends=False)
    msg = "Usage: scrubber check [OPTIONS] COMMAND [ARGS]..."
    assert output[0] == msg
    assert "Commands:" in output
    commands = [cmd.split()[0] for cmd in output[output.index("Commands:") + 1 :]]
    # With older click version (e.g. 7.0-1), the text wrapping can be different,
    # resulting in some docstring text included in this command list, so check we find
    # the expected commands instead
    for command in ["init", "list", "run", "stalled"]:
        assert command in commands


def test_check_init_storage(mocker, scrubber_db, swh_storage):
    mocker.patch("swh.scrubber.get_scrubber_db", return_value=scrubber_db)
    result = invoke(
        scrubber_db,
        [
            "check",
            "init",
            "storage",
            "--object-type",
            "snapshot",
            "--nb-partitions",
            "4",
            "--name",
            "cfg1",
        ],
        storage=swh_storage,
    )
    assert_result(result)
    msg = "Created configuration cfg1 [1] for checking snapshot in postgresql storage"
    assert result.output.strip() == msg

    # error: cfg name already exists
    result = invoke(
        scrubber_db,
        [
            "check",
            "init",
            "storage",
            "--object-type",
            "snapshot",
            "--nb-partitions",
            "8",
            "--name",
            "cfg1",
        ],
        storage=swh_storage,
    )
    assert result.exit_code == 1, result.output
    msg = "Error: Configuration cfg1 already exists"
    assert result.output.strip() == msg


def test_check_init_storage_flags(mocker, scrubber_db, swh_storage):
    mocker.patch("swh.scrubber.get_scrubber_db", return_value=scrubber_db)
    arg_list = [
        "check",
        "init",
        "storage",
        "--object-type",
        "snapshot",
        "--nb-partitions",
        "4",
        "--name",
    ]

    name = "cfg1"
    result = invoke(
        scrubber_db,
        arg_list + [name],
        storage=swh_storage,
    )
    assert_result(result)

    cfg_entry = scrubber_db.config_get(scrubber_db.config_get_by_name(name))
    assert cfg_entry.check_hashes is True
    assert cfg_entry.check_references is True

    name = "cfg2"
    result = invoke(
        scrubber_db,
        arg_list + [name, "--no-check-references"],
        storage=swh_storage,
    )
    assert_result(result)

    cfg_entry = scrubber_db.config_get(scrubber_db.config_get_by_name(name))
    assert cfg_entry.check_hashes is True
    assert cfg_entry.check_references is False

    name = "cfg3"
    result = invoke(
        scrubber_db,
        arg_list + [name, "--no-check-hashes"],
        storage=swh_storage,
    )
    assert_result(result)

    cfg_entry = scrubber_db.config_get(scrubber_db.config_get_by_name(name))
    assert cfg_entry.check_hashes is False
    assert cfg_entry.check_references is True


def test_check_init_objstorage(mocker, scrubber_db, swh_storage, swh_objstorage):
    config_name = "cfg1"
    mocker.patch("swh.scrubber.get_scrubber_db", return_value=scrubber_db)
    result = invoke(
        scrubber_db,
        [
            "check",
            "init",
            "objstorage",
            "--object-type",
            "content",
            "--nb-partitions",
            "4",
            "--name",
            config_name,
        ],
        storage=swh_storage,
        objstorage=swh_objstorage,
    )
    assert_result(result)
    msg = f"Created configuration {config_name} [1] for checking content in memory objstorage"
    assert result.output.strip() == msg

    cfg_entry = scrubber_db.config_get(scrubber_db.config_get_by_name(config_name))
    assert cfg_entry.check_hashes is True
    assert cfg_entry.check_references is True

    # error: config name already exists
    result = invoke(
        scrubber_db,
        [
            "check",
            "init",
            "objstorage",
            "--object-type",
            "content",
            "--nb-partitions",
            "8",
            "--name",
            config_name,
        ],
        storage=swh_storage,
        objstorage=swh_objstorage,
    )
    assert result.exit_code == 1, result.output
    msg = f"Error: Configuration {config_name} already exists"
    assert result.output.strip() == msg

    # error: missing objstorage config
    result = invoke(
        scrubber_db,
        [
            "check",
            "init",
            "objstorage",
            "--object-type",
            "content",
            "--nb-partitions",
            "8",
            "--name",
            config_name,
        ],
        storage=swh_storage,
    )
    assert result.exit_code == 1, result.output
    msg = "Error: You must have an object storage configured in your config file."
    assert result.output.strip() == msg


def test_check_init_journal_flags(
    mocker, scrubber_db, kafka_server, kafka_prefix, kafka_consumer_group
):
    mocker.patch("swh.scrubber.get_scrubber_db", return_value=scrubber_db)
    arg_list = [
        "check",
        "init",
        "journal",
        "--object-type",
        "snapshot",
        "--name",
    ]

    name = "cfg1"
    result = invoke(
        scrubber_db,
        arg_list + [name],
        kafka_server=kafka_server,
        kafka_prefix=kafka_prefix,
        kafka_consumer_group=kafka_consumer_group,
    )
    assert_result(result)

    cfg_entry = scrubber_db.config_get(scrubber_db.config_get_by_name(name))
    assert cfg_entry.check_hashes is True
    assert cfg_entry.check_references is False


def test_check_run_ko(mocker, scrubber_db, swh_storage):
    # using the config id instead of the config name
    result = invoke(scrubber_db, ["check", "run"], storage=swh_storage)
    assert result.exit_code == 2, result.output
    assert "Error: A valid configuration name/id must be given." in result.output


def test_check_run_storage(mocker, scrubber_db, swh_storage):
    storage_checker = MagicMock()
    StorageChecker = mocker.patch(
        "swh.scrubber.storage_checker.StorageChecker", return_value=storage_checker
    )
    get_scrubber_db = mocker.patch(
        "swh.scrubber.get_scrubber_db", return_value=scrubber_db
    )
    result = invoke(
        scrubber_db,
        [
            "check",
            "init",
            "storage",
            "--object-type",
            "snapshot",
            "--nb-partitions",
            "4",
            "--name",
            "cfg1",
        ],
        storage=swh_storage,
    )
    assert_result(result)
    msg = "Created configuration cfg1 [1] for checking snapshot in postgresql storage"
    assert result.output.strip() == msg

    result = invoke(scrubber_db, ["check", "run", "cfg1"], storage=swh_storage)
    assert_result(result)
    assert result.output == ""

    get_scrubber_db.assert_called_with(cls="postgresql", db=scrubber_db.conn.dsn)
    StorageChecker.assert_called_once_with(
        db=scrubber_db,
        config_id=1,
        storage=StorageChecker.mock_calls[0][2]["storage"],
        limit=0,
    )
    assert storage_checker.method_calls == [call.run()]

    # using the config id instead of the config name
    result = invoke(
        scrubber_db, ["check", "run", "--config-id", "1"], storage=swh_storage
    )
    assert_result(result)
    assert result.output == ""


def test_check_run_objstorage_partition(
    mocker, scrubber_db, swh_storage, swh_objstorage
):
    config_name = "cfg1"
    objstorage_checker = MagicMock()
    ObjectStorageCheckerFromStoragePartition = mocker.patch(
        "swh.scrubber.objstorage_checker.ObjectStorageCheckerFromStoragePartition",
        return_value=objstorage_checker,
    )
    get_scrubber_db = mocker.patch(
        "swh.scrubber.get_scrubber_db", return_value=scrubber_db
    )
    result = invoke(
        scrubber_db,
        [
            "check",
            "init",
            "objstorage",
            "--object-type",
            "content",
            "--nb-partitions",
            "4",
            "--name",
            config_name,
        ],
        storage=swh_storage,
        objstorage=swh_objstorage,
    )
    assert_result(result)
    msg = f"Created configuration {config_name} [1] for checking content in memory objstorage"
    assert result.output.strip() == msg

    result = invoke(
        scrubber_db,
        ["check", "run", config_name],
        storage=swh_storage,
        objstorage=swh_objstorage,
    )
    assert_result(result)
    assert result.output == ""

    get_scrubber_db.assert_called_with(cls="postgresql", db=scrubber_db.conn.dsn)
    ObjectStorageCheckerFromStoragePartition.assert_called_once_with(
        db=scrubber_db,
        config_id=1,
        storage=ObjectStorageCheckerFromStoragePartition.mock_calls[0][2]["storage"],
        objstorage=ObjectStorageCheckerFromStoragePartition.mock_calls[0][2][
            "objstorage"
        ],
        limit=0,
    )
    assert objstorage_checker.method_calls == [call.run()]

    # using the config id instead of the config name
    result = invoke(
        scrubber_db,
        ["check", "run", "--config-id", "1"],
        storage=swh_storage,
        objstorage=swh_objstorage,
    )
    assert_result(result)
    assert result.output == ""


def test_check_run_objstorage_journal(
    mocker,
    scrubber_db,
    kafka_server,
    kafka_prefix,
    kafka_consumer_group,
    swh_objstorage,
):
    config_name = "cfg1"
    journal_checker = MagicMock()
    ObjectStorageCheckerFromJournal = mocker.patch(
        "swh.scrubber.objstorage_checker.ObjectStorageCheckerFromJournal",
        return_value=journal_checker,
    )
    get_scrubber_db = mocker.patch(
        "swh.scrubber.get_scrubber_db", return_value=scrubber_db
    )
    result = invoke(
        scrubber_db,
        [
            "check",
            "init",
            "objstorage",
            "--object-type",
            "content",
            "--name",
            config_name,
        ],
        kafka_server=kafka_server,
        kafka_prefix=kafka_prefix,
        kafka_consumer_group=kafka_consumer_group,
        objstorage=swh_objstorage,
    )
    assert_result(result)
    msg = f"Created configuration {config_name} [1] for checking content in memory objstorage"
    assert result.output.strip() == msg

    result = invoke(
        scrubber_db,
        ["check", "run", "--use-journal", config_name],
        kafka_server=kafka_server,
        kafka_prefix=kafka_prefix,
        kafka_consumer_group=kafka_consumer_group,
        objstorage=swh_objstorage,
    )
    assert_result(result)
    assert result.output == ""

    assert get_scrubber_db.call_count == 2
    get_scrubber_db.assert_called_with(cls="postgresql", db=scrubber_db.conn.dsn)

    ObjectStorageCheckerFromJournal.assert_called_once_with(
        db=scrubber_db,
        journal_client_config={
            "brokers": kafka_server,
            "cls": "kafka",
            "group_id": kafka_consumer_group,
            "prefix": kafka_prefix,
            "on_eof": "stop",
        },
        config_id=1,
        objstorage=ObjectStorageCheckerFromJournal.mock_calls[0][2]["objstorage"],
    )
    assert journal_checker.method_calls == [call.run()]


def test_check_run_journal(
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
        [
            "check",
            "init",
            "journal",
            "--object-type",
            "snapshot",
            "--nb-partitions",
            "4",
            "--name",
            "cfg1",
        ],
        kafka_server=kafka_server,
        kafka_prefix=kafka_prefix,
        kafka_consumer_group=kafka_consumer_group,
    )
    assert_result(result)
    msg = "Created configuration cfg1 [1] for checking snapshot in kafka journal"
    assert result.output.strip() == msg

    result = invoke(
        scrubber_db,
        ["check", "run", "cfg1"],
        kafka_server=kafka_server,
        kafka_prefix=kafka_prefix,
        kafka_consumer_group=kafka_consumer_group,
    )
    assert_result(result)
    assert result.output == ""

    assert get_scrubber_db.call_count == 2
    get_scrubber_db.assert_called_with(cls="postgresql", db=scrubber_db.conn.dsn)

    JournalChecker.assert_called_once_with(
        db=scrubber_db,
        journal_client_config={
            "brokers": kafka_server,
            "cls": "kafka",
            "group_id": kafka_consumer_group,
            "prefix": kafka_prefix,
            "on_eof": "stop",
        },
        config_id=1,
    )
    assert journal_checker.method_calls == [call.run()]


def test_check_list(mocker, scrubber_db, swh_storage):
    mocker.patch("swh.scrubber.get_scrubber_db", return_value=scrubber_db)
    result = invoke(scrubber_db, ["check", "list"], storage=swh_storage)
    assert_result(result)
    assert result.output == ""
    with swh_storage.db() as db:
        dsn = db.conn.dsn

    result = invoke(
        scrubber_db,
        [
            "check",
            "init",
            "storage",
            "--object-type",
            "snapshot",
            "--nb-partitions",
            "4",
            "--name",
            "cfg1",
        ],
        storage=swh_storage,
    )
    assert_result(result)

    result = invoke(scrubber_db, ["check", "list"], storage=swh_storage)
    assert_result(result)
    expected = f"[1] cfg1: snapshot, 4, storage:postgresql ({dsn})\n"
    assert result.output == expected, result.output


def test_check_stalled(mocker, scrubber_db, swh_storage):
    mocker.patch("swh.scrubber.get_scrubber_db", return_value=scrubber_db)
    result = invoke(scrubber_db, ["check", "list"], storage=swh_storage)
    assert_result(result)
    assert result.output == ""

    result = invoke(
        scrubber_db,
        [
            "check",
            "init",
            "storage",
            "--object-type",
            "snapshot",
            "--nb-partitions",
            "4",
            "--name",
            "cfg1",
        ],
        storage=swh_storage,
    )
    assert_result(result)

    result = invoke(scrubber_db, ["check", "stalled", "cfg1"], storage=swh_storage)
    assert_result(result)
    expected = "No stuck partition found for cfg1 [id=1, type=snapshot]\n"
    assert result.output == expected, result.output

    # insert a partition started 20mn ago
    with scrubber_db.transaction() as cur:
        cur.execute(
            "INSERT INTO checked_partition VALUES (1, 0, now() - '20m'::interval, NULL);"
        )

    # there are no existing completed partition, defaults to 1h to be considered as stalled
    # so a partition just added is not stalled
    result = invoke(scrubber_db, ["check", "stalled", "cfg1"], storage=swh_storage)
    assert_result(result)
    expected = "No stuck partition found for cfg1 [id=1, type=snapshot]\n"
    assert result.output == expected, result.output

    # insert a partition started 2 hours from now
    with scrubber_db.transaction() as cur:
        cur.execute(
            "INSERT INTO checked_partition VALUES (1, 1, now() - '2h'::interval, NULL);"
        )
    # it is considered as stalled by default
    result = invoke(scrubber_db, ["check", "stalled", "cfg1"], storage=swh_storage)
    assert_result(result)
    expected = """\
Stuck partitions for cfg1 [id=1, type=snapshot]:
1:	stuck since today (2 hours)
"""
    assert result.output == expected, result.output

    # explicitly specify a delay > 2h to be considered as stelles: no one stalled
    result = invoke(
        scrubber_db, ["check", "stalled", "--for", "8000", "cfg1"], storage=swh_storage
    )
    assert_result(result)
    expected = "No stuck partition found for cfg1 [id=1, type=snapshot]\n"
    assert result.output == expected, result.output

    # insert a transaction that took 5mn to run
    with scrubber_db.transaction() as cur:
        cur.execute(
            "INSERT INTO checked_partition "
            "VALUES (1, 2, now() - '2h'::interval, now() - '1h55m'::interval);"
        )
    # so now both partitions 0 and 1 should be considered a stalled
    result = invoke(scrubber_db, ["check", "stalled", "cfg1"], storage=swh_storage)
    assert_result(result)
    expected = """\
Stuck partitions for cfg1 [id=1, type=snapshot]:
0:	stuck since today (20 minutes)
1:	stuck since today (2 hours)
"""
    assert result.output == expected, result.output


def test_check_running(mocker, scrubber_db, swh_storage):
    mocker.patch("swh.scrubber.get_scrubber_db", return_value=scrubber_db)
    result = invoke(scrubber_db, ["check", "list"], storage=swh_storage)
    assert_result(result)
    assert result.output == ""

    result = invoke(
        scrubber_db,
        [
            "check",
            "init",
            "storage",
            "--object-type",
            "snapshot",
            "--nb-partitions",
            "4",
            "--name",
            "cfg1",
        ],
        storage=swh_storage,
    )
    assert_result(result)

    result = invoke(scrubber_db, ["check", "running", "cfg1"], storage=swh_storage)
    assert_result(result)
    expected = "No running partition found for cfg1 [id=1, type=snapshot]\n"
    assert result.output == expected, result.output

    # insert a partition started 20mn ago
    with scrubber_db.transaction() as cur:
        cur.execute(
            "INSERT INTO checked_partition VALUES (1, 0, now() - '20m'::interval, NULL);"
        )
    result = invoke(scrubber_db, ["check", "running", "cfg1"], storage=swh_storage)
    assert_result(result)
    expected = """\
Running partitions for cfg1 [id=1, type=snapshot]:
0:	running since today (20 minutes)
"""
    assert result.output == expected, result.output

    # insert another partition started 2 hours ago
    with scrubber_db.transaction() as cur:
        cur.execute(
            "INSERT INTO checked_partition VALUES (1, 1, now() - '2h'::interval, NULL);"
        )
    result = invoke(scrubber_db, ["check", "running", "cfg1"], storage=swh_storage)
    assert_result(result)
    expected = """\
Running partitions for cfg1 [id=1, type=snapshot]:
0:	running since today (20 minutes)
1:	running since today (2 hours)
"""
    assert result.output == expected, result.output

    # insert another partition whose scrubbing did not start yet
    with scrubber_db.transaction() as cur:
        cur.execute("INSERT INTO checked_partition VALUES (1, 2, NULL, NULL);")

    result = invoke(scrubber_db, ["check", "running", "cfg1"], storage=swh_storage)

    # not scrubbed partition should not be displayed
    assert_result(result)
    assert result.output == expected, result.output


def test_check_stats(mocker, scrubber_db, swh_storage):
    from swh.scrubber.storage_checker import get_datastore

    mocker.patch("swh.scrubber.get_scrubber_db", return_value=scrubber_db)
    result = invoke(scrubber_db, ["check", "list"], storage=swh_storage)
    assert_result(result)
    assert result.output == ""

    for otype in ("snapshot", "revision", "release"):
        result = invoke(
            scrubber_db,
            [
                "check",
                "init",
                "storage",
                "--object-type",
                otype,
                "--nb-partitions",
                "4",
                "--name",
                f"cfg_{otype}",
            ],
            storage=swh_storage,
        )
        assert_result(result)

    result = invoke(
        scrubber_db, ["check", "stats", "cfg_snapshot"], storage=swh_storage
    )
    assert_result(result)

    for otype in ("snapshot", "revision", "release"):
        result = invoke(
            scrubber_db,
            ["check", "stats", "--json", f"cfg_{otype}"],
            storage=swh_storage,
        )
        stats = json.loads(result.output)
        assert stats == {
            "config": {
                "name": f"cfg_{otype}",
                "datastore": {
                    "package": "storage",
                    "cls": "postgresql",
                    "instance": get_datastore(swh_storage).instance,
                },
                "object_type": otype,
                "nb_partitions": 4,
                "check_hashes": True,
                "check_references": True,
            },
            "min_duration": 0,
            "max_duration": 0,
            "avg_duration": 0,
            "checked_partition": 0,
            "running_partition": 0,
            "missing_object": 0,
            "missing_object_reference": 0,
            "corrupt_object": 0,
        }

    with scrubber_db.transaction() as cur:
        # insert a pair of checked partitions
        cur.execute(
            "INSERT INTO checked_partition "
            "VALUES (1, 0, now() - '40m'::interval, now() - '20m'::interval)"
        )
        cur.execute(
            "INSERT INTO checked_partition "
            "VALUES (1, 1, now() - '20m'::interval, now() - '10m'::interval)"
        )
        # and a pair of running ones
        cur.execute(
            "INSERT INTO checked_partition "
            "VALUES (1, 2, now() - '10m'::interval, NULL)"
        )
        cur.execute(
            "INSERT INTO checked_partition "
            "VALUES (1, 3, now() - '20m'::interval, NULL)"
        )
        # also add a checked partitions for another config entry
        cur.execute(
            "INSERT INTO checked_partition "
            "VALUES (2, 0, now() - '40m'::interval, now() - '20m'::interval)"
        )
        cur.execute(
            "INSERT INTO checked_partition "
            "VALUES (2, 1, now() - '40m'::interval, now() - '20m'::interval)"
        )
        # and add a running checker for another config entry
        cur.execute(
            "INSERT INTO checked_partition "
            "VALUES (2, 3, now() - '20m'::interval, NULL)"
        )
    result = invoke(
        scrubber_db, ["check", "stats", "-j", "cfg_snapshot"], storage=swh_storage
    )
    assert_result(result)
    stats = json.loads(result.output)
    assert stats["config"]["name"] == "cfg_snapshot"
    assert stats["min_duration"] == 600
    assert stats["max_duration"] == 1200
    assert stats["avg_duration"] == 900
    assert stats["checked_partition"] == 2
    assert stats["running_partition"] == 2


def test_check_reset(mocker, scrubber_db, swh_storage):
    mocker.patch("swh.scrubber.get_scrubber_db", return_value=scrubber_db)
    result = invoke(scrubber_db, ["check", "list"], storage=swh_storage)
    assert_result(result)
    assert result.output == ""

    result = invoke(
        scrubber_db,
        [
            "check",
            "init",
            "storage",
            "--object-type",
            "snapshot",
            "--nb-partitions",
            "4",
            "--name",
            "cfg1",
        ],
        storage=swh_storage,
    )
    assert_result(result)

    result = invoke(scrubber_db, ["check", "stalled", "cfg1"], storage=swh_storage)
    assert_result(result)
    expected = "No stuck partition found for cfg1 [id=1, type=snapshot]\n"
    assert result.output == expected, result.output

    # insert a few partitions
    with scrubber_db.transaction() as cur:
        cur.execute(
            "INSERT INTO checked_partition "
            "VALUES (1, 0, now() - '20m'::interval, NULL);"
        )
        cur.execute(
            "INSERT INTO checked_partition "
            "VALUES (1, 1, now() - '2h'::interval, NULL);"
        )
        cur.execute(
            "INSERT INTO checked_partition "
            "VALUES (1, 2, now() - '2h'::interval, now() - '1h55m'::interval);"
        )

    # partitions 0 and 1 are considered as stalled
    result = invoke(scrubber_db, ["check", "stalled", "cfg1"], storage=swh_storage)
    assert_result(result)
    expected = """\
Stuck partitions for cfg1 [id=1, type=snapshot]:
0:	stuck since today (20 minutes)
1:	stuck since today (2 hours)
"""
    assert result.output == expected, result.output

    # let's reset them
    result = invoke(
        scrubber_db, ["check", "stalled", "--reset", "cfg1"], storage=swh_storage
    )
    assert_result(result)
    expected = """\
Stuck partitions for cfg1 [id=1, type=snapshot]:
0:	stuck since today (20 minutes)
	partition reset
1:	stuck since today (2 hours)
	partition reset
"""  # noqa: W191,E101
    assert result.output == expected, result.output
    with scrubber_db.transaction() as cur:
        cur.execute(
            "SELECT partition_id, end_date "
            "FROM checked_partition "
            "WHERE config_id=1 AND start_date is NULL"
        )
        assert cur.fetchall() == [(0, None), (1, None)]

    # for good measure, check the next few selected partitions, expected 0, 1 and 3
    assert next(scrubber_db.checked_partition_iter_next(1)) == 0
    assert next(scrubber_db.checked_partition_iter_next(1)) == 1
    assert next(scrubber_db.checked_partition_iter_next(1)) == 3


def test_locate_origins(mocker, scrubber_db, swh_storage, naive_graph_client):
    origin_locator = MagicMock()
    OriginLocator = mocker.patch(
        "swh.scrubber.origin_locator.OriginLocator", return_value=origin_locator
    )
    get_scrubber_db = mocker.patch(
        "swh.scrubber.get_scrubber_db", return_value=scrubber_db
    )
    mocker.patch(
        "swh.graph.http_client.RemoteGraphClient",
        return_value=naive_graph_client,
    )

    result = invoke(scrubber_db, ["locate"], storage=swh_storage)
    assert_result(result)
    assert result.output == ""

    get_scrubber_db.assert_called_once_with(cls="postgresql", db=scrubber_db.conn.dsn)
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
    assert_result(result)
    assert result.output == ""

    get_scrubber_db.assert_called_once_with(cls="postgresql", db=scrubber_db.conn.dsn)
    Fixer.assert_called_once_with(
        db=scrubber_db,
        start_object=CoreSWHID.from_string("swh:1:cnt:" + "00" * 20),
        end_object=CoreSWHID.from_string("swh:1:snp:" + "ff" * 20),
    )
    assert fixer.method_calls == [call.run()]


# deprecated commands
def test_check_storage(mocker, scrubber_db, swh_storage):
    storage_checker = MagicMock()
    StorageChecker = mocker.patch(
        "swh.scrubber.storage_checker.StorageChecker", return_value=storage_checker
    )
    get_scrubber_db = mocker.patch(
        "swh.scrubber.get_scrubber_db", return_value=scrubber_db
    )
    result = invoke(
        scrubber_db,
        [
            "check",
            "init",
            "storage",
            "--object-type",
            "snapshot",
            "--nb-partitions",
            "4",
            "--name",
            "cfg1",
        ],
        storage=swh_storage,
    )
    assert_result(result)
    msg = "Created configuration cfg1 [1] for checking snapshot in postgresql storage"
    assert result.output.strip() == msg

    result = invoke(scrubber_db, ["check", "storage", "cfg1"], storage=swh_storage)
    assert_result(result)
    assert (
        result.output.strip()
        == "DeprecationWarning: The command 'storage' is deprecated."
    )

    get_scrubber_db.assert_called_with(cls="postgresql", db=scrubber_db.conn.dsn)
    StorageChecker.assert_called_once_with(
        db=scrubber_db,
        config_id=1,
        storage=StorageChecker.mock_calls[0][2]["storage"],
        limit=0,
    )
    assert storage_checker.method_calls == [call.run()]

    # using the config id instead of the config name
    result = invoke(
        scrubber_db, ["check", "storage", "--config-id", "1"], storage=swh_storage
    )
    assert_result(result)
    assert (
        result.output.strip()
        == "DeprecationWarning: The command 'storage' is deprecated."
    )


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
        [
            "check",
            "init",
            "journal",
            "--object-type",
            "snapshot",
            "--nb-partitions",
            "4",
            "--name",
            "cfg1",
        ],
        kafka_server=kafka_server,
        kafka_prefix=kafka_prefix,
        kafka_consumer_group=kafka_consumer_group,
    )
    assert_result(result)
    msg = "Created configuration cfg1 [1] for checking snapshot in kafka journal"
    assert result.output.strip() == msg

    result = invoke(
        scrubber_db,
        ["check", "journal", "cfg1"],
        kafka_server=kafka_server,
        kafka_prefix=kafka_prefix,
        kafka_consumer_group=kafka_consumer_group,
    )
    assert_result(result)
    assert (
        result.output.strip()
        == "DeprecationWarning: The command 'journal' is deprecated."
    )

    assert get_scrubber_db.call_count == 2
    get_scrubber_db.assert_called_with(cls="postgresql", db=scrubber_db.conn.dsn)

    JournalChecker.assert_called_once_with(
        db=scrubber_db,
        journal_client_config={
            "brokers": kafka_server,
            "cls": "kafka",
            "group_id": kafka_consumer_group,
            "prefix": kafka_prefix,
            "on_eof": "stop",
        },
        config_id=1,
    )
    assert journal_checker.method_calls == [call.run()]
