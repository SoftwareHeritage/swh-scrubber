# Copyright (C) 2020-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import tempfile
from unittest.mock import MagicMock

from click.testing import CliRunner
import yaml

from swh.scrubber.check_storage import storage_db
from swh.scrubber.cli import scrubber_cli_group

CLI_CONFIG = {
    "storage": {
        "cls": "postgresql",
        "db": "<replaced at runtime>",
        "objstorage": {"cls": "memory"},
    },
    "scrubber_db": {"cls": "local", "db": "<replaced at runtime>"},
}


def invoke(swh_storage, scrubber_db, args):
    runner = CliRunner()

    config = copy.deepcopy(CLI_CONFIG)
    with storage_db(swh_storage) as db:
        config["storage"]["db"] = db.conn.dsn

    config["scrubber_db"]["db"] = scrubber_db.conn.dsn

    with tempfile.NamedTemporaryFile("a", suffix=".yml") as config_fd:
        yaml.dump(config, config_fd)
        config_fd.seek(0)
        args = ["-C" + config_fd.name] + list(args)
        result = runner.invoke(scrubber_cli_group, args, catch_exceptions=False)
    return result


def test_check_storage(swh_storage, mocker, scrubber_db):
    storage_checker = MagicMock()
    StorageChecker = mocker.patch(
        "swh.scrubber.check_storage.StorageChecker", return_value=storage_checker
    )
    get_scrubber_db = mocker.patch(
        "swh.scrubber.get_scrubber_db", return_value=scrubber_db
    )
    result = invoke(
        swh_storage, scrubber_db, ["check", "storage", "--object-type=snapshot"]
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
