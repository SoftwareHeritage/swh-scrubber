# Copyright (C) 2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
import os
from subprocess import CalledProcessError

from click.testing import CliRunner
import pytest

from swh.core.cli.db import db as swhdb
from swh.core.db import BaseDb
from swh.core.db.db_utils import import_swhmodule, swh_db_upgrade, swh_db_version
from swh.core.db.tests.test_cli import craft_conninfo
from swh.model.swhids import ObjectType
from swh.scrubber.db import ScrubberDb


@pytest.fixture
def cli_runner():
    return CliRunner()


@pytest.fixture()
def current_version():
    return 6


def test_datadir(datadir):
    assert datadir.endswith("/swh/scrubber/tests/data")


@pytest.fixture()
def mock_import_swhmodule(mocker, datadir, current_version):
    """This bypasses the module manipulation to make import_swhmodule return a mock
    object suitable for data test files listing via get_sql_for_package.

    For a given module `test.<mod>`, return a MagicMock object with a __name__
    set to `<mod>` and __file__ pointing to `data/<mod>/__init__.py`.

    The Mock object also defines a `get_datastore()` attribute on which the
    `current_version` attribute is set to `current_version`.

    Typical usage::

      def test_xxx(cli_runner, mock_import_swhmodule):
        conninfo = craft_conninfo(test_db, "new-db")
        module_name = "test.cli"
        # the command below will use sql scripts from
        #     swh/core/db/tests/data/cli/sql/*.sql
        cli_runner.invoke(swhdb, ["init", module_name, "--dbname", conninfo])

    """

    def import_swhmodule_mock(modname):
        if modname.startswith("test."):
            dirname = modname.split(".", 1)[1]

            def get_datastore(*args, **kw):
                return mocker.MagicMock(current_version=current_version)

            return mocker.MagicMock(
                __name__=modname,
                __file__=os.path.join(datadir, dirname, "__init__.py"),
                get_datastore=get_datastore,
            )
        else:
            return import_swhmodule(modname)

    return mocker.patch("swh.core.db.db_utils.import_swhmodule", import_swhmodule_mock)


def test_upgrade_6_to_7(
    cli_runner, postgresql, mock_import_swhmodule, datadir, current_version
):
    """Check 6 to 7 migration"""
    module = "test.cli"
    conninfo = craft_conninfo(postgresql)
    result = cli_runner.invoke(swhdb, ["init-admin", module, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"
    result = cli_runner.invoke(swhdb, ["init", module, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    assert swh_db_version(conninfo) == current_version

    new_version = swh_db_upgrade(conninfo, module, 7)
    assert new_version == 7
    assert swh_db_version(conninfo) == 7

    tmap = {otype.name.lower(): otype.value for otype in ObjectType}
    db = ScrubberDb(postgresql)

    # corrupt objects
    corrupt_objects = list(db.corrupt_object_iter())
    assert len(corrupt_objects) == 15

    got = [
        (
            o.config.datastore.instance,
            o.config.object_type,
            o.config.name,
            str(o.id),
        )
        for o in corrupt_objects
    ]

    # expected result for corrupt objects
    expected = []
    for otype, n, ds, port in (
        ("release", 16, 1, 5432),
        ("revision", 16, 1, 5432),
        ("release", 16, 2, 5433),
    ):
        expected += [
            (str(port), otype, f"ds{ds}-{otype}-{n}", f"swh:1:{tmap[otype]}:{i:040X}")
            for i in range(5)
        ]

    assert set(got) == set(expected)

    # missing objects
    missing_objects = list(db.missing_object_iter())
    assert len(missing_objects) == 15

    got = [
        (o.config.datastore.instance, o.config.object_type, o.config.name, str(o.id))
        for o in missing_objects
    ]

    # expected result for missing objects
    # For those, the object type of the check_config entry is the one of the
    # reference object (not the one of the missing object).
    # E.g. the check_config for snapshot generates release missing object entries.
    expected = []
    for checked_otype, missing_otype, n, ds, port in (
        ("snapshot", "release", 16, 1, 5432),
        ("snapshot", "revision", 16, 1, 5432),
        ("snapshot", "release", 16, 2, 5433),
    ):
        expected += [
            (
                str(port),
                checked_otype,
                f"ds{ds}-{checked_otype}-{n}",
                f"swh:1:{tmap[missing_otype]}:{i:040X}",
            )
            for i in range(5)
        ]

    assert set(got) == set(expected)

    # missing_object_references
    for mo in missing_objects:
        mo_refs = list(db.missing_object_reference_iter(mo.id))
        assert mo.config in [mor.config for mor in mo_refs]
        # for each missing object, there is only one reference from the same
        # check session (same check_config)
        assert len([mor for mor in mo_refs if mor.config == mo.config]) == 1


def test_upgrade_6_to_7_fails_corrupt(
    cli_runner, postgresql, mock_import_swhmodule, datadir, current_version
):
    """Check 6 to 7 migration fails

    in case there is a corrupt_object row with a datastore that matches 2
    check_configs for the object type

    """

    module = "test.cli"
    conninfo = craft_conninfo(postgresql)
    result = cli_runner.invoke(swhdb, ["init-admin", module, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"
    result = cli_runner.invoke(swhdb, ["init", module, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    assert swh_db_version(conninfo) == current_version

    cnx = BaseDb.connect(conninfo)
    with cnx.transaction() as cur:
        # datastore 3 have 2 check_config entries for the release object_type
        cur.execute(
            "insert into corrupt_object(id, datastore, object) " "values (%s, %s, %s)",
            ("swh:1:rel:0000000000000000000000000000000000000000", 3, b"\x00"),
        )
    with pytest.raises(CalledProcessError):
        swh_db_upgrade(conninfo, module, 7)

    assert swh_db_version(conninfo) == 6


def test_upgrade_6_to_7_fails_missing_reference(
    cli_runner, postgresql, mock_import_swhmodule, datadir, current_version
):
    """Check 6 to 7 migration fails

    in case there is a missing_object_reference row with a datastore that matches 2
    check_configs for the object (reference_id) type.
    """
    module = "test.cli"
    conninfo = craft_conninfo(postgresql)
    result = cli_runner.invoke(swhdb, ["init-admin", module, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"
    result = cli_runner.invoke(swhdb, ["init", module, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    assert swh_db_version(conninfo) == current_version

    cnx = BaseDb.connect(conninfo)
    with cnx.transaction() as cur:
        # datastore 3 have 2 check_config entries for the release object_type
        cur.execute(
            "insert into missing_object_reference(missing_id, reference_id, datastore) "
            "values (%s, %s, %s)",
            (
                "swh:1:dir:0000000000000000000000000000000000000000",
                "swh:1:rel:0000000000000000000000000000000000000000",
                3,
            ),
        )

    with pytest.raises(CalledProcessError):
        swh_db_upgrade(conninfo, module, 7)

    assert swh_db_version(conninfo) == 6


def test_upgrade_6_to_7_fails_missing(
    cli_runner, postgresql, mock_import_swhmodule, datadir, current_version
):
    """Check 6 to 7 migration fails

    in case there is a missing_object row with a datastore that matches 2
    check_configs for the object type.
    """

    module = "test.cli"
    conninfo = craft_conninfo(postgresql)
    result = cli_runner.invoke(swhdb, ["init-admin", module, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"
    result = cli_runner.invoke(swhdb, ["init", module, "--dbname", conninfo])
    assert result.exit_code == 0, f"Unexpected output: {result.output}"

    assert swh_db_version(conninfo) == current_version

    cnx = BaseDb.connect(conninfo)
    with cnx.transaction() as cur:
        # datastore 3 have 2 check_config entries for the release object_type
        cur.execute(
            "insert into missing_object(id, datastore) " "values (%s, %s)",
            ("swh:1:rel:0000000000000000000000000000000000000000", 3),
        )

    with pytest.raises(CalledProcessError):
        swh_db_upgrade(conninfo, module, 7)

    assert swh_db_version(conninfo) == 6
