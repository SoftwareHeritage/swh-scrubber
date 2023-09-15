# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import logging
from pathlib import Path
import subprocess
from unittest.mock import MagicMock
import zlib

import attr
import pytest

from swh.journal.serializers import kafka_to_value, value_to_kafka
from swh.model.hashutil import hash_to_bytes
from swh.model.model import Directory, DirectoryEntry
from swh.scrubber.db import (
    ConfigEntry,
    CorruptObject,
    Datastore,
    FixedObject,
    ScrubberDb,
)
from swh.scrubber.fixer import Fixer

from .conftest import CORRUPT_DIRECTORY, ORIGINAL_DIRECTORY


@pytest.fixture
def corrupt_object(config_entry) -> CorruptObject:
    return CorruptObject(
        id=ORIGINAL_DIRECTORY.swhid(),
        config=config_entry,
        first_occurrence=datetime.datetime.now(tz=datetime.timezone.utc),
        object_=value_to_kafka(CORRUPT_DIRECTORY.to_dict()),
    )


def test_no_object(scrubber_db: ScrubberDb, mocker) -> None:
    """There is no object to recover -> nothing happens"""
    fixer = Fixer(db=scrubber_db)
    fixer.run()

    with scrubber_db.conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM fixed_object")
        assert cur.fetchone() == (0,)


def test_no_origin(
    scrubber_db: ScrubberDb, corrupt_object: CorruptObject, mocker
) -> None:
    """There is no origin to recover objects from -> nothing happens"""
    scrubber_db.corrupt_object_add(
        corrupt_object.id, corrupt_object.config, corrupt_object.object_
    )

    fixer = Fixer(db=scrubber_db)
    fixer.run()

    with scrubber_db.conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM fixed_object")
        assert cur.fetchone() == (0,)


def test_already_fixed(
    scrubber_db: ScrubberDb, corrupt_object: CorruptObject, datastore: Datastore, mocker
) -> None:
    """All corrupt objects are already fixed -> nothing happens"""
    fixed_object = FixedObject(
        id=corrupt_object.id,
        object_=value_to_kafka(ORIGINAL_DIRECTORY.to_dict()),
        method="whatever means necessary",
    )
    scrubber_db.corrupt_object_add(
        corrupt_object.id, corrupt_object.config, corrupt_object.object_
    )
    with scrubber_db.cursor() as cur:
        scrubber_db.object_origin_add(cur, corrupt_object.id, ["http://example.org/"])
        scrubber_db.fixed_object_add(cur, [fixed_object])

    subprocess_run = mocker.patch("subprocess.run")

    scrubber_db = MagicMock(wraps=scrubber_db)

    fixer = Fixer(db=scrubber_db)
    fixer.run()

    # Check the Fixer did not try to fix the object again
    scrubber_db.fixed_object_add.assert_not_called()
    subprocess_run.assert_not_called()
    with scrubber_db.conn.cursor() as cur:
        cur.execute("SELECT id, method FROM fixed_object")
        assert list(cur) == [(str(fixed_object.id), fixed_object.method)]


def _run_fixer_with_clone(
    scrubber_db: ScrubberDb,
    mocker,
    caplog,
    corrupt_object: CorruptObject,
    subprocess_run_side_effect,
) -> None:
    """Helper for all tests that involve running the fixer with a clone:
    adds a corrupt object and an origin to the DB, mocks subprocess.run with the
    given function, and runs the fixer with caplog"""
    scrubber_db.corrupt_object_add(
        corrupt_object.id, corrupt_object.config, corrupt_object.object_
    )
    with scrubber_db.cursor() as cur:
        scrubber_db.object_origin_add(cur, corrupt_object.id, ["http://example.org/"])

    subprocess_run = mocker.patch(
        "subprocess.run", side_effect=subprocess_run_side_effect
    )

    fixer = Fixer(db=scrubber_db)
    with caplog.at_level(logging.CRITICAL):
        with caplog.at_level(logging.INFO, logger="swh.scrubber.fixer"):
            fixer.run()

    subprocess_run.assert_called()


def test_failed_clone(
    scrubber_db: ScrubberDb, corrupt_object: CorruptObject, mocker, caplog
) -> None:
    """Corrupt object found with an origin, but the origin's clone is broken somehow"""
    scrubber_db = MagicMock(wraps=scrubber_db)

    _run_fixer_with_clone(
        scrubber_db,
        mocker,
        caplog,
        corrupt_object=corrupt_object,
        subprocess_run_side_effect=subprocess.CalledProcessError(1, "foo"),
    )

    scrubber_db.fixed_object_add.assert_not_called()
    with scrubber_db.conn.cursor() as cur:
        cur.execute("SELECT id, method FROM fixed_object")
        assert list(cur) == []

    assert (
        "swh.scrubber.fixer",
        logging.ERROR,
        "Failed to clone http://example.org/",
    ) in caplog.record_tuples


def test_empty_origin(
    scrubber_db: ScrubberDb, corrupt_object: CorruptObject, mocker, caplog
) -> None:
    """Corrupt object found with an origin, but the origin's clone is missing
    the object"""
    scrubber_db = MagicMock(wraps=scrubber_db)
    real_subprocess_run = subprocess.run

    def subprocess_run(args, **kwargs):
        (*head, path) = args
        assert head == ["git", "clone", "--bare", "http://example.org/"]
        real_subprocess_run(["git", "init", "--bare", path])

    _run_fixer_with_clone(
        scrubber_db,
        mocker,
        caplog,
        corrupt_object=corrupt_object,
        subprocess_run_side_effect=subprocess_run,
    )

    scrubber_db.fixed_object_add.assert_not_called()
    with scrubber_db.conn.cursor() as cur:
        cur.execute("SELECT id, method FROM fixed_object")
        assert list(cur) == []

    assert (
        "swh.scrubber.fixer",
        logging.INFO,
        "swh:1:dir:61992617462fff81509bda4a24b54c96ea74a007 not found in origin",
    ) in caplog.record_tuples


def test_parseable_directory_from_origin(
    scrubber_db: ScrubberDb, corrupt_object: CorruptObject, mocker, caplog
) -> None:
    """Corrupt object found with an origin, and the object is found in the origin's
    clone as expected."""
    scrubber_db = MagicMock(wraps=scrubber_db)
    real_subprocess_run = subprocess.run

    def subprocess_run(args, **kwargs):
        (*head, path) = args
        assert head == ["git", "clone", "--bare", "http://example.org/"]
        real_subprocess_run(["git", "init", "--bare", path])
        object_dir_path = Path(path) / "objects/61"
        object_path = object_dir_path / "992617462fff81509bda4a24b54c96ea74a007"
        object_dir_path.mkdir()
        with open(object_path, "wb") as fd:
            fd.write(zlib.compress(ORIGINAL_DIRECTORY.raw_manifest))

    _run_fixer_with_clone(
        scrubber_db,
        mocker,
        caplog,
        corrupt_object=corrupt_object,
        subprocess_run_side_effect=subprocess_run,
    )

    scrubber_db.fixed_object_add.assert_called_once()
    fixed_objects = list(scrubber_db.fixed_object_iter())
    assert len(fixed_objects) == 1

    assert fixed_objects[0].id == ORIGINAL_DIRECTORY.swhid()
    assert fixed_objects[0].method == "from_origin"
    assert (
        Directory.from_dict(kafka_to_value(fixed_objects[0].object_))
        == ORIGINAL_DIRECTORY
    )

    assert caplog.record_tuples == []


def test_unparseable_directory(
    scrubber_db: ScrubberDb, config_entry: ConfigEntry, mocker, caplog
) -> None:
    """Corrupt object found with an origin, and the object is found in the origin's
    clone as expected; but Dulwich cannot parse it.
    It was probably loaded by an old version of the loader that was more permissive,
    by using libgit2."""
    scrubber_db = MagicMock(wraps=scrubber_db)
    real_subprocess_run = subprocess.run

    raw_manifest = b"this is not a parseable manifest"
    raw_manifest = f"tree {len(raw_manifest)}\x00".encode() + raw_manifest

    original_directory = Directory(
        entries=(
            DirectoryEntry(
                name=b"dir1",
                type="dir",
                target=hash_to_bytes("4b825dc642cb6eb9a060e54bf8d69288fbee4904"),
                perms=0o040755,
            ),
        ),
        raw_manifest=raw_manifest,
    )
    assert original_directory.id.hex() == "a518fa6b46bad74e95588d2bfdf4455398a2216a"

    corrupt_directory = attr.evolve(original_directory, raw_manifest=None)
    corrupt_object = CorruptObject(
        id=original_directory.swhid(),
        config=config_entry,
        object_=value_to_kafka(corrupt_directory.to_dict()),
        first_occurrence=datetime.datetime.now(tz=datetime.timezone.utc),
    )

    def subprocess_run(args, **kwargs):
        (*head, path) = args
        if head[0:2] != ["git", "clone"]:
            return real_subprocess_run(args, **kwargs)
        assert head == ["git", "clone", "--bare", "http://example.org/"]
        real_subprocess_run(["git", "init", "--bare", path])
        object_dir_path = Path(path) / "objects/a5"
        object_path = object_dir_path / "18fa6b46bad74e95588d2bfdf4455398a2216a"
        object_dir_path.mkdir()
        with open(object_path, "wb") as fd:
            fd.write(zlib.compress(raw_manifest))

    _run_fixer_with_clone(
        scrubber_db,
        mocker,
        caplog,
        corrupt_object=corrupt_object,
        subprocess_run_side_effect=subprocess_run,
    )

    scrubber_db.fixed_object_add.assert_called_once()
    fixed_objects = list(scrubber_db.fixed_object_iter())
    assert len(fixed_objects) == 1

    assert fixed_objects[0].id == original_directory.swhid()
    assert fixed_objects[0].method == "manifest_from_origin"
    assert (
        Directory.from_dict(kafka_to_value(fixed_objects[0].object_))
        == original_directory
    )

    assert caplog.record_tuples == [
        (
            "swh.scrubber.fixer",
            logging.INFO,
            r"Dulwich failed to parse b'tree 32\x00this is not a parseable manifest'",
        )
    ]
