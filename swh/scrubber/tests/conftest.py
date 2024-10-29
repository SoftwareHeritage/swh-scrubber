# Copyright (C) 2022-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
from functools import partial

import attr
import pytest
from pytest_postgresql import factories

from swh.core.db.db_utils import initialize_database_for_module
from swh.journal.serializers import value_to_kafka
from swh.journal.writer import get_journal_writer
from swh.model.hashutil import hash_to_bytes
from swh.model.model import Directory, DirectoryEntry
from swh.model.swhids import ObjectType
from swh.model.tests.swh_model_data import DIRECTORIES
from swh.scrubber import get_scrubber_db
from swh.scrubber.db import ConfigEntry, CorruptObject, Datastore, ScrubberDb

scrubber_postgresql_proc = factories.postgresql_proc(
    load=[
        partial(
            initialize_database_for_module,
            modname="scrubber",
            version=ScrubberDb.current_version,
        )
    ],
)

postgresql_scrubber = factories.postgresql("scrubber_postgresql_proc")

OBJECT_TYPE = ObjectType.DIRECTORY
PARTITION_ID = 2
NB_PARTITIONS = 64
DIRECTORY = [dir_ for dir_ in DIRECTORIES if len(dir_.entries) > 1][0]
ORIGINAL_DIRECTORY = Directory(
    entries=(
        DirectoryEntry(
            name=b"dir1",
            type="dir",
            target=hash_to_bytes("4b825dc642cb6eb9a060e54bf8d69288fbee4904"),
            perms=0o040755,
        ),
        DirectoryEntry(
            name=b"file1.ext",
            type="file",
            target=hash_to_bytes("86bc6b377e9d25f9d26777a4a28d08e63e7c5779"),
            perms=0o644,
        ),
        DirectoryEntry(
            name=b"subprepo1",
            type="rev",
            target=hash_to_bytes("c7f96242d73c267adc77c2908e64e0c1cb6a4431"),
            perms=0o160000,
        ),
    ),
    raw_manifest=(
        b"tree 102\x00"
        b"160000 subprepo1\x00\xc7\xf9bB\xd7<&z\xdcw\xc2\x90\x8ed\xe0\xc1\xcbjD1"
        b"644 file1.ext\x00\x86\xbck7~\x9d%\xf9\xd2gw\xa4\xa2\x8d\x08\xe6>|Wy"
        b"40755 dir1\x00K\x82]\xc6B\xcbn\xb9\xa0`\xe5K\xf8\xd6\x92\x88\xfb\xeeI\x04"
    ),
)

# A directory with its entries in canonical order, but a hash computed as if
# computed in the reverse order.
# This happens when entries get normalized (either by the loader or accidentally
# in swh-storage)
CORRUPT_DIRECTORY = attr.evolve(ORIGINAL_DIRECTORY, raw_manifest=None)
assert ORIGINAL_DIRECTORY != CORRUPT_DIRECTORY
assert (
    hash_to_bytes("61992617462fff81509bda4a24b54c96ea74a007")
    == ORIGINAL_DIRECTORY.id
    == CORRUPT_DIRECTORY.id
)
assert (
    hash_to_bytes("81fda5b242e65fc81201e590d0f0ce5f582fbcdd")
    == CORRUPT_DIRECTORY.compute_hash()
    != CORRUPT_DIRECTORY.id
)
assert ORIGINAL_DIRECTORY.entries == CORRUPT_DIRECTORY.entries


@pytest.fixture
def datastore():
    return Datastore(package="storage", cls="postgresql", instance="service=swh-test")


@pytest.fixture
def scrubber_config(postgresql_scrubber):
    return {"cls": "postgresql", "db": postgresql_scrubber.info.dsn}


@pytest.fixture
def scrubber_db(scrubber_config):
    return get_scrubber_db(**scrubber_config)


@pytest.fixture
def config_id(scrubber_db, datastore) -> int:
    return scrubber_db.config_add(
        f"cfg_{OBJECT_TYPE}_{NB_PARTITIONS}", datastore, OBJECT_TYPE, NB_PARTITIONS
    )


@pytest.fixture
def config_entry(scrubber_db, config_id) -> ConfigEntry:
    return scrubber_db.config_get(config_id)


@pytest.fixture
def corrupt_object(scrubber_db, config_entry):
    return CorruptObject(
        id=ORIGINAL_DIRECTORY.swhid(),
        config=config_entry,
        first_occurrence=datetime.datetime.now(tz=datetime.timezone.utc),
        object_=value_to_kafka(CORRUPT_DIRECTORY.to_dict()),
    )


@pytest.fixture
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


@pytest.fixture
def journal_writer(kafka_server: str, kafka_prefix: str):
    return get_journal_writer(
        cls="kafka",
        brokers=[kafka_server],
        client_id="kafka_writer",
        prefix=kafka_prefix,
        anonymize=False,
    )
