# Copyright (C) 2022-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json

import pytest

from swh.scrubber.db import Datastore

from .storage_checker_tests import *  # noqa


@pytest.fixture
def swh_storage_backend_config(swh_storage_cassandra_backend_config):
    return swh_storage_cassandra_backend_config


@pytest.fixture
def datastore(swh_storage):
    return Datastore(
        package="storage",
        cls="cassandra",
        instance=json.dumps(
            {
                "keyspace": swh_storage.keyspace,
                "hosts": swh_storage.hosts,
                "port": swh_storage.port,
            }
        ),
    )


@pytest.mark.skip(  # type: ignore[no-redef]
    "Duplicate directory entries are not representable in Cassandra, "
    "so this feature is irrelevant"
)
def test_directory_duplicate_entries():
    pass
