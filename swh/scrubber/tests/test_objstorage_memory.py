# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from .objstorage_checker_tests import *  # noqa

# Use cassandra storage and an objstorage with memory backend to run
# the tests


@pytest.fixture
def swh_storage_backend_config(swh_storage_cassandra_backend_config):
    return swh_storage_cassandra_backend_config


@pytest.fixture
def swh_objstorage_config():
    return {"cls": "memory"}
