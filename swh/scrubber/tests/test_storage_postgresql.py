# Copyright (C) 2022-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.scrubber.db import Datastore
from swh.scrubber.storage_checker import postgresql_storage_db

from .storage_checker_tests import *  # noqa


@pytest.fixture
def swh_storage_backend_config(swh_storage_postgresql_backend_config):
    return swh_storage_postgresql_backend_config


@pytest.fixture
def datastore(swh_storage):
    with postgresql_storage_db(swh_storage) as db:
        return Datastore(
            package="storage",
            cls="postgresql",
            instance=db.conn.info.dsn,
        )
