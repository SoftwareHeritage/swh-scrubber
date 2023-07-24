# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import partial

import pytest
from pytest_postgresql import factories

from swh.core.db.db_utils import initialize_database_for_module
from swh.model.swhids import ObjectType
from swh.scrubber.db import Datastore, ScrubberDb

scrubber_postgresql_proc = factories.postgresql_proc(
    load=[partial(initialize_database_for_module, modname="scrubber", version=6)],
)

postgresql_scrubber = factories.postgresql("scrubber_postgresql_proc")

OBJECT_TYPE = ObjectType.DIRECTORY
PARTITION_ID = 2
NB_PARTITIONS = 64


@pytest.fixture
def datastore():
    return Datastore(package="storage", cls="postgresql", instance="service=swh-test")


@pytest.fixture
def scrubber_db(postgresql_scrubber):
    db = ScrubberDb(postgresql_scrubber)
    with db.conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE corrupt_object")
        cur.execute("TRUNCATE TABLE datastore CASCADE")
    return db


@pytest.fixture
def config_id(scrubber_db, datastore):
    return scrubber_db.config_add(
        f"cfg_{OBJECT_TYPE}_{NB_PARTITIONS}", datastore, OBJECT_TYPE, NB_PARTITIONS
    )
