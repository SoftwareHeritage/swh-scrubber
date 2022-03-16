# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import partial

import pytest
from pytest_postgresql import factories

from swh.core.db.pytest_plugin import initialize_database_for_module, postgresql_fact
from swh.scrubber.db import ScrubberDb

scrubber_postgresql_proc = factories.postgresql_proc(
    dbname="scrubber",
    load=[partial(initialize_database_for_module, modname="scrubber", version=1)],
)

postgresql_scrubber = postgresql_fact("scrubber_postgresql_proc")


@pytest.fixture
def scrubber_db(postgresql_scrubber):
    db = ScrubberDb(postgresql_scrubber)
    with db.conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE corrupt_object")
        cur.execute("TRUNCATE TABLE datastore CASCADE")
    yield db
