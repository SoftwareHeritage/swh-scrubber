# Copyright (C) 2020-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.scrubber import get_scrubber_db
from swh.scrubber.db import ScrubberDb


def test_get_scrubber_db(scrubber_db):
    assert isinstance(scrubber_db, ScrubberDb)


@pytest.mark.parametrize("clz", ["something", "anything"])
def test_get_scrubber_db_raise(clz):
    with pytest.raises(ValueError, match="Unknown"):
        get_scrubber_db(clz, db="service=scrubber-db")
