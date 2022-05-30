# Copyright (C) 2020-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any

import pytest

from swh.scrubber import get_scrubber_db


@pytest.mark.parametrize("clz", ["local", "postgresql"])
def test_get_scrubber_db(mocker, clz):
    mock_scrubber = mocker.patch("swh.scrubber.db.ScrubberDb")

    def test_connect(db_str: str, **kwargs) -> Any:
        return "connection-result"

    mock_scrubber.connect.side_effect = test_connect

    actual_result = get_scrubber_db(clz, db="service=scrubber-db")

    assert mock_scrubber.connect.called is True
    assert actual_result == "connection-result"


@pytest.mark.parametrize("clz", ["something", "anything"])
def test_get_scrubber_db_raise(clz):
    assert clz not in ["local", "postgresql"]

    with pytest.raises(ValueError, match="Unknown"):
        get_scrubber_db(clz, db="service=scrubber-db")
