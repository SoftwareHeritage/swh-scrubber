# Copyright (C) 2022-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from swh.scrubber.db import ScrubberDb


def get_scrubber_db(cls: str, **kwargs) -> ScrubberDb:
    from swh.core.config import get_swh_backend_module

    _, BackendCls = get_swh_backend_module("scrubber", cls)
    assert BackendCls is not None
    return BackendCls(**kwargs)


get_datastore = get_scrubber_db
