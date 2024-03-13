# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from typing_extensions import Protocol, runtime_checkable

from swh.core.statsd import Statsd

from .db import ConfigEntry, Datastore


@runtime_checkable
class CheckerInterface(Protocol):
    @property
    def config(self) -> ConfigEntry:
        ...

    @property
    def datastore(self) -> Datastore:
        ...

    @property
    def statsd(self) -> Statsd:
        ...

    def run(self) -> None:
        ...
