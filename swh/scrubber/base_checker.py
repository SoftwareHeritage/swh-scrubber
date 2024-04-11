# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from abc import ABC, abstractmethod
from itertools import count, islice
import logging
from typing import Any, Dict, Iterable, Optional

import psycopg2
import tenacity

from swh.core.statsd import Statsd
from swh.model import swhids

from .db import ConfigEntry, Datastore, ScrubberDb

logger = logging.getLogger(__name__)


class BaseChecker(ABC):
    """Base Checker class wrapping common features."""

    def __init__(
        self,
        db: ScrubberDb,
        config_id: int,
    ):
        self.db = db
        self.config_id = config_id
        self._config: Optional[ConfigEntry] = None
        self._statsd: Optional[Statsd] = None
        self.statsd_constant_tags: Dict[str, Any] = {
            "object_type": self.object_type.name.lower(),
            "datastore_package": self.datastore.package,
            "datastore_cls": self.datastore.cls,
            "datastore_instance": self.datastore.instance,
        }

    @property
    def config(self) -> ConfigEntry:
        """Returns a :class:`ConfigEntry` instance containing checker configuration."""
        if self._config is None:
            self._config = self.db.config_get(self.config_id)

        assert self._config is not None
        return self._config

    @property
    def datastore(self) -> Datastore:
        """Returns a :class:`Datastore` instance representing the source of data
        being checked."""
        return self.config.datastore

    @property
    def statsd(self) -> Statsd:
        """Returns a :class:`Statsd` instance to send statsd metrics."""
        if self._statsd is None:
            self._statsd = Statsd(
                namespace="swh_scrubber",
                constant_tags=self.statsd_constant_tags,
            )
        return self._statsd

    @property
    def object_type(self) -> swhids.ObjectType:
        """Returns the type of object being checked."""
        return self.config.object_type

    @property
    def check_hashes(self) -> bool:
        return self.config.check_hashes

    @property
    def check_references(self) -> bool:
        return self.config.check_references

    @abstractmethod
    def run(self) -> None:
        """Run the checker processing, derived classes must implement this method."""
        pass


class BasePartitionChecker(BaseChecker):
    """Base class for checkers processing partition of objects."""

    def __init__(
        self,
        db: ScrubberDb,
        config_id: int,
        limit: int = 0,
    ):
        super().__init__(db=db, config_id=config_id)
        self.limit = limit
        self.statsd_constant_tags["nb_partitions"] = self.nb_partitions

    @property
    def nb_partitions(self) -> int:
        """Returns the number of partitions set in configuration."""
        return self.config.nb_partitions

    def run(self) -> None:
        """Runs on all objects of ``object_type`` in each partition between
        ``start_partition_id`` (inclusive) and ``end_partition_id`` (exclusive).
        """
        counter: Iterable[int] = count()
        if self.limit:
            counter = islice(counter, 0, self.limit)
        for _, partition_id in zip(
            counter, self.db.checked_partition_iter_next(self.config_id)
        ):
            logger.debug(
                "Processing %s partition %d/%d",
                self.object_type,
                partition_id,
                self.nb_partitions,
            )

            self._check_partition(self.object_type, partition_id)

            self.db.checked_partition_upsert(
                self.config_id,
                partition_id,
            )

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(psycopg2.OperationalError),
        wait=tenacity.wait_random_exponential(min=10, max=180),
    )
    def _check_partition(
        self, object_type: swhids.ObjectType, partition_id: int
    ) -> None:
        "Retryable method checking objects in partition."
        return self.check_partition(object_type, partition_id)

    @abstractmethod
    def check_partition(
        self, object_type: swhids.ObjectType, partition_id: int
    ) -> None:
        """Abstract method that derived classes must implement to check objects
        in partition."""
        pass
