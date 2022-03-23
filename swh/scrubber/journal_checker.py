# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Reads all objects in a swh-storage instance and recomputes their checksums."""

import logging
from typing import Any, Dict, List

from swh.journal.client import get_journal_client
from swh.journal.serializers import kafka_to_value
from swh.model import model

from .db import Datastore, ScrubberDb

logger = logging.getLogger(__name__)


class JournalChecker:
    """Reads a chunk of a swh-storage database, recomputes checksums, and
    reports errors in a separate database."""

    _datastore = None

    def __init__(self, db: ScrubberDb, journal_client: Dict[str, Any]):
        self.db = db
        self.journal_client_config = journal_client
        self.journal_client = get_journal_client(
            **journal_client,
            # Remove default deserializer; so process_kafka_values() gets the message
            # verbatim so it can archive it with as few modifications a possible.
            value_deserializer=lambda obj_type, msg: msg,
        )

    def datastore_info(self) -> Datastore:
        """Returns a :class:`Datastore` instance representing the journal instance
        being checked."""
        if self._datastore is None:
            config = self.journal_client_config
            if config["cls"] == "kafka":
                self._datastore = Datastore(
                    package="journal",
                    cls="kafka",
                    instance=(
                        f"brokers={config['brokers']!r} prefix={config['prefix']!r}"
                    ),
                )
            else:
                raise NotImplementedError(
                    f"StorageChecker(journal_client={self.journal_client_config!r})"
                    f".datastore()"
                )
        return self._datastore

    def run(self):
        """Runs a journal client with the given configuration.
        This method does not return, unless otherwise configured (with ``stop_on_eof``).
        """
        self.journal_client.process(self.process_kafka_messages)

    def process_kafka_messages(self, all_messages: Dict[str, List[bytes]]):
        for (object_type, messages) in all_messages.items():
            cls = getattr(model, object_type.capitalize())
            for message in messages:
                object_ = cls.from_dict(kafka_to_value(message))
                real_id = object_.compute_hash()
                if object_.id != real_id:
                    self.db.corrupt_object_add(
                        object_.swhid(), self.datastore_info(), message
                    )
