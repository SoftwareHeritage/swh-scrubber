# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Reads all objects in a swh-storage instance and recomputes their checksums."""

import json
import logging
from typing import Any, Dict, List, Optional

from swh.journal.client import get_journal_client
from swh.journal.serializers import kafka_to_value
from swh.model import model

from .db import ConfigEntry, Datastore, ScrubberDb

logger = logging.getLogger(__name__)


def get_datastore(journal_cfg) -> Datastore:
    if journal_cfg.get("cls") == "kafka":
        datastore = Datastore(
            package="journal",
            cls="kafka",
            instance=json.dumps(
                {
                    "brokers": journal_cfg["brokers"],
                    "group_id": journal_cfg["group_id"],
                    "prefix": journal_cfg["prefix"],
                }
            ),
        )
    else:
        raise NotImplementedError(
            f"JournalChecker(journal={journal_cfg!r}).datastore()"
        )
    return datastore


class JournalChecker:
    """Reads a chunk of a swh-storage database, recomputes checksums, and
    reports errors in a separate database."""

    _config: Optional[ConfigEntry] = None
    _datastore: Optional[Datastore] = None

    def __init__(self, db: ScrubberDb, config_id: int, journal: Dict[str, Any]):
        self.db = db
        self.config_id = config_id

        if self.config.check_references:
            raise ValueError(
                "The journal checcker cannot check for references, please set "
                "the 'check_references' to False in the config entry %s.",
                self.config_id,
            )
        self.journal_client_config = journal.copy()
        if "object_types" in self.journal_client_config:
            raise ValueError(
                "The journal_client configuration entry should not define the "
                "object_types field; this is handled by the scrubber configuration entry"
            )
        self.journal_client_config["object_types"] = [
            self.config.object_type.name.lower()
        ]
        self.journal_client = get_journal_client(
            **self.journal_client_config,
            # Remove default deserializer; so process_kafka_values() gets the message
            # verbatim so it can archive it with as few modifications a possible.
            value_deserializer=lambda obj_type, msg: msg,
        )

    @property
    def config(self) -> ConfigEntry:
        if self._config is None:
            self._config = self.db.config_get(self.config_id)

        assert self._config is not None
        return self._config

    @property
    def datastore(self) -> Datastore:
        """Returns a :class:`Datastore` instance representing the journal instance
        being checked."""
        return self.config.datastore

    def run(self):
        """Runs a journal client with the given configuration.
        This method does not return, unless otherwise configured (with ``stop_on_eof``).
        """
        self.journal_client.process(self.process_kafka_messages)

    def process_kafka_messages(self, all_messages: Dict[str, List[bytes]]):
        for (object_type, messages) in all_messages.items():
            logger.debug("Processing %s %s", len(messages), object_type)
            cls = getattr(model, object_type.capitalize())
            for message in messages:
                object_ = cls.from_dict(kafka_to_value(message))
                real_id = object_.compute_hash()
                if object_.id != real_id:
                    self.db.corrupt_object_add(object_.swhid(), self.config, message)
