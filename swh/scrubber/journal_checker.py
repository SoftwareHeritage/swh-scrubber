# Copyright (C) 2021-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Reads all objects in a swh-storage instance and recomputes their checksums."""

import json
import logging
from typing import Any, Dict, List

import attr

from swh.journal.client import get_journal_client
from swh.journal.serializers import kafka_to_value
from swh.model import model

from .base_checker import BaseChecker
from .db import Datastore, ScrubberDb

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
            f"JournalChecker(journal_client_config={journal_cfg!r}).datastore()"
        )
    return datastore


class JournalChecker(BaseChecker):
    """Reads a chunk of a swh-storage database, recomputes checksums, and
    reports errors in a separate database."""

    def __init__(
        self, db: ScrubberDb, config_id: int, journal_client_config: Dict[str, Any]
    ):
        super().__init__(db=db, config_id=config_id)

        if self.config.check_references:
            raise ValueError(
                "The journal checker cannot check for references, please set "
                "the 'check_references' to False in the config entry %s.",
                self.config_id,
            )
        self.journal_client_config = journal_client_config.copy()
        if "object_types" in self.journal_client_config:
            raise ValueError(
                "The journal_client configuration entry should not define the "
                "object_types field; this is handled by the scrubber configuration entry"
            )
        self.journal_client_config["object_types"] = [self.object_type.name.lower()]
        self.journal_client = get_journal_client(
            **self.journal_client_config,
            # Remove default deserializer; so process_kafka_values() gets the message
            # verbatim so it can archive it with as few modifications a possible.
            value_deserializer=lambda obj_type, msg: msg,
        )

    def run(self) -> None:
        """Runs a journal client with the given configuration.

        This method does not return, unless the client is configured with ``on_eof``
        parameter equals to ``EofBehavior.STOP`` (``stop`` in YAML).
        """
        self.journal_client.process(self.process_kafka_messages)

    def process_kafka_messages(self, all_messages: Dict[str, List[bytes]]):
        for object_type, messages in all_messages.items():
            logger.debug("Processing %s %s", len(messages), object_type)
            cls = getattr(model, object_type.capitalize())
            for message in messages:
                if object_type == "directory":
                    d = kafka_to_value(message)
                    (
                        has_duplicate_dir_entries,
                        object_,
                    ) = cls.from_possibly_duplicated_entries(
                        entries=tuple(
                            map(model.DirectoryEntry.from_dict, d["entries"])
                        ),
                        raw_manifest=d.get("raw_manifest"),
                    )
                    object_ = attr.evolve(object_, id=d["id"])
                    if has_duplicate_dir_entries:
                        self.statsd.increment(
                            "duplicate_directory_entries_total",
                            tags={"object_type": "directory"},
                        )
                else:
                    object_ = cls.from_dict(kafka_to_value(message))
                    has_duplicate_dir_entries = False
                real_id = object_.compute_hash()
                if object_.id != real_id or has_duplicate_dir_entries:
                    self.db.corrupt_object_add(object_.swhid(), self.config, message)
