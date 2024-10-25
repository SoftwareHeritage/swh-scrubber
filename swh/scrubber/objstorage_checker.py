# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging
from typing import Any, Dict, List, Optional, Protocol

from swh.core.statsd import Statsd
from swh.journal.client import get_journal_client
from swh.journal.serializers import kafka_to_value, value_to_kafka
from swh.model.model import Content
from swh.model.swhids import ObjectType
from swh.objstorage.exc import ObjCorruptedError, ObjNotFoundError
from swh.objstorage.interface import ObjStorageInterface, objid_from_dict
from swh.storage.interface import StorageInterface

from .base_checker import BaseChecker, BasePartitionChecker
from .db import ConfigEntry, Datastore, ScrubberDb

logger = logging.getLogger(__name__)


def get_objstorage_datastore(objstorage_config):
    objstorage_config = dict(objstorage_config)
    return Datastore(
        package="objstorage",
        cls=objstorage_config.pop("cls"),
        instance=json.dumps(objstorage_config),
    )


class ObjectStorageCheckerProtocol(Protocol):
    db: ScrubberDb
    objstorage: ObjStorageInterface

    @property
    def config(self) -> ConfigEntry: ...

    @property
    def statsd(self) -> Statsd: ...


class ContentCheckerMixin(ObjectStorageCheckerProtocol):
    """Mixin class implementing content checks used by object storage checkers."""

    def check_content(self, content: Content) -> None:
        """Checks if a content exists in an object storage (if ``check_references`` is set to
        :const:`True` in checker config) or if a content is corrupted in an object storage (if
        ``check_hashes`` is set to :const:`True` in checker config).
        """

        content_hashes = objid_from_dict(content.hashes())
        try:
            self.objstorage.check(content_hashes)
        except ObjNotFoundError:
            if self.config.check_references:
                self.statsd.increment("missing_object_total")
                self.db.missing_object_add(
                    id=content.swhid(), reference_ids={}, config=self.config
                )
        except ObjCorruptedError:
            if self.config.check_hashes:
                self.statsd.increment("hash_mismatch_total")
                self.db.corrupt_object_add(
                    id=content.swhid(),
                    config=self.config,
                    serialized_object=value_to_kafka(content.to_dict()),
                )


class ObjectStorageCheckerFromStoragePartition(
    BasePartitionChecker, ContentCheckerMixin
):
    """A partition based checker to detect missing and corrupted contents in an object storage.

    It iterates on content objects referenced in a storage instance, check they are available
    in a given object storage instance (if ``check_references`` is set to :const:`True` in
    checker config) then retrieve their bytes from it in order to recompute checksums and detect
    corruptions (if ``check_hashes`` is set to :const:`True` in checker config)."""

    def __init__(
        self,
        db: ScrubberDb,
        config_id: int,
        storage: StorageInterface,
        objstorage: Optional[ObjStorageInterface] = None,
        limit: int = 0,
    ):
        super().__init__(db=db, config_id=config_id, limit=limit)
        self.storage = storage
        self.objstorage = (
            objstorage if objstorage is not None else getattr(storage, "objstorage")
        )

        object_type = self.object_type.name.lower()

        if object_type != "content":
            raise ValueError(
                "ObjectStorageCheckerFromStoragePartition can only check objects of type "
                f"content, checking objects of type {object_type} is not supported."
            )

        if self.objstorage is None:
            raise ValueError(
                "An object storage must be provided to ObjectStorageCheckerFromStoragePartition."  # noqa
            )

    def check_partition(self, object_type: ObjectType, partition_id: int) -> None:
        page_token = None
        while True:
            page = self.storage.content_get_partition(
                partition_id=partition_id,
                nb_partitions=self.nb_partitions,
                page_token=page_token,
            )
            contents = page.results

            with self.statsd.timed(
                "batch_duration_seconds", tags={"operation": "check_hashes"}
            ):
                logger.debug("Checking %s content object hashes", len(contents))
                for content in contents:
                    self.check_content(content)

            page_token = page.next_page_token
            if page_token is None:
                break


class ObjectStorageCheckerFromJournal(BaseChecker, ContentCheckerMixin):
    """A journal based checker to detect missing and corrupted contents in an object storage.

    It iterates on content objects referenced in a kafka topic, check they are available
    in a given object storage instance then retrieve their bytes from it in order to
    recompute checksums and detect corruptions."""

    def __init__(
        self,
        db: ScrubberDb,
        config_id: int,
        journal_client_config: Dict[str, Any],
        objstorage: ObjStorageInterface,
    ):
        super().__init__(db=db, config_id=config_id)
        self.objstorage = objstorage

        object_type = self.object_type.name.lower()

        if object_type != "content":
            raise ValueError(
                "ObjectStorageCheckerFromJournal can only check objects of type content,"
                f"checking objects of type {object_type} is not supported."
            )

        self.journal_client_config = journal_client_config.copy()
        if "object_types" in self.journal_client_config:
            raise ValueError(
                "The journal_client configuration entry should not define the "
                "object_types field; this is handled by the scrubber configuration entry"
            )
        self.journal_client_config["object_types"] = [object_type]
        self.journal_client = get_journal_client(
            **self.journal_client_config,
            # Remove default deserializer; so process_kafka_values() gets the message
            # verbatim so it can archive it with as few modifications a possible.
            value_deserializer=lambda obj_type, msg: msg,
        )

    def run(self) -> None:
        self.journal_client.process(self.process_kafka_messages)

    def process_kafka_messages(self, all_messages: Dict[str, List[bytes]]):
        for message in all_messages["content"]:
            self.check_content(Content.from_dict(kafka_to_value(message)))
