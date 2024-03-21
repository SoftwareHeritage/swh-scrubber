# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging
from typing import Iterable, Optional

from swh.journal.serializers import value_to_kafka
from swh.model.model import Content
from swh.model.swhids import ObjectType
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.interface import ObjStorageInterface, objid_from_dict
from swh.storage.interface import StorageInterface

from .base_checker import BasePartitionChecker
from .db import Datastore, ScrubberDb

logger = logging.getLogger(__name__)


def get_objstorage_datastore(objstorage_config):
    objstorage_config = dict(objstorage_config)
    return Datastore(
        package="objstorage",
        cls=objstorage_config.pop("cls"),
        instance=json.dumps(objstorage_config),
    )


class ObjectStorageChecker(BasePartitionChecker):
    """A checker to detect missing and corrupted contents in an object storage.

    It iterates on content objects referenced in a storage instance, check they
    are available in a given object storage instance then retrieve their bytes
    from it in order to recompute checksums and detect corruptions."""

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
        self.statsd_constant_tags["datastore_instance"] = self.datastore.instance

    def check_partition(self, object_type: ObjectType, partition_id: int) -> None:
        if object_type != ObjectType.CONTENT:
            raise ValueError(
                "ObjectStorageChecker can only check objects of type content,"
                f"checking objects of type {object_type.name.lower()} is not supported."
            )

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
                self.check_contents(contents)

            page_token = page.next_page_token
            if page_token is None:
                break

    def check_contents(self, contents: Iterable[Content]) -> None:
        for content in contents:
            content_hashes = objid_from_dict(content.hashes())
            try:
                content_bytes = self.objstorage.get(content_hashes)
            except ObjNotFoundError:
                if self.check_references:
                    self.statsd.increment("missing_object_total")
                    self.db.missing_object_add(
                        id=content.swhid(), reference_ids={}, config=self.config
                    )
            else:
                if self.check_hashes:
                    recomputed_hashes = objid_from_dict(
                        Content.from_data(content_bytes).hashes()
                    )
                    if content_hashes != recomputed_hashes:
                        self.statsd.increment("hash_mismatch_total")
                        self.db.corrupt_object_add(
                            id=content.swhid(),
                            config=self.config,
                            serialized_object=value_to_kafka(content.to_dict()),
                        )
