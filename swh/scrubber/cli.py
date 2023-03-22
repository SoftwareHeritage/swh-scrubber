# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from typing import Optional

import click

from swh.core.cli import CONTEXT_SETTINGS
from swh.core.cli import swh as swh_cli_group


@swh_cli_group.group(name="scrubber", context_settings=CONTEXT_SETTINGS)
@click.option(
    "--config-file",
    "-C",
    default=None,
    type=click.Path(
        exists=True,
        dir_okay=False,
    ),
    help="Configuration file.",
)
@click.pass_context
def scrubber_cli_group(ctx, config_file: Optional[str]) -> None:
    """main command group of the datastore scrubber

    Expected config format::

        scrubber_db:
            cls: local
            db: "service=..."    # libpq DSN

        # for storage checkers + origin locator only:
        storage:
            cls: postgresql     # cannot be remote for checkers, as they need direct
                                # access to the pg DB
            db": "service=..."  # libpq DSN
            objstorage:
                cls: memory

        # for journal checkers only:
        journal:
            # see https://docs.softwareheritage.org/devel/apidoc/swh.journal.client.html
            # for the full list of options
            sasl.mechanism: SCRAM-SHA-512
            security.protocol: SASL_SSL
            sasl.username: ...
            sasl.password: ...
            group_id: ...
            privileged: True
            message.max.bytes: 524288000
            brokers:
              - "broker1.journal.softwareheritage.org:9093
              - "broker2.journal.softwareheritage.org:9093
              - "broker3.journal.softwareheritage.org:9093
              - "broker4.journal.softwareheritage.org:9093
              - "broker5.journal.softwareheritage.org:9093
            object_types: [directory, revision, snapshot, release]
            auto_offset_reset: earliest
    """
    from swh.core import config

    from . import get_scrubber_db

    if not config_file:
        config_file = os.environ.get("SWH_CONFIG_FILENAME")

    if config_file:
        if not os.path.exists(config_file):
            raise ValueError("%s does not exist" % config_file)
        conf = config.read(config_file)
    else:
        conf = {}

    if "scrubber_db" not in conf:
        ctx.fail("You must have a scrubber_db configured in your config file.")

    ctx.ensure_object(dict)
    ctx.obj["config"] = conf
    ctx.obj["db"] = get_scrubber_db(**conf["scrubber_db"])


@scrubber_cli_group.group(name="check")
@click.pass_context
def scrubber_check_cli_group(ctx):
    """group of commands which read from data stores and report errors."""
    pass


@scrubber_check_cli_group.command(name="storage")
@click.option(
    "--object-type",
    type=click.Choice(
        # use a hardcoded list to prevent having to load the
        # replay module at cli loading time
        [
            "snapshot",
            "revision",
            "release",
            "directory",
            # TODO:
            # "raw_extrinsic_metadata",
            # "extid",
        ]
    ),
)
@click.option("--start-partition-id", default=0, type=int)
@click.option("--end-partition-id", default=4096, type=int)
@click.option("--nb-partitions", default=4096, type=int)
@click.pass_context
def scrubber_check_storage(
    ctx,
    object_type: str,
    start_partition_id: int,
    end_partition_id: int,
    nb_partitions: int,
):
    """Reads a swh-storage instance, and reports corrupt objects to the scrubber DB.

    This runs a single thread; parallelism is achieved by running this command multiple
    times, on disjoint ranges.

    All objects of type ``object_type`` are ordered, and split into the given number
    of partitions. When running in parallel, the number of partitions should be the
    same for all workers or they may work on overlapping or non-exhaustive ranges.

    Then, this process will check all partitions in the given
    ``[start_partition_id, end_partition_id)`` range. When running in parallel, these
    ranges should be set so that processes over the whole ``[0, nb_partitions)`` range.

    For example in order to have 8 threads checking revisions in parallel and with 64k
    checkpoints (to recover on crashes), the CLI should be ran 8 times with these
    parameters::

        --object-type revision --nb-partitions 65536 --start-partition-id 0 --end-partition-id 8192
        --object-type revision --nb-partitions 65536 --start-partition-id 8192 --end-partition-id 16384
        --object-type revision --nb-partitions 65536 --start-partition-id 16384 --end-partition-id 24576
        --object-type revision --nb-partitions 65536 --start-partition-id 24576 --end-partition-id 32768
        --object-type revision --nb-partitions 65536 --start-partition-id 32768 --end-partition-id 40960
        --object-type revision --nb-partitions 65536 --start-partition-id 40960 --end-partition-id 49152
        --object-type revision --nb-partitions 65536 --start-partition-id 49152 --end-partition-id 57344
        --object-type revision --nb-partitions 65536 --start-partition-id 57344 --end-partition-id 65536
    """  # noqa
    conf = ctx.obj["config"]
    if "storage" not in conf:
        ctx.fail("You must have a storage configured in your config file.")

    from swh.storage import get_storage

    from .storage_checker import StorageChecker

    checker = StorageChecker(
        db=ctx.obj["db"],
        storage=get_storage(**conf["storage"]),
        object_type=object_type,
        start_partition_id=start_partition_id,
        end_partition_id=end_partition_id,
        nb_partitions=nb_partitions,
    )

    checker.run()


@scrubber_check_cli_group.command(name="journal")
@click.pass_context
def scrubber_check_journal(ctx) -> None:
    """Reads a complete kafka journal, and reports corrupt objects to
    the scrubber DB."""
    conf = ctx.obj["config"]
    if "journal" not in conf:
        ctx.fail("You must have a journal configured in your config file.")

    from .journal_checker import JournalChecker

    checker = JournalChecker(
        db=ctx.obj["db"],
        journal=conf["journal"],
    )

    checker.run()


@scrubber_cli_group.command(name="locate")
@click.option("--start-object", default="swh:1:cnt:" + "00" * 20)
@click.option("--end-object", default="swh:1:snp:" + "ff" * 20)
@click.pass_context
def scrubber_locate_origins(ctx, start_object: str, end_object: str):
    """For each known corrupt object reported in the scrubber DB, looks up origins
    that may contain this object, and records them; so they can be used later
    for recovery."""
    conf = ctx.obj["config"]
    if "storage" not in conf:
        ctx.fail("You must have a storage configured in your config file.")
    if "graph" not in conf:
        ctx.fail("You must have a graph configured in your config file.")

    from swh.graph.http_client import RemoteGraphClient
    from swh.model.model import CoreSWHID
    from swh.storage import get_storage

    from .origin_locator import OriginLocator

    locator = OriginLocator(
        db=ctx.obj["db"],
        storage=get_storage(**conf["storage"]),
        graph=RemoteGraphClient(**conf["graph"]),
        start_object=CoreSWHID.from_string(start_object),
        end_object=CoreSWHID.from_string(end_object),
    )

    locator.run()


@scrubber_cli_group.command(name="fix")
@click.option("--start-object", default="swh:1:cnt:" + "00" * 20)
@click.option("--end-object", default="swh:1:snp:" + "ff" * 20)
@click.pass_context
def scrubber_fix_objects(ctx, start_object: str, end_object: str):
    """For each known corrupt object reported in the scrubber DB, looks up origins
    that may contain this object, and records them; so they can be used later
    for recovery."""
    from swh.model.model import CoreSWHID

    from .fixer import Fixer

    fixer = Fixer(
        db=ctx.obj["db"],
        start_object=CoreSWHID.from_string(start_object),
        end_object=CoreSWHID.from_string(end_object),
    )

    fixer.run()
