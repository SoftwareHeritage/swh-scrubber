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
        journal_client:
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
@click.option("--start-object", default="00" * 20)
@click.option("--end-object", default="ff" * 20)
@click.pass_context
def scrubber_check_storage(ctx, object_type: str, start_object: str, end_object: str):
    """Reads a postgresql storage, and reports corrupt objects to the scrubber DB."""
    conf = ctx.obj["config"]
    if "storage" not in conf:
        ctx.fail("You must have a storage configured in your config file.")

    from swh.storage import get_storage

    from .storage_checker import StorageChecker

    checker = StorageChecker(
        db=ctx.obj["db"],
        storage=get_storage(**conf["storage"]),
        object_type=object_type,
        start_object=start_object,
        end_object=end_object,
    )

    checker.run()


@scrubber_check_cli_group.command(name="journal")
@click.pass_context
def scrubber_check_journal(ctx) -> None:
    """Reads a complete kafka journal, and reports corrupt objects to
    the scrubber DB."""
    conf = ctx.obj["config"]
    if "journal_client" not in conf:
        ctx.fail("You must have a journal_client configured in your config file.")

    from .journal_checker import JournalChecker

    checker = JournalChecker(
        db=ctx.obj["db"],
        journal_client=conf["journal_client"],
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

    from swh.graph.client import RemoteGraphClient
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
