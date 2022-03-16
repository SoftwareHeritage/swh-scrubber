# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

import click

from swh.core.cli import CONTEXT_SETTINGS
from swh.core.cli import swh as swh_cli_group


@swh_cli_group.group(name="scrubber", context_settings=CONTEXT_SETTINGS)
@click.option(
    "--config-file",
    "-C",
    default=None,
    type=click.Path(exists=True, dir_okay=False,),
    help="Configuration file.",
)
@click.pass_context
def scrubber_cli_group(ctx, config_file):
    """main command group of the datastore scrubber
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
    """group of commands which read from data stores and report errors.
    """
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
@click.option("--start-object", default="0" * 40)
@click.option("--end-object", default="f" * 40)
@click.pass_context
def scrubber_check_storage(ctx, object_type: str, start_object: str, end_object: str):
    conf = ctx.obj["config"]
    if "storage" not in conf:
        ctx.fail("You must have a storage configured in your config file.")

    from swh.storage import get_storage

    from .check_storage import StorageChecker

    checker = StorageChecker(
        db=ctx.obj["db"],
        storage=get_storage(**conf["storage"]),
        object_type=object_type,
        start_object=start_object,
        end_object=end_object,
    )

    checker.check_storage()
