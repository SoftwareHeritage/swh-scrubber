# Copyright (C) 2022-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from typing import Optional
import warnings

import click

from swh.core.cli import CONTEXT_SETTINGS
from swh.core.cli import swh as swh_cli_group
from swh.model.swhids import ObjectType


def _fix_sphinx_docstring():
    """Remove \b markers used by click to prevent text rewrapping in docstring."""

    def decorator(f):
        if "SWH_DOC_BUILD" in os.environ:
            f.__doc__ = f.__doc__.replace("\b", "")
        return f

    return decorator


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
@_fix_sphinx_docstring()
def scrubber_cli_group(ctx, config_file: Optional[str]) -> None:
    """main command group of the datastore scrubber

    \b
    Expected config format::
        \b
        scrubber:
            cls: postgresql
            db: "service=..."    # libpq DSN
        \b
        # for storage checkers + origin locator only:
        storage:
            cls: postgresql     # cannot be remote for checkers, as they need direct
                                # access to the pg DB
            db": "service=..."  # libpq DSN
            objstorage:
                cls: memory
        \b
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

    ctx.ensure_object(dict)
    ctx.obj["config"] = conf
    if "scrubber_db" in conf:
        warnings.warn(
            "the 'scrubber_db' configuration section has been renamed to 'scrubber'; "
            f"please update your configuration file {config_file}",
            DeprecationWarning,
        )
        conf["scrubber"] = conf.pop("scrubber_db")

    if "scrubber" in conf:
        ctx.obj["db"] = get_scrubber_db(**conf["scrubber"])


@scrubber_cli_group.group(name="check")
@click.pass_context
def scrubber_check_cli_group(ctx):
    """group of commands which read from data stores and report errors."""
    pass


@scrubber_check_cli_group.command(name="init")
@click.argument("backend", type=click.Choice(["storage", "journal", "objstorage"]))
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
            "content",
            # TODO:
            # "raw_extrinsic_metadata",
            # "extid",
        ]
    ),
)
@click.option("--nb-partitions", default=4096, type=int)
@click.option("--name", default=None, type=str)
@click.option("--check-hashes/--no-check-hashes", default=True)
@click.option("--check-references/--no-check-references", default=None)
@click.pass_context
def scrubber_check_init(
    ctx,
    backend: str,
    object_type: str,
    nb_partitions: int,
    name: Optional[str],
    check_hashes: bool,
    check_references: Optional[bool],
):
    """Initialise a scrubber check configuration for the datastore defined in the
    configuration file and given object_type.

    A checker configuration configuration consists simply in a set of:

    - backend: the datastore type being scrubbed (storage, objstorage or journal),

    - object-type: the type of object being checked,

    - nb-partitions: the number of partitions the hash space is divided
      in; must be a power of 2,

    - name: an unique name for easier reference,

    - check-hashes: flag (default to True) to select the hash validation step for
      this scrubbing configuration,

    - check-references: flag (default to True for storage and False for the journal
      backend) to select the reference validation step for this scrubbing configuration.
    """
    if not object_type or not name:
        raise click.ClickException(
            "Invalid parameters: you must provide the object type and configuration name"
        )

    conf = ctx.obj["config"]
    if "db" not in ctx.obj:
        ctx.fail("You must have a scrubber configured in your config file.")
    db = ctx.obj["db"]

    if backend == "storage":
        if check_references is None:
            check_references = True
        if "storage" not in conf:
            raise click.ClickException(
                "You must have a storage configured in your config file."
            )
        from swh.storage import get_storage

        from .storage_checker import get_datastore as get_storage_datastore

        datastore = get_storage_datastore(storage=get_storage(**conf["storage"]))
        db.datastore_get_or_add(datastore)
    elif backend == "journal":
        if check_references is None:
            check_references = False
        if "journal" not in conf:
            raise click.ClickException(
                "You must have a journal configured in your config file."
            )
        from .journal_checker import get_datastore as get_journal_datastore

        datastore = get_journal_datastore(journal_cfg=conf["journal"])
        db.datastore_get_or_add(datastore)
        nb_partitions = 1
    elif backend == "objstorage":
        if check_references is None:
            check_references = True
        if object_type != "content":
            raise click.ClickException(
                "Object storage scrubber can only check content objects, "
                f"not {object_type} ones."
            )

        if "objstorage" not in conf:
            raise click.ClickException(
                "You must have an object storage configured in your config file."
            )
        from .objstorage_checker import get_objstorage_datastore

        datastore = get_objstorage_datastore(objstorage_config=conf["objstorage"])
    else:
        raise click.ClickException(f"Backend type {backend} is not supported")

    if db.config_get_by_name(name):
        raise click.ClickException(f"Configuration {name} already exists")

    assert check_references is not None

    config_id = db.config_add(
        name,
        datastore,
        getattr(ObjectType, object_type.upper()),
        nb_partitions,
        check_hashes=check_hashes,
        check_references=check_references,
    )
    click.echo(
        f"Created configuration {name} [{config_id}] for checking {object_type} "
        f"in {datastore.cls} {datastore.package}"
    )


@scrubber_check_cli_group.command(name="list")
@click.pass_context
def scrubber_check_list(
    ctx,
):
    """List the know configurations"""
    conf = ctx.obj["config"]
    if "db" not in ctx.obj:
        ctx.fail("You must have a scrubber configured in your config file.")
    if "storage" not in conf:
        ctx.fail("You must have a storage configured in your config file.")
    db = ctx.obj["db"]

    for id_, cfg in db.config_iter():
        ds = cfg.datastore
        if not ds:
            click.echo(
                f"[{id_}] {cfg.name}: Invalid configuration entry; datastore not found"
            )
        else:
            click.echo(
                f"[{id_}] {cfg.name}: {cfg.object_type}, {cfg.nb_partitions}, "
                f"{ds.package}:{ds.cls} ({ds.instance})"
            )


@scrubber_check_cli_group.command(name="stalled")
@click.argument(
    "name",
    type=str,
    default=None,
    required=False,  # can be given by config_id instead
)
@click.option(
    "--config-id",
    type=int,
)
@click.option(
    "--for",
    "delay",
    type=str,
    default="auto",
    help="Delay for a partition to be considered as stuck; in seconds or 'auto'",
)
@click.option(
    "--reset",
    is_flag=True,
    default=False,
    help="Reset the stalled partition so it can be grabbed by a scrubber worker",
)
@click.pass_context
def scrubber_check_stalled(
    ctx, name: str, config_id: int, delay: Optional[str], reset: bool
):
    """List the stuck partitions for a given config"""
    import datetime

    from humanize import naturaldate, naturaldelta

    if "db" not in ctx.obj:
        ctx.fail("You must have a scrubber configured in your config file.")
    db = ctx.obj["db"]
    if name and config_id is None:
        config_id = db.config_get_by_name(name)

    if config_id is None:
        raise click.ClickException("A valid configuration name/id must be set")

    cfg = db.config_get(config_id)
    delay_td: Optional[datetime.timedelta]
    if delay == "auto":
        delay_td = None
    elif delay:
        delay_td = datetime.timedelta(seconds=int(delay))
    in_flight = list(db.checked_partition_get_stuck(config_id, delay_td))
    if in_flight:
        click.echo(
            f"Stuck partitions for {cfg.name} [id={config_id}, "
            f"type={cfg.object_type.name.lower()}]:"
        )
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        for partition, stuck_since in in_flight:
            click.echo(
                f"{partition}:\tstuck since {naturaldate(stuck_since)} "
                f"({naturaldelta(now-stuck_since)})"
            )
            if reset:
                if db.checked_partition_reset(config_id, partition):
                    click.echo("\tpartition reset")
                else:
                    click.echo("\tpartition NOT reset")

    else:
        click.echo(
            f"No stuck partition found for {cfg.name} [id={config_id}, "
            f"type={cfg.object_type.name.lower()}]"
        )


@scrubber_check_cli_group.command(name="running")
@click.argument(
    "name",
    type=str,
    default=None,
    required=False,  # can be given by config_id instead
)
@click.option(
    "--config-id",
    type=int,
)
@click.pass_context
def scrubber_check_running(ctx, name: str, config_id: int):
    """List partitions being checked for the check session <name>"""
    import datetime

    from humanize import naturaldate, naturaldelta

    if "db" not in ctx.obj:
        ctx.fail("You must have a scrubber configured in your config file.")
    db = ctx.obj["db"]
    if name and config_id is None:
        config_id = db.config_get_by_name(name)

    if config_id is None:
        raise click.ClickException("A valid configuration name/id must be set")

    cfg = db.config_get(config_id)
    in_flight = list(db.checked_partition_get_running(config_id))
    if in_flight:
        click.echo(
            f"Running partitions for {cfg.name} [id={config_id}, "
            f"type={cfg.object_type.name.lower()}]:"
        )
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        for partition, running_since in in_flight:
            click.echo(
                f"{partition}:\trunning since {naturaldate(running_since)} "
                f"({naturaldelta(now-running_since)})"
            )
    else:
        click.echo(
            f"No running partition found for {cfg.name} [id={config_id}, "
            f"type={cfg.object_type.name.lower()}]"
        )


@scrubber_check_cli_group.command(name="stats")
@click.argument(
    "name",
    type=str,
    default=None,
    required=False,  # can be given by config_id instead
)
@click.option(
    "--config-id",
    type=int,
)
@click.option(
    "-j",
    "--json",
    "json_format",
    is_flag=True,
)
@click.pass_context
def scrubber_check_stats(ctx, name: str, config_id: int, json_format: bool):
    """Display statistics for the check session <name>"""
    from dataclasses import asdict
    from json import dumps
    import textwrap

    from humanize import naturaldelta

    if "db" not in ctx.obj:
        ctx.fail("You must have a scrubber configured in your config file.")
    db = ctx.obj["db"]
    if name and config_id is None:
        config_id = db.config_get_by_name(name)

    if config_id is None:
        raise click.ClickException("A valid configuration name/id must be set")

    cfg = db.config_get(config_id)
    nb_partitions = cfg.nb_partitions
    stats = db.config_get_stats(config_id)

    if json_format:
        stats["config"] = asdict(stats["config"])
        stats["config"]["object_type"] = stats["config"]["object_type"].name.lower()
        click.echo(dumps(stats, indent=2))
    else:
        percentage = stats["checked_partition"] / nb_partitions * 100.0
        click.echo(
            textwrap.dedent(
                f"""\
                Check session {name} ({config_id}):
                  object type: {cfg.object_type.name}
                  datastore: {cfg.datastore.instance}
                  check hashes: {cfg.check_hashes}
                  check references: {cfg.check_references}
                  partitions:
                    total: {nb_partitions}
                    running: {stats['running_partition']}
                    done: {stats['checked_partition']} ({percentage:.2f}%)"""
            )
        )
        if stats["checked_partition"]:
            click.echo(
                "  "
                + textwrap.dedent(
                    f"""\
                    duration:
                        min: {naturaldelta(stats['min_duration'])}
                        avg: {naturaldelta(stats['avg_duration'])}
                        max: {naturaldelta(stats['max_duration'])}"""
                )
            )
        if cfg.check_hashes:
            click.echo(f"  corrupted objects: {stats['corrupt_object']}")
        if cfg.check_references:
            click.echo(f"  missing objects: {stats['missing_object']}")
            click.echo(f"  from references: {stats['missing_object_reference']}")


@scrubber_check_cli_group.command(name="run")
@click.argument(
    "name",
    type=str,
    default=None,
    required=False,  # can be given by config_id instead
)
@click.option(
    "--config-id",
    type=int,
    default=None,
    help="Config ID (is config name is not given as argument)",
)
@click.option(
    "--use-journal",
    is_flag=True,
    default=False,
    help=(
        "Flag only relevant for running an object storage scrubber, "
        "if set content ids are consumed from a kafka topic of SWH journal "
        "instead of getting them from a storage"
    ),
)
@click.option("--limit", default=0, type=int)
@click.pass_context
def scrubber_check_run(
    ctx,
    name: Optional[str],
    config_id: Optional[int],
    use_journal: bool,
    limit: int,
):
    """Run the scrubber checker configured as `name` and reports corrupt
    objects to the scrubber DB.

    This runs a single thread; parallelism is achieved by running this command
    multiple times.

    This command references an existing scrubbing configuration (either by name
    or by id); the configuration holds the object type, number of partitions
    and the storage configuration this scrubbing session will check on.

    """
    if "db" not in ctx.obj:
        ctx.fail("You must have a scrubber configured in your config file.")
    db = ctx.obj["db"]
    if name and config_id is None:
        config_id = db.config_get_by_name(name)

    if config_id is None:
        ctx.fail("A valid configuration name/id must be given.")

    from swh.scrubber.base_checker import BaseChecker

    scrubber_cfg = db.config_get(config_id)
    datastore = scrubber_cfg.datastore
    conf = ctx.obj["config"]

    assert config_id is not None
    checker: BaseChecker

    if datastore.package == "storage":
        if "storage" not in conf:
            ctx.fail("You must have a storage configured in your config file.")
        from swh.storage import get_storage

        from .storage_checker import StorageChecker

        checker = StorageChecker(
            db=db,
            storage=get_storage(**conf["storage"]),
            config_id=config_id,
            limit=limit,
        )
    elif datastore.package == "objstorage":
        if not use_journal and "storage" not in conf:
            ctx.fail("You must have a storage configured in your config file.")
        if use_journal and "journal" not in conf:
            ctx.fail("You must have a journal configured in your config file.")
        if "objstorage" not in conf:
            ctx.fail("You must have an object storage configured in your config file.")
        from swh.objstorage.factory import get_objstorage

        if use_journal:
            from .objstorage_checker import ObjectStorageCheckerFromJournal

            checker = ObjectStorageCheckerFromJournal(
                db=db,
                journal_client_config=conf["journal"],
                objstorage=get_objstorage(**conf["objstorage"]),
                config_id=config_id,
            )
        else:
            from swh.storage import get_storage

            from .objstorage_checker import ObjectStorageCheckerFromStoragePartition

            checker = ObjectStorageCheckerFromStoragePartition(
                db=db,
                storage=get_storage(**conf["storage"]),
                objstorage=get_objstorage(**conf["objstorage"]),
                config_id=config_id,
                limit=limit,
            )
    elif datastore.package == "journal":
        if "journal" not in conf:
            ctx.fail("You must have a journal configured in your config file.")
        from .journal_checker import JournalChecker

        checker = JournalChecker(
            db=db,
            journal_client_config=conf["journal"],
            config_id=config_id,
        )
    else:
        ctx.fail(f"Unsupported scruber package {datastore.package}")

    checker.run()


@scrubber_check_cli_group.command(name="storage", deprecated=True, hidden=True)
@click.argument(
    "name",
    type=str,
    default=None,
    required=False,  # can be given by config_id instead
)
@click.option(
    "--config-id",
    type=int,
    default=None,
    help="Config ID (is config name is not given as argument)",
)
@click.option("--limit", default=0, type=int)
@click.pass_context
def scrubber_check_storage(
    ctx,
    name: Optional[str],
    config_id: Optional[int],
    limit: int,
):
    """Reads a swh-storage instance, and reports corrupt objects to the scrubber DB.

    This runs a single thread; parallelism is achieved by running this command
    multiple times.

    This command references an existing scrubbing configuration (either by name
    or by id); the configuration holds the object type, number of partitions
    and the storage configuration this scrubbing session will check on.

    All objects of type ``object_type`` are ordered, and split into the given
    number of partitions.

    Then, this process will check all partitions. The status of the ongoing
    check session is stored in the database, so the number of concurrent
    workers can be dynamically adjusted.

    """
    conf = ctx.obj["config"]
    if "db" not in ctx.obj:
        ctx.fail("You must have a scrubber configured in your config file.")
    if "storage" not in conf:
        ctx.fail("You must have a storage configured in your config file.")
    db = ctx.obj["db"]

    from swh.storage import get_storage

    from .storage_checker import StorageChecker

    if name and config_id is None:
        from .storage_checker import get_datastore as get_storage_datastore

        cfg = conf["storage"]
        datastore = get_storage_datastore(storage=get_storage(**cfg))
        datastore_id = db.datastore_get_or_add(datastore)
        config_id = db.config_get_by_name(name, datastore_id)
    elif name is None and config_id is not None:
        assert db.config_get(config_id) is not None

    if config_id is None:
        raise click.ClickException("A valid configuration name/id must be set")

    checker = StorageChecker(
        db=ctx.obj["db"],
        storage=get_storage(**conf["storage"]),
        config_id=config_id,
        limit=limit,
    )

    checker.run()


@scrubber_check_cli_group.command(name="journal", deprecated=True, hidden=True)
@click.argument(
    "name",
    type=str,
    default=None,
    required=False,  # can be given by config_id instead
)
@click.option(
    "--config-id",
    type=int,
    help="Config ID (is config name is not given as argument)",
)
@click.pass_context
def scrubber_check_journal(ctx, name, config_id) -> None:
    """Reads a complete kafka journal, and reports corrupt objects to
    the scrubber DB."""
    conf = ctx.obj["config"]
    if "db" not in ctx.obj:
        ctx.fail("You must have a scrubber configured in your config file.")
    if "journal" not in conf:
        ctx.fail("You must have a journal configured in your config file.")
    db = ctx.obj["db"]

    if name and config_id is None:
        from .journal_checker import get_datastore as get_journal_datastore

        cfg = conf["journal"]
        datastore = get_journal_datastore(journal_cfg=cfg)
        datastore_id = db.datastore_get_or_add(datastore)
        config_id = db.config_get_by_name(name, datastore_id)
    elif name is None and config_id is not None:
        assert db.config_get(config_id) is not None

    if config_id is None:
        raise click.ClickException("A valid configuration name/id must be set")

    from .journal_checker import JournalChecker

    checker = JournalChecker(
        db=ctx.obj["db"],
        journal_client_config=conf["journal"],
        config_id=config_id,
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
