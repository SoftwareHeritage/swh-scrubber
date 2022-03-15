import click

from swh.core.cli import CONTEXT_SETTINGS
from swh.core.cli import swh as swh_cli_group


@swh_cli_group.group(name="scrubber", context_settings=CONTEXT_SETTINGS)
@click.pass_context
def scrubber_cli_group(ctx):
    """main command of the datastore scrubber
    """
