import click

from ska_src_clients.common.utility import format_output

@click.group(help="Generic configuration operations.")
def config():
    """Generic configuration operations."""

@config.command(help="Get values from a configuration file.")
@click.pass_context
def get(ctx):
    def flatten_dict(d, parent_key='', sep='.'):
        """ Recursively flatten a nested dictionary into a single-level dict. """
        items = {}
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(flatten_dict(v, new_key, sep=sep))
            else:
                items[new_key] = v
        return items

    format_output(
        flatten_dict(ctx.obj['config']) if not ctx.obj['json'] else ctx.obj['config'],
        table_field_names=["Configuration Item", "Value"],
        json_output=ctx.obj['json']
    )