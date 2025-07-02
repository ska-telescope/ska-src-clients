import click
from ska_src_clients.api import StatusAPI
from ska_src_clients.common.utility import format_output

@click.group(help="Generic operations against the SRCNet APIs.")
def api():
    """SRCNet API operations."""

@api.command(help="Check the health status of an API.")
@click.argument('api_name')
@click.pass_context
def health(ctx, api_name):
    result = StatusAPI(session=ctx.obj['session']).health(api_name)
    format_output(result, ctx.obj['json'])

@api.command(help="List all configured API services.")
@click.pass_context
def ls(ctx):
    result = sorted(list(ctx.obj['config'].get('apis', {}).keys()))
    format_output(
        result,
        table_field_names=["Name"],
        json_output=ctx.obj['json']
    )

@api.command(help="Ping an API to check connectivity.")
@click.argument('api_name')
@click.pass_context
def ping(ctx, api_name):
    result = StatusAPI(session=ctx.obj['session']).ping(api_name)
    format_output(result, ctx.obj['json'])