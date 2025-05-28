import click
from ska_src_clients.api import StatusAPI
from ska_src_clients.common.utility import format_output

@click.group()
def api():
    """API operations"""

@api.command(help="Check the health status of a service")
@click.argument('service')
@click.pass_context
def health(ctx, service):
    result = StatusAPI(session=ctx.obj['session']).health(service)
    format_output(result, ctx.obj['json'])

@api.command(help="List all configured API services")
@click.pass_context
def ls(ctx):
    result = sorted(list(ctx.obj['config'].get('apis', {}).keys()))
    format_output(result, ctx.obj['json'])

@api.command(help="Ping a service to check connectivity")
@click.argument('service')
@click.pass_context
def ping(ctx, service):
    result = StatusAPI(session=ctx.obj['session']).ping(service)
    format_output(result, ctx.obj['json'])