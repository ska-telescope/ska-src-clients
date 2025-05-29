import click

from .site import site
from ska_src_clients.api import SiteAPI
from ska_src_clients.common.utility import format_output


@site.group(help="Operations related to services.")
def service():
    """Service operations"""
    pass

@service.command(help="Disable a service by unique ID.")
@click.argument('service_id')
@click.pass_context
def disable(ctx, service_id):
    result = SiteAPI(session=ctx.obj['session']).disable_service(service_id)
    format_output(result, ctx.obj['json'])

@service.command(help="Enable a service by unique ID.")
@click.argument('service_id')
@click.pass_context
def enable(ctx, service_id):
    result = SiteAPI(session=ctx.obj['session']).enable_service(service_id)
    format_output(result, ctx.obj['json'])

@service.command(help="Get service details by unique ID.")
@click.argument('service_id')
@click.pass_context
def get(ctx, service_id):
    result = SiteAPI(session=ctx.obj['session']).get_service(service_id)
    fields = [
        "parent_node_name", "parent_site_name", "name", "id", "type", "scope",
        "parent_compute_id", "associated_storage_area_id", "prefix", "host",
        "port", "path", "is_force_disabled"
    ]
    headers = {
        "parent_node_name": "Node", "parent_site_name": "Site", "name": "Name",
        "id": "ID", "type": "Type", "scope": "Scope", "parent_compute_id": "Compute ID",
        "associated_storage_area_id": "Storage Area ID", "prefix": "Prefix",
        "host": "Host", "port": "Port", "path": "Path", "is_force_disabled": "Force Disabled?"
    }
    format_output([result], ctx.obj['json'], table_fields=fields, headers_map=headers, list_of_dicts=True)

@service.command(help="List services.")
@click.option('--type', help="Filter by service type")
@click.option('--node', help="Filter by node name")
@click.option('--site', help="Filter by site name")
@click.option('--scope', default='all', help="Scope filter (default: all)")
@click.pass_context
def ls(ctx, type, node, site, scope):
    result = SiteAPI(session=ctx.obj['session']).list_services(type, node, site, scope)
    fields = ["parent_node_name", "parent_site_name", "name", "id", "type", "scope"]
    headers = {
        "parent_node_name": "Node", "parent_site_name": "Site", "name": "Name",
        "id": "ID", "type": "Type", "scope": "Scope"
    }
    format_output(result, ctx.obj['json'], table_fields=fields, headers_map=headers, list_of_dicts=True)

@service.command(help="List available service types.")
@click.pass_context
def types(ctx):
    result = SiteAPI(session=ctx.obj['session']).list_service_types()

    if ctx.obj['json']:
        format_output(result, json_output=True)
    else:
        formatted = (
            [{'scope': 'Global', 'name': name} for name in result.get('global', [])] +
            [{'scope': 'Local', 'name': name} for name in result.get('local', [])]
        )
        format_output(
            formatted,
            json_output=False,
            table_fields=["scope", "name"],
            headers_map={"scope": "Scope", "name": "Name"},
            list_of_dicts=True
        )
