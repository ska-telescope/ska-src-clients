import click

from .site import site
from ska_src_clients.api import SiteAPI
from ska_src_clients.common.utility import format_output


@site.group(help="Operations related to compute resources.")
def compute():
    """Compute operations"""
    pass

@compute.command(help="List compute resources.")
@click.option('--node', help="Filter by node name")
@click.option('--site', help="Filter by site name")
@click.pass_context
def ls(ctx, node, site):
    result = SiteAPI(session=ctx.obj['session']).list_compute(node, site)
    fields = ["parent_node_name", "parent_site_name", "id", "description"]
    headers = {
        "parent_node_name": "Node", "parent_site_name": "Site", "id": "ID",
        "description": "Description"
    }
    format_output(result, ctx.obj['json'], table_fields=fields, headers_map=headers, list_of_dicts=True)

@compute.command(help="Get compute details by ID.")
@click.argument('compute_id')
@click.option('--services', is_flag=True, help="Include associated services")
@click.pass_context
def get(ctx, compute_id, services):
    result = SiteAPI(session=ctx.obj['session']).get_compute(compute_id)
    fields = [
        "parent_node_name", "parent_site_name", "id", "description",
        "hardware_type", "hardware_capabilities"
    ]
    headers = {
        "parent_node_name": "Node", "parent_site_name": "Site", "id": "ID",
        "description": "Description", "hardware_type": "Hardware Type",
        "hardware_capabilities": "Capabilities"
    }
    if services:
        svc_info = "\n".join(
            f"{svc.get('id')} / {svc.get('type')} / {svc.get('name')}"
            for svc in result.get('associated_local_services', [])
        )
        result['services'] = svc_info
        fields.append('services')
        headers['services'] = 'Services (ID / Type / Name)'

    format_output([result], ctx.obj['json'], table_fields=fields, headers_map=headers, list_of_dicts=True)