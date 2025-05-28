import click

from ska_src_clients.api import SiteAPI
from ska_src_clients.common.utility import format_output

@click.group(help="Site operations: manage sites, compute, services, storage, and storage areas.")
def site():
    """Site operations"""

# --- Site Commands ---
@site.command(
    help="List all sites."
)
@click.pass_context
def ls(ctx):
    """List all sites"""
    result = SiteAPI(session=ctx.obj['session']).list_sites()
    fields = ["parent_node_name", "name", "id", "description", "latitude", "longitude"]
    headers = {
        "parent_node_name": "Node", "name": "Site", "id": "ID",
        "description": "Description", "latitude": "Latitude", "longitude": "Longitude"
    }
    format_output(result, ctx.obj['json'], table_fields=fields, headers_map=headers, list_of_dicts=True)

@site.command(
    help="Get detailed information about a specific site by its unique ID."
)
@click.option('--id', required=True, help="Site ID")
@click.pass_context
def get(ctx, id):
    """Get details of a site by ID"""
    result = SiteAPI(session=ctx.obj['session']).get_site(id)
    fields = [
        "parent_node_name", "name", "id", "description", "latitude", "longitude",
        "country", "primary_contact_email", "secondary_contact_email"
    ]
    headers = {
        "parent_node_name": "Node", "name": "Site", "id": "ID",
        "description": "Description", "latitude": "Latitude", "longitude": "Longitude",
        "country": "Country", "primary_contact_email": "Primary Email",
        "secondary_contact_email": "Secondary Email"
    }
    format_output([result], ctx.obj['json'], table_fields=fields, headers_map=headers, list_of_dicts=True)

# --- Compute Subgroup ---
@site.group(help="Operations related to compute resources at sites.")
def compute():
    """Site compute operations"""

@compute.command(
    short_help="Get compute details",
    help="Retrieve details about a compute resource by ID, optionally including its associated services."
)
@click.option('--id', required=True, help="Compute ID")
@click.option('--services', is_flag=True, help="Include associated services")
@click.pass_context
def get(ctx, id, services):
    """Get compute details"""
    result = SiteAPI(session=ctx.obj['session']).get_compute(id)
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
            f"{svc['id']} / {svc['type']} / {svc['name']}"
            for svc in result.get('associated_local_services', [])
        )
        result['services'] = svc_info
        fields.append('services')
        headers['services'] = 'Services (ID / Type / Name)'
    format_output([result], ctx.obj['json'], table_fields=fields, headers_map=headers, list_of_dicts=True)

@compute.command(
    short_help="List compute resources",
    help="List all compute resources under a specific node or site."
)
@click.option('--node', help="Filter by node name")
@click.option('--site', help="Filter by site name")
@click.pass_context
def ls(ctx, node, site):
    """List compute resources"""
    result = SiteAPI(session=ctx.obj['session']).list_compute(node, site)
    fields = ["parent_node_name", "parent_site_name", "id", "description"]
    headers = {
        "parent_node_name": "Node", "parent_site_name": "Site", "id": "ID",
        "description": "Description"
    }
    format_output(result, ctx.obj['json'], table_fields=fields, headers_map=headers, list_of_dicts=True)

# --- Service Subgroup ---
@site.group(help="Operations related to site services.")
def service():
    """Site service operations"""

@service.command(
    short_help="Get service details",
    help="Get detailed information about a service by its unique ID."
)
@click.option('--id', required=True, help="Service ID")
@click.pass_context
def get(ctx, id):
    """Get details of a service"""
    result = SiteAPI(session=ctx.obj['session']).get_service(id)
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

@service.command(
    short_help="Enable a service",
    help="Enable a service by providing its unique UUID."
)
@click.argument('service_uuid')
@click.pass_context
def enable(ctx, service_uuid):
    """Enable a service"""
    result = SiteAPI(session=ctx.obj['session']).enable_service(service_uuid)
    format_output(result, ctx.obj['json'])

@service.command(
    short_help="Disable a service",
    help="Disable a service by providing its unique UUID."
)
@click.argument('service_uuid')
@click.pass_context
def disable(ctx, service_uuid):
    """Disable a service"""
    result = SiteAPI(session=ctx.obj['session']).disable_service(service_uuid)
    format_output(result, ctx.obj['json'])

@service.command(
    short_help="List services",
    help="List all services, optionally filtered by type, node, site, or scope."
)
@click.option('--type', help="Filter by service type")
@click.option('--node', help="Filter by node name")
@click.option('--site', help="Filter by site name")
@click.option('--scope', default='all', help="Scope filter (default: all)")
@click.pass_context
def ls(ctx, type, node, site, scope):
    """List services"""
    result = SiteAPI(session=ctx.obj['session']).list_services(type, node, site, scope)
    fields = ["parent_node_name", "parent_site_name", "name", "id", "type", "scope"]
    headers = {
        "parent_node_name": "Node", "parent_site_name": "Site", "name": "Name",
        "id": "ID", "type": "Type", "scope": "Scope"
    }
    format_output(result, ctx.obj['json'], table_fields=fields, headers_map=headers, list_of_dicts=True)

@service.command(
    short_help="List service types",
    help="List all available service types, both global and local."
)
@click.pass_context
def types(ctx):
    """List service types"""
    result = SiteAPI(session=ctx.obj['session']).list_service_types()
    formatted = [
        {'scope': 'Global', 'name': name} for name in result.get('global', [])
    ] + [
        {'scope': 'Local', 'name': name} for name in result.get('local', [])
    ]
    format_output(
        formatted, ctx.obj['json'],
        table_fields=["scope", "name"],
        headers_map={"scope": "Scope", "name": "Name"},
        list_of_dicts=True
    )

# --- Storage Subgroup ---
@site.group(help="Operations related to site storage resources.")
def storage():
    """Site storage operations"""

@storage.command(
    short_help="Get storage details",
    help="Get detailed information about a storage resource by its unique ID."
)
@click.argument('id')
@click.pass_context
def get(ctx, id):
    """Get details of a storage"""
    result = SiteAPI(session=ctx.obj['session']).get_storage(id)
    format_output(result, ctx.obj['json'])

@storage.command(
    short_help="List storage resources",
    help="List all storage resources, optionally filtered by node or site."
)
@click.option('--node', help="Filter by node name")
@click.option('--site', help="Filter by site name")
@click.pass_context
def ls(ctx, node, site):
    """List storage resources"""
    result = SiteAPI(session=ctx.obj['session']).list_storages(node, site)
    format_output(result, ctx.obj['json'])

# --- Storage-Area Subgroup ---
@site.group(name='storage-area', help="Operations related to storage areas within sites.")
def storage_area():
    """Site storage area operations"""

@storage_area.command(
    short_help="Get storage area details",
    help="Get detailed information about a storage area by its unique ID."
)
@click.argument('id')
@click.pass_context
def get(ctx, id):
    """Get details of a storage area"""
    result = SiteAPI(session=ctx.obj['session']).get_storage_area(id)
    format_output(result, ctx.obj['json'])

@storage_area.command(
    short_help="List storage areas",
    help="List all storage areas, optionally filtered by node or site."
)
@click.option('--node', help="Filter by node name")
@click.option('--site', help="Filter by site name")
@click.pass_context
def ls(ctx, node, site):
    """List storage areas"""
    result = SiteAPI(session=ctx.obj['session']).list_storage_areas(node, site)
    format_output(result, ctx.obj['json'])
