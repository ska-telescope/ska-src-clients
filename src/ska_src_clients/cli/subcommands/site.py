import webbrowser

import click

from ska_src_clients.api import SiteAPI
from ska_src_clients.common.utility import format_output


@click.group(help="Operations related to sites.")
def site():
    """Site operations."""
    pass

@site.command(help="Get information about a specific site by its unique ID.")
@click.argument('site_id')
@click.pass_context
def get(ctx, site_id):
    result = SiteAPI(session=ctx.obj['session']).get_site(site_id)
    result_to_table_field_names_mapping = {
        "id": "ID",
        "parent_node_name": "Parent Node Name",
        "name": "Site Name",
        "description": "Description",
        "latitude": "Latitude",
        "longitude": "Longitude",
        "country": "Country",
        "primary_contact_email": "Primary Email",
        "secondary_contact_email": "Secondary Email",
        "is_force_disabled": "Force Disabled?"
    }

    reformatted = [{key: result.get(key, "") for key in result_to_table_field_names_mapping.keys()}]

    format_output(
        reformatted if not ctx.obj['json'] else result,
        table_field_names=result_to_table_field_names_mapping.values(),
        json_output=ctx.obj['json']
    )

@site.command(help="List sites.")
@click.option('--node', help="Filter by node name.")
@click.pass_context
def ls(ctx, node):
    result = SiteAPI(session=ctx.obj['session']).list_sites(node_name=node)
    result_to_table_field_names_mapping = {
        "id": "ID",
        "parent_node_name": "Parent Node Name",
        "name": "Site Name",
        "description": "Description",
        "latitude": "Latitude",
        "longitude": "Longitude",
        "country": "Country",
        "primary_contact_email": "Primary Email",
        "is_force_disabled": "Force Disabled?"
    }

    reformatted = []
    for data in result:
        filtered = {key: data.get(key, "") for key in
                    result_to_table_field_names_mapping.keys()}
        reformatted.append(filtered)

    format_output(
        reformatted if not ctx.obj['json'] else result,
        table_field_names=result_to_table_field_names_mapping.values(),
        json_output=ctx.obj['json']
    )
#######################
# SUBCOMMAND: compute #
#######################

@site.group(help="Operations related to compute resources.")
def compute():
    """Compute operations"""
    pass

@compute.command(help="Get compute details by ID.")
@click.argument('compute_id')
@click.pass_context
def get(ctx, compute_id):
    result = SiteAPI(session=ctx.obj['session']).get_compute(compute_id)
    result_to_table_field_names_mapping = {
        "id": "ID",
        "name": "Name",
        "parent_node_name": "Parent Node Name",
        "parent_site_name": "Site Name",
        "description": "Description",
        "hardware_type": "Hardware Type",
        "hardware_capabilities": "Capabilities",
        "is_force_disabled": "Force Disabled?"
    }

    reformatted = [{key: result.get(key, "") for key in result_to_table_field_names_mapping.keys()}]

    format_output(
        reformatted if not ctx.obj['json'] else result,
        table_field_names=result_to_table_field_names_mapping.values(),
        json_output=ctx.obj['json']
    )

@compute.command(help="List compute resources.")
@click.option('--node', help="Filter by node name.")
@click.option('--site', help="Filter by site name.")
@click.pass_context
def ls(ctx, node, site):
    result = SiteAPI(session=ctx.obj['session']).list_compute(node_name=node, site_name=site)
    result_to_table_field_names_mapping = {
        "id": "ID",
        "name": "Name",
        "parent_node_name": "Parent Node Name",
        "parent_site_name": "Site Name",
        "description": "Description",
        "hardware_capabilities": "Capabilities",
        "is_force_disabled": "Force Disabled?"
    }

    reformatted = []
    for data in result:
        filtered = {key: data.get(key, "") for key in
                    result_to_table_field_names_mapping.keys()}
        reformatted.append(filtered)

    format_output(
        reformatted if not ctx.obj['json'] else result,
        table_field_names=result_to_table_field_names_mapping.values(),
        json_output=ctx.obj['json']
    )

#######################
# SUBCOMMAND: service #
#######################

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
    result_to_table_field_names_mapping = {
        "id": "ID",
        "name": "Name",
        "parent_node_name": "Parent Node Name",
        "parent_site_name": "Parent Site Name",
        "parent_compute_id": "Parent Compute ID",
        "type": "Type",
        "scope": "Scope",
        "associated_storage_area_id": "Storage Area ID",
        "prefix": "Prefix",
        "host": "Host",
        "port": "Port",
        "path": "Path",
        "is_force_disabled": "Force Disabled?"
    }

    reformatted = [{key: result.get(key, "") for key in result_to_table_field_names_mapping.keys()}]

    format_output(
        reformatted if not ctx.obj['json'] else result,
        table_field_names=result_to_table_field_names_mapping.values(),
        json_output=ctx.obj['json']
    )


@service.command(help="List services.")
@click.option('--type', help="Filter by service type.")
@click.option('--node', help="Filter by node name.")
@click.option('--site', help="Filter by site name.")
@click.option('--scope', default='all', help="Filter by scope (global||local||all).")
@click.pass_context
def ls(ctx, type, node, site, scope):
    result = SiteAPI(session=ctx.obj['session']).list_services(service_type=type, node_name=node,
                                                               site_name=site, scope=scope)
    result_to_table_field_names_mapping = {
        "id": "ID",
        "name": "Name",
        "parent_node_name": "Parent Node Name",
        "parent_site_name": "Parent Site Name",
        "parent_compute_id": "Parent Compute ID",
        "type": "Type",
        "scope": "Scope",
        "is_force_disabled": "Force Disabled?"
    }

    reformatted = []
    for data in result:
        filtered = {key: data.get(key, "") for key in
                    result_to_table_field_names_mapping.keys()}
        reformatted.append(filtered)

    format_output(
        reformatted if not ctx.obj['json'] else result,
        table_field_names=result_to_table_field_names_mapping.values(),
        json_output=ctx.obj['json']
    )

@service.command(help="List available service types.")
@click.pass_context
def types(ctx):
    result = SiteAPI(session=ctx.obj['session']).list_service_types()

    reformatted = (
        [{'scope': 'Global', 'name': name} for name in result.get('global', [])] +
        [{'scope': 'Local', 'name': name} for name in result.get('local', [])]
    )

    format_output(
        reformatted if not ctx.obj['json'] else result,
        table_field_names=["Scope", "Name"],
        json_output=ctx.obj['json'],
    )

#######################
# SUBCOMMAND: storage #
#######################

@site.group(help="Operations related to storage resources.")
def storage():
    """Site storage operations"""
    pass

@storage.command(help="Get storage details by unique ID.")
@click.argument('storage_id')
@click.pass_context
def get(ctx, storage_id):
    result = SiteAPI(session=ctx.obj['session']).get_storage(storage_id)
    result_to_table_field_names_mapping = {
        "id": "ID",
        "name": "Name",
        "parent_node_name": "Parent Node Name",
        "parent_site_name": "Parent Site Name",
        "host": "Host",
        "base_path": "Base Path",
        "srm": "SRM",
        "device_type": "Device Type",
        "size_in_terabytes": "Size (TB)",
        "is_force_disabled": "Force Disabled?"
    }

    reformatted = [{key: result.get(key, "") for key in result_to_table_field_names_mapping.keys()}]

    format_output(
        reformatted if not ctx.obj['json'] else result,
        table_field_names=result_to_table_field_names_mapping.values(),
        json_output=ctx.obj['json']
    )

@storage.command(help="List storage resources.")
@click.option('--node', help="Filter by node name.")
@click.option('--site', help="Filter by site name.")
@click.pass_context
def ls(ctx, node, site):
    result = SiteAPI(session=ctx.obj['session']).list_storages(node_name=node, site_name=site)
    result_to_table_field_names_mapping = {
        "id": "ID",
        "name": "Name",
        "parent_node_name": "Parent Node Name",
        "parent_site_name": "Parent Site Name",
        "srm": "SRM",
        "device_type": "Device Type",
        "size_in_terabytes": "Size (TB)",
        "is_force_disabled": "Force Disabled?"
    }

    reformatted = []
    for data in result:
        filtered = {key: data.get(key, "") for key in
                    result_to_table_field_names_mapping.keys()}
        reformatted.append(filtered)

    format_output(
        reformatted if not ctx.obj['json'] else result,
        table_field_names=result_to_table_field_names_mapping.values(),
        json_output=ctx.obj['json']
    )

############################
# SUBCOMMAND: storage-area #
############################

@site.group(name='storage-area', help="Operations related to storage areas.")
def storage_area():
    """Storage area operations"""
    pass

@storage_area.command(help="Get storage area details by unique ID.")
@click.argument('storage_area_id')
@click.pass_context
def get(ctx, storage_area_id):
    result = SiteAPI(session=ctx.obj['session']).get_storage_area(storage_area_id)
    result_to_table_field_names_mapping = {
        "id": "ID",
        "name": "Name",
        "parent_node_name": "Parent Node Name",
        "parent_site_name": "Parent Site Name",
        "parent_storage_id": "Parent Storage ID",
        "type": "Type",
        "relative_path": "Relative Path",
        "tier": "Tier",
        "is_force_disabled": "Force Disabled?"
    }

    reformatted = [{key: result.get(key, "") for key in result_to_table_field_names_mapping.keys()}]

    format_output(
        reformatted if not ctx.obj['json'] else result,
        table_field_names=result_to_table_field_names_mapping.values(),
        json_output=ctx.obj['json']
    )

@storage_area.command(help="List storage areas.")
@click.option('--node', help="Filter by node name.")
@click.option('--site', help="Filter by site name.")
@click.pass_context
def ls(ctx, node, site):
    result = SiteAPI(session=ctx.obj['session']).list_storage_areas(node_name=node, site_name=site)
    result_to_table_field_names_mapping = {
        "id": "ID",
        "name": "Name",
        "parent_node_name": "Parent Node Name",
        "parent_site_name": "Parent Site Name",
        "parent_storage_id": "Parent Storage ID",
        "type": "Type",
        "relative_path": "Relative Path",
        "tier": "Tier",
        "is_force_disabled": "Force Disabled?"
    }

    reformatted = []
    for data in result:
        filtered = {key: data.get(key, "") for key in
                    result_to_table_field_names_mapping.keys()}
        reformatted.append(filtered)

    format_output(
        reformatted if not ctx.obj['json'] else result,
        table_field_names=result_to_table_field_names_mapping.values(),
        json_output=ctx.obj['json']
    )
