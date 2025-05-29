import click

from ska_src_clients.api import SiteAPI
from ska_src_clients.common.utility import format_output


@click.group(help="Operations related to sites.")
def site():
    """Site operations"""
    pass

@site.command(help="List sites.")
@click.pass_context
def ls(ctx):
    result = SiteAPI(session=ctx.obj['session']).list_sites()
    fields = ["parent_node_name", "name", "id", "description", "latitude", "longitude"]
    headers = {
        "parent_node_name": "Node", "name": "Site", "id": "ID",
        "description": "Description", "latitude": "Latitude", "longitude": "Longitude"
    }
    format_output(result, ctx.obj['json'], table_fields=fields, headers_map=headers, list_of_dicts=True)

@site.command(help="Get information about a specific site by its unique ID.")
@click.argument('site_id')
@click.pass_context
def get(ctx, site_id):
    result = SiteAPI(session=ctx.obj['session']).get_site(site_id)
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

