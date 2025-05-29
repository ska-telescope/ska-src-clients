import click

from .site import site
from ska_src_clients.api import SiteAPI
from ska_src_clients.common.utility import format_output


@site.group(name='storage-area', help="Operations related to storage areas.")
def storage_area():
    """Storage area operations"""
    pass

@storage_area.command(help="Get storage area details by unique ID.")
@click.argument('storage_area_id')
@click.pass_context
def get(ctx, storage_area_id):
    result = SiteAPI(session=ctx.obj['session']).get_storage_area(storage_area_id)

    if ctx.obj['json']:
        format_output(result, json_output=True)
    else:
        row = {
            "parent_node": result.get("parent_node_name"),
            "parent_site": result.get("parent_site_name"),
            "name": result.get("name", "-"),
            "id": result.get("id"),
            "parent_storage_id": result.get("parent_storage_id"),
            "type": result.get("type"),
            "relative_path": result.get("relative_path", "-"),
            "tier": result.get("tier", "-")
        }

        table_fields = [
            "parent_node", "parent_site", "name", "id",
            "parent_storage_id", "type", "relative_path", "tier"
        ]
        headers_map = {
            "parent_node": "Parent Node",
            "parent_site": "Parent Site",
            "name": "Name",
            "id": "ID",
            "parent_storage_id": "Parent Storage ID",
            "type": "Type",
            "relative_path": "Relative Path",
            "tier": "Tier"
        }

        format_output(
            [row],
            json_output=False,
            table_fields=table_fields,
            headers_map=headers_map,
            list_of_dicts=True
        )

@storage_area.command(help="List storage areas.")
@click.option('--node', help="Filter by node name")
@click.option('--site', help="Filter by site name")
@click.pass_context
def ls(ctx, node, site):
    result = SiteAPI(session=ctx.obj['session']).list_storage_areas(node, site)

    if ctx.obj['json']:
        format_output(result, json_output=True)
    else:
        display_rows = []
        for entry in result:
            row = {
                "parent_node": entry.get("parent_node_name"),
                "parent_site": entry.get("parent_site_name"),
                "parent_storage_id": entry.get("parent_storage_id"),
                "id": entry.get("id"),
                "type": entry.get("type"),
                "relative_path": entry.get("relative_path", "-"),
                "name": entry.get("name", "-"),
                "tier": entry.get("tier", "-")
            }
            display_rows.append(row)

        table_fields = [
            "parent_node", "parent_site", "parent_storage_id",
            "id", "type", "relative_path", "name", "tier"
        ]
        headers_map = {
            "parent_node": "Parent Node",
            "parent_site": "Parent Site",
            "parent_storage_id": "Parent Storage ID",
            "id": "ID",
            "type": "Type",
            "relative_path": "Relative Path",
            "name": "Name",
            "tier": "Tier"
        }

        format_output(
            display_rows,
            json_output=False,
            table_fields=table_fields,
            headers_map=headers_map,
            list_of_dicts=True
        )
