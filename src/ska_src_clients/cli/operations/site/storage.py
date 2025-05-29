import click

from .site import site
from ska_src_clients.api import SiteAPI
from ska_src_clients.common.utility import format_output


@site.group(help="Operations related to storage resources.")
def storage():
    """Site storage operations"""
    pass

@storage.command(help="List storage resources.")
@click.option('--node', help="Filter by node name")
@click.option('--site', help="Filter by site name")
@click.pass_context
def ls(ctx, node, site):
    result = SiteAPI(session=ctx.obj['session']).list_storages(node, site)

    if ctx.obj['json']:
        format_output(result, json_output=True)
    else:
        display_rows = []
        for entry in result:
            row = {
                "parent_node": entry.get("parent_node_name"),
                "parent_site": entry.get("parent_site_name"),
                "name": entry.get("name", "-"),
                "id": entry.get("id"),
                "host": entry.get("host"),
                "base_path": entry.get("base_path"),
                "srm": entry.get("srm"),
                "device_type": entry.get("device_type", "-"),
                "size_tb": entry.get("size_in_terabytes", "-"),
            }
            display_rows.append(row)

        table_fields = [
            "parent_node", "parent_site", "name", "id", "host",
            "base_path", "srm", "device_type", "size_tb"
        ]
        headers_map = {
            "parent_node": "Parent Node",
            "parent_site": "Parent Site",
            "name": "Name",
            "id": "ID",
            "host": "Host",
            "base_path": "Base Path",
            "srm": "SRM",
            "device_type": "Device Type",
            "size_tb": "Size (TB)"
        }

        format_output(
            display_rows,
            json_output=False,
            table_fields=table_fields,
            headers_map=headers_map,
            list_of_dicts=True
        )

@storage.command(help="Get storage details by unique ID.")
@click.argument('storage_id')
@click.option('--areas', is_flag=True, help="Include associated storage areas")
@click.pass_context
def get(ctx, storage_id, areas):
    result = SiteAPI(session=ctx.obj['session']).get_storage(storage_id)

    if ctx.obj['json']:
        format_output(result, json_output=True)
    else:
        protocols = ", ".join(
            f"{proto.get('prefix')} / {proto.get('port')}"
            for proto in result.get("supported_protocols", [])
        )

        row = {
            "parent_node": result.get("parent_node_name"),
            "parent_site": result.get("parent_site_name"),
            "name": result.get("name", "-"),
            "id": result.get("id"),
            "host": result.get("host"),
            "base_path": result.get("base_path"),
            "srm": result.get("srm"),
            "device_type": result.get("device_type", "-"),
            "size_tb": result.get("size_in_terabytes", "-"),
            "protocols": protocols or "-"
        }

        table_fields = [
            "parent_node", "parent_site", "name", "id", "host",
            "base_path", "srm", "device_type", "size_tb", "protocols"
        ]
        headers_map = {
            "parent_node": "Parent Node",
            "parent_site": "Parent Site",
            "name": "Name",
            "id": "ID",
            "host": "Host",
            "base_path": "Base Path",
            "srm": "SRM",
            "device_type": "Device Type",
            "size_tb": "Size (TB)",
            "protocols": "Protocols (Prefix / Port)"
        }

        if areas:
            area_list = result.get("areas", [])
            area_str = ", ".join(f"{a.get('id')} / {a.get('name')}" for a in area_list)
            row["areas"] = area_str or "-"
            table_fields.append("areas")
            headers_map["areas"] = "Areas (ID / Name)"

        format_output(
            [row],
            json_output=False,
            table_fields=table_fields,
            headers_map=headers_map,
            list_of_dicts=True
        )
