import click

from ska_src_clients.common.utility import flatten_dict, format_output

@click.group(name='config')
def config():
    """Configuration operations"""

@config.command()
@click.option('--section', required=True, help="Config section to retrieve")
@click.pass_context
def get(ctx, section):
    config = ctx.obj['config']
    result = config.get(section, f"Section {section} not found")

    if ctx.obj['json']:
        format_output(result, json_output=True)
    else:
        display_rows = []

        if isinstance(result, dict):
            flattened = flatten_dict(result, parent_key=section)
            for key, value in flattened.items():
                display_rows.append({
                    "key": key,
                    "value": str(value)
                })

            table_fields = ["key", "value"]
            headers_map = {"key": "Key", "value": "Value"}

            format_output(
                display_rows,
                json_output=False,
                table_fields=table_fields,
                headers_map=headers_map,
                list_of_dicts=True
            )
        else:
            # Just a single value, show directly
            format_output({section: result}, json_output=False)