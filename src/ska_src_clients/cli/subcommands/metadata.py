import click
import json

from ska_src_clients.api import MetadataAPI
from ska_src_clients.common.utility import format_output

@click.group(help="Metadata operations for managing object metadata.")
def metadata():
    """Metadata operations."""

@metadata.command(help="Get metadata for a given DID.")
@click.option('--namespace', required=True, help='DID namespace.')
@click.option('--name', required=True, help='DID name.')
@click.option('--stores', default='file', help='Comma-separated store types: file, science')
@click.option('--showempty', is_flag=True, help='Include empty metadata fields.')
@click.pass_context
def get(ctx, namespace, name, stores, showempty):
    plugins = []
    if 'file' in stores.split(','):
        plugins.append('DID_COLUMN')
    if 'science' in stores.split(','):
        plugins.append('POSTGRES_JSON')

    outputs = {}
    for plugin in plugins:
        metadata = MetadataAPI(session=ctx.obj['session']).get_metadata(namespace, name, plugin)
        if not showempty:
            metadata = {k: v for k, v in metadata.items() if v is not None}
        outputs[plugin] = metadata

    flattened = []
    for plugin, data in outputs.items():
        for key, value in data.items():
            flattened.append({
                'store': plugin,
                'key': key,
                'value': value
            })

    format_output(
        flattened if not ctx.obj['json'] else format_output(outputs, json_output=True),
        table_field_names=["Store", "Key", "Value"],
        json_output = ctx.obj['json'],
    )

@metadata.command(
    help="Set or update metadata for a given DID. "
         "The metadata must be provided as a valid JSON string."
)
@click.option('--namespace', required=True, help='DID namespace.')
@click.option('--name', required=True, help='DID name.')
@click.option('--metadata', required=True, help='Metadata JSON string.')
@click.pass_context
def set(ctx, namespace, name, metadata):
    try:
        metadata_dict = json.loads(metadata)
    except json.JSONDecodeError:
        click.echo("Invalid JSON format for --metadata", err=True)
        ctx.exit(1)
    result = MetadataAPI(session=ctx.obj['session']).set_metadata(namespace, name, metadata_dict)

    if ctx.obj['json']:
        format_output(result, json_output=True)
    else:
        if result.get('successful'):
            click.secho("✔ Metadata updated successfully", fg='green')
        else:
            click.secho("✖ Metadata update failed", fg='red')