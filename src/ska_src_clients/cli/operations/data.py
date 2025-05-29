import click

from ska_src_clients.api import DataAPI, SiteAPI
from ska_src_clients.common.utility import format_output, plot_scatter_world_map, url_to_parts

@click.group()
def data():
    """Data operations"""

@data.command()
@click.option('--namespace', required=True)
@click.option('--name', required=True)
@click.option('--sort', default='nearest_by_ip')
@click.option('--ip_address', default='')
@click.option('--no_verify', is_flag=True)
@click.option('--output')
@click.pass_context
def download(ctx, namespace, name, sort, ip_address, no_verify, output):
    """Download data"""
    result = DataAPI(session=ctx.obj['session']).download(namespace, name, sort, ip_address, not no_verify, output)
    format_output(result, ctx.obj['json'])

@data.command()
@click.option('--namespace', required=True)
@click.option('--name', required=True)
@click.option('--sort', default='nearest_by_ip')
@click.option('--ip_address', default='')
@click.pass_context
def locate(ctx, namespace, name, sort, ip_address, plot):
    """Locate data"""
    result = DataAPI(session=ctx.obj['session']).locate(namespace, name, sort, ip_address)
    format_output(result, ctx.obj['json'])

@data.command()
@click.option('--namespace', required=True)
@click.option('--name', required=True)
@click.option('--detail', is_flag=True)
@click.option('--filters', default=None)
@click.option('--limit', default=100)
@click.pass_context
def ls(ctx, namespace, name, detail, filters, limit):
    """List files in namespace"""
    result = DataAPI(session=ctx.obj['session']).list_files_in_namespace(namespace, name, detail, filters, limit)
    format_output(result, ctx.obj['json'])

@data.command()
@click.option('--to_storage_area_uuid', required=True)
@click.option('--dids', multiple=True, required=True)
@click.option('--lifetime', required=True)
@click.option('--parent_namespace')
@click.pass_context
def move_request(ctx, to_storage_area_uuid, dids, lifetime, parent_namespace):
    """Request data move"""
    result = DataAPI(session=ctx.obj['session']).move_request(to_storage_area_uuid, dids, lifetime, parent_namespace)
    format_output(result, ctx.obj['json'])

@data.command()
@click.option('--job_id', required=True)
@click.pass_context
def move_status(ctx, job_id):
    """Check move status"""
    result = DataAPI(session=ctx.obj['session']).move_status(job_id)
    format_output(result, ctx.obj['json'])

@data.command()
@click.option('--to_storage_area_uuid', required=True)
@click.option('--dids', multiple=True, required=True)
@click.option('--lifetime', required=True)
@click.option('--parent_namespace')
@click.pass_context
def stage_request(ctx, to_storage_area_uuid, dids, lifetime, parent_namespace):
    """Request data staging"""
    result = DataAPI(session=ctx.obj['session']).stage_request(to_storage_area_uuid, dids, lifetime, parent_namespace)
    format_output(result, ctx.obj['json'])

@data.command()
@click.option('--job_id', required=True)
@click.pass_context
def stage_status(ctx, job_id):
    """Check stage status"""
    result = DataAPI(session=ctx.obj['session']).stage_status(job_id)
    format_output(result, ctx.obj['json'])

@data.command()
@click.option('--path', required=True)
@click.option('--ingest_service_id', required=True)
@click.option('--namespace', required=True)
@click.option('--extra_metadata', default='{}')
@click.option('--metadata_suffix', default='.meta')
@click.option('--debug', is_flag=True)
@click.pass_context
def upload_ingest(ctx, path, ingest_service_id, namespace, extra_metadata, metadata_suffix, debug):
    """Upload data for ingest"""
    result = DataAPI(session=ctx.obj['session']).upload_for_ingest(
        path, ingest_service_id, namespace, metadata_suffix, extra_metadata, debug
    )
    format_output(result, ctx.obj['json'])

@data.command(name='namespace_ls')
@click.pass_context
def namespace_ls(ctx):
    """List namespaces"""
    result = DataAPI(session=ctx.obj['session']).list_namespaces()
    format_output(result, ctx.obj['json'])