import click

from ska_src_clients.api import DataAPI, SiteAPI
from ska_src_clients.common.utility import format_output, url_to_parts

@click.group()
def data():
    """Data operations."""

@data.command(help="Download data by namespace and name.")
@click.option('--namespace', required=True, help='DID namespace.')
@click.option('--name', required=True, help='DID name.')
@click.option('--sort', default='nearest_by_ip', help='Sorting algorithm (random||nearest_by_ip).')
@click.option('--ip-address', default='', help='IP address (nearest_by_ip only).')
@click.option('--no-verify', is_flag=True, help='Use insecure connnection if necessary.')
@click.option('--output', help='Output filename.')
@click.pass_context
def download(ctx, namespace, name, sort, ip_address, no_verify, output):
    result = DataAPI(session=ctx.obj['session']).download(namespace, name, sort, ip_address, not no_verify, output)
    format_output(result, ctx.obj['json'])

@data.command(help='Locate data by a namespace and name.')
@click.option('--namespace', required=True, help='DID namespace.')
@click.option('--name', required=True, help='DID name.')
@click.option('--sort', default='nearest_by_ip', help='Sorting algorithm (random||nearest_by_ip).')
@click.option('--ip-address', default='', help='IP address (nearest_by_ip only).')
@click.pass_context
def locate(ctx, namespace, name, sort, ip_address):
    result = DataAPI(session=ctx.obj['session']).locate(namespace, name, sort, ip_address)
    format_output(result, ctx.obj['json'])

@data.command(help="List files in a namespace.")
@click.option('--namespace', required=True, help='DID namespace.')
@click.option('--name', required=True, help='DID name.')
@click.option('--detail', is_flag=True, help='Detailed view?')
@click.option('--filters', default=None, help='Filter expression (Rucio only).')
@click.option('--limit', default=100, help='Maximum number of results to return.')
@click.pass_context
def ls(ctx, namespace, name, detail, filters, limit):
    result = DataAPI(session=ctx.obj['session']).list_files_in_namespace(namespace, name, detail, filters, limit)
    format_output(result, ctx.obj['json'])

@data.command(help="Upload data for ingest.")
@click.option('--path', required=True, help='Path to data directory to be uploaded.')
@click.option('--ingest-service-id', required=True, help='The ingest service ID.')
@click.option('--namespace', required=True, help='The namespace to upload into.')
@click.option('--extra-metadata', default='{}', help='Extra metadata to include.')
@click.option('--metadata-suffix', default='.meta', help='Suffix of metadata files.')
@click.option('--debug', is_flag=True, help='Debug mode?')
@click.pass_context
def upload_ingest(ctx, path, ingest_service_id, namespace, extra_metadata, metadata_suffix, debug):
    result = DataAPI(session=ctx.obj['session']).upload_for_ingest(
        path, ingest_service_id, namespace, metadata_suffix, extra_metadata, debug
    )
    format_output(result, ctx.obj['json'])

####################
# SUBCOMMAND: move #
####################

@data.group(help="Operations related to data movement.")
def move():
    """Data movement operations"""
    pass

@move.command(help="Make a data movement request.")
@click.option('--to-storage-area-id', required=True,
              help='The storage area ID to move data into')
@click.option('--dids', multiple=True, required=True, help='The DIDs to move.')
@click.option('--lifetime', required=True, help='Lifetime of rule (s)')
@click.option('--parent-namespace', help='The parent namespace.')
@click.pass_context
def request(ctx, to_storage_area_id, dids, lifetime, parent_namespace):
    result = DataAPI(session=ctx.obj['session']).move_request(to_storage_area_id, dids, lifetime, parent_namespace)
    if ctx.obj['json']:
        format_output(result, json_output=True)
    else:
        if result:
            click.secho("✔ Job submitted successfully ({})".format(result.get("job_id")), fg='green')
        else:
            click.secho("✖ Job submission failed", fg='red')

@move.command(help='Get the status of a data movement request.')
@click.option('--job-id', required=True, help="The movement job ID.")
@click.pass_context
def status(ctx, job_id):
    result = DataAPI(session=ctx.obj['session']).move_status(job_id)
    format_output(result, ctx.obj['json'])

#########################
# SUBCOMMAND: namespace #
#########################

@data.group(help="Operations related to data namespaces.")
def namespace():
    """Data namespace operations"""
    pass

@namespace.command(help="List namespaces.")
@click.pass_context
def ls(ctx):
    result = DataAPI(session=ctx.obj['session']).list_namespaces()
    format_output(result, ctx.obj['json'])

#####################
# SUBCOMMAND: stage #
#####################

@data.group(help="Operations related to data staging.")
def stage():
    """Data staging operations"""
    pass

@stage.command(help="Make a data staging request.")
@click.option('--to-storage-area-id', required=True,
              help='The storage area ID to stage data at')
@click.option('--dids', multiple=True, required=True, help='The DIDs to move.')
@click.option('--lifetime', required=True, help='Lifetime of rule (s)')
@click.option('--parent-namespace', help='The parent namespace.')
@click.pass_context
def request(ctx, to_storage_area_id, dids, lifetime, parent_namespace):
    result = DataAPI(session=ctx.obj['session']).stage_request(to_storage_area_id, dids, lifetime, parent_namespace)
    if ctx.obj['json']:
        format_output(result, json_output=True)
    else:
        if result:
            click.secho("✔ Job submitted successfully ({})".format(result.get("job_id")), fg='green')
        else:
            click.secho("✖ Job submission failed", fg='red')

@stage.command(help="Get the status of data staging request.")
@click.option('--job-id', required=True, help='The staging job ID.')
@click.pass_context
def status(ctx, job_id):
    result = DataAPI(session=ctx.obj['session']).stage_status(job_id)
    format_output(result, ctx.obj['json'])
