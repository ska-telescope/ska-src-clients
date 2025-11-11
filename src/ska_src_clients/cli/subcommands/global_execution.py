""" subcommand for global execution compute operations. """

import click

from ska_src_clients.api.global_execution import GlobalExecutionAPI
from ska_src_clients.common.utility import format_output


@click.group(help="Compute Jobs on Global Executions API.")
def global_execution():
    """Compute Jobs on Global Executions API."""


def create_job_parameters(param:dict) -> dict:
    """Create job parameters from the provided dictionary."""
    job_params = {}
    for key, value in param.items():
        if value is not None:
            job_params[key] = value
    return job_params



@global_execution.command(help="Submit a job to the Global Execution API.")
@click.option('--container_image', required=True, help='On which container image to run the job.')
@click.option('--script', required=True, help='Script to run in the container.')
@click.option('--job_count', default=None, help='Number of jobs to run (default: 1).')
@click.option('--vo', default=None, help='Virtual Organization (VO) to use (default: wlcg).')
@click.option('--queue_name', default=None, help='Queue name to submit the job to.')
@click.option('--prod_source_label', default=None, help='Production source label for the job.')
@click.option('--working_group', default=None, help='Working group to associate with the job.')
@click.option('--no_build', is_flag=False, help='Do not build the container image before running the job.')
@click.option('--no_separate_log', is_flag=False, help='Do not create a separate log file for the job.')
@click.pass_context
def submit(ctx,
           container_image,
           script,
           job_count,
           vo,
           queue_name,
           prod_source_label,
           working_group,
           no_build,
           no_separate_log):
    parameters = create_job_parameters({
        "job_count": job_count,
        "vo": vo,
        "queue_name": queue_name,
        "prod_source_label": prod_source_label,
        "working_group": working_group,
        "no_build": no_build,
        "no_separate_log": no_separate_log
    })
    result = GlobalExecutionAPI(session=ctx.obj['session']).submit_job(container_image=container_image, script=script,
                                                                       parameters=parameters)
    format_output(result, json_output=True)



@global_execution.command(help="Get the status of a job from the Global Execution API.")
@click.argument('job_id')
@click.pass_context
def status(ctx, job_id):
    """Get the status of a job from the Global Execution API."""
    result = GlobalExecutionAPI(session=ctx.obj['session']).get_job_status(job_id=job_id)
    format_output(result, json_output=True)
