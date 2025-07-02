import click
import json
import logging
import os
import sys
import webbrowser

from django.core.management import execute_from_command_line

from ska_src_clients.cli.subcommands import api, config, data, metadata, node, token, site
from ska_src_clients.common.utility import load_config, parts_to_url
from ska_src_clients.session.oidc import OIDCSession


@click.group(help="SRCNet Operator CLI")
@click.option('-c', 'config_path', help='Override default paths to configuration file.')
@click.option('--debug', is_flag=True, help='Enable debug mode.')
@click.option('--json', is_flag=True, help='Output as JSON.')
@click.pass_context
def cli(ctx, config_path, debug, json):
    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO,
                        format="%(asctime)s [%(name)s] %(module)10s %(levelname)5s %(process)d\t%(message)s")
    if config_path:
        config = load_config([config_path])
    else:
        config = load_config()
    if not config:
        logging.critical("No valid config file found.")
        exit(1)
    session = OIDCSession(config=config)
    session.load_tokens_from_disk()
    ctx.obj = {
        'config': config,
        'session': session,
        'json': json
    }

@cli.command(help="Start the oper GUI server.")
@click.option('--host', default="0.0.0.0", help='Host to run the GUI server on')
@click.option('--port', default=8000, help='Port to run the GUI server on')
@click.pass_context
def gui(ctx, host, port):
    """Start the GUI server."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'gui.settings')
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'gui'))
    execute_from_command_line([
        'manage.py', 'runserver', "{}:{}".format(host, port)])

cli.add_command(gui)
cli.add_command(api)
cli.add_command(config)
cli.add_command(data)
cli.add_command(metadata)
cli.add_command(node)
cli.add_command(site)
cli.add_command(token)

if __name__ == '__main__':
    cli()
