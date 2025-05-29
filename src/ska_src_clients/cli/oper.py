import click
import json
import logging
import os
import sys
import yaml

from pathlib import Path

from ska_src_clients.cli.operations import api, config, data, metadata, token, site
from ska_src_clients.session.oidc import OIDCSession


def load_config(config_paths):
    for file_path in config_paths:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                try:
                    config = yaml.safe_load(f)
                    logging.debug(f"Loaded config from {file_path}")
                    return config
                except yaml.YAMLError:
                    continue
    logging.critical("No valid config file found.")
    exit(1)

@click.group(help="SRCNet Operator CLI")
@click.option('-c', 'config_paths', multiple=True, default=[
    'etc/cfg/oper.yml',
    os.path.join(Path.home(), '.local/etc/cfg/user.yml'),
    '/usr/local/etc/oper.yml'
], help='Path to configuration file')
@click.option('--debug', is_flag=True, help='Enable debug mode')
@click.option('--json', is_flag=True, help='Output as JSON')
@click.pass_context
def cli(ctx, config_paths, debug, json):
    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO,
                        format="%(asctime)s [%(name)s] %(module)10s %(levelname)5s %(process)d\t%(message)s")
    config = load_config(config_paths)
    session = OIDCSession(config=config)
    session.load_tokens_from_disk()
    ctx.obj = {
        'config': config,
        'session': session,
        'json': json
    }

@cli.command(help="Start the oper GUI server")
@click.option('--port', default=8000, help='Port to run the GUI server on')
@click.pass_context
def gui(ctx, port):
    """Start the GUI server."""
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'gui.settings')
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'gui'))

    from django.core.management import execute_from_command_line
    execute_from_command_line(['manage.py', 'runserver', f'0.0.0.0:{port}'])

cli.add_command(gui)
cli.add_command(api)
cli.add_command(config)
cli.add_command(data)
cli.add_command(metadata)
cli.add_command(site)
cli.add_command(token)

if __name__ == '__main__':
    cli()
