import click
import logging
import os
import yaml
import json
from pathlib import Path

from ska_src_clients.cli.operations.api import api
from ska_src_clients.cli.operations.config import config
from ska_src_clients.cli.operations.data import data
from ska_src_clients.cli.operations.metadata import metadata
from ska_src_clients.cli.operations.site import site
from ska_src_clients.cli.operations.token import token
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

@click.group(help=""
                  "SRCNet Operator CLI"
                  ""
                  "Use --help on any subcommand to see options."
             )
@click.option('-c', 'config_paths', multiple=True, default=[
    'etc/cfg/base.yml',
    os.path.join(Path.home(), '.local', 'etc', 'cfg', 'base.yml'),
    '/usr/local/etc/base.yml'
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

cli.add_command(api)
cli.add_command(config)
cli.add_command(data)
cli.add_command(metadata)
cli.add_command(site)
cli.add_command(token)

if __name__ == '__main__':
    cli()
