import json
import logging
import os
import sys

import click
from trogon import tui

from ska_src_clients.cli.subcommands.global_execution import global_execution
from ska_src_clients.cli.subcommands import api, config, data, metadata, node, token, site
from ska_src_clients.common.utility import load_config
from ska_src_clients.tui.oper import OperApp
from ska_src_clients.session.oidc import OIDCSession


@tui()
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

@click.command(help="Launch the TUI operator interface.")
@click.pass_context
def tui(ctx):
    OperApp().run()

cli.add_command(api)
cli.add_command(config)
cli.add_command(data)
cli.add_command(metadata)
cli.add_command(node)
cli.add_command(site)
cli.add_command(token)
cli.add_command(tui)
cli.add_command(global_execution)

if __name__ == '__main__':
    cli()
