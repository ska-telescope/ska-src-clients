import webbrowser

import click

from ska_src_clients.api import NodeAPI


@click.group(help="Operations related to nodes.")
def node():
    """Node operations."""
    pass

@node.command(help="Add a node.")
@click.pass_context
def add(ctx):
    url = NodeAPI(session=ctx.obj['session']).get_add_node_www_url()
    webbrowser.open_new_tab(url)

@node.command(help="Edit a node.")
@click.argument('node_name')
@click.pass_context
def edit(ctx, node_name):
    url = NodeAPI(session=ctx.obj['session']).get_edit_node_www_url(node_name)
    webbrowser.open_new_tab(url)