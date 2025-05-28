import click
import datetime

from ska_src_clients.common.utility import format_output
from ska_src_clients.session.oidc import OIDCSession

@click.group()
def token():
    """Token operations"""

@token.command()
@click.argument('target_service')
@click.pass_context
def exchange(ctx, target_service):
    """Exchange the current token for a different service"""
    session: OIDCSession = ctx.obj['session']
    result = session.exchange_token(target_service)

    if ctx.obj['json']:
        format_output({"status": "exchanged", "new_token": result}, json_output=True)
    else:
        if result:
            click.secho(f"✔ Token exchanged successfully for service '{target_service}'", fg='green')
        else:
            click.secho(f"✖ Token exchange failed for service '{target_service}'", fg='red')

@token.command()
@click.argument('service')
@click.pass_context
def get(ctx, service):
    """Get contents of an existing access token"""
    session: OIDCSession = ctx.obj['session']
    token = session.get_access_token(service)
    format_output(token, ctx.obj['json'])

@token.command(name='ls')
@click.pass_context
def ls(ctx):
    """List existing access tokens"""
    session: OIDCSession = ctx.obj['session']
    tokens = session.list_access_tokens()

    if ctx.obj['json']:
        format_output(tokens, json_output=True)
    else:
        rows = []
        for service, data in tokens.items():
            expires_at_epoch = data.get("expires_at")
            if expires_at_epoch:
                expires_dt_utc = datetime.datetime.utcfromtimestamp(expires_at_epoch).strftime('%Y-%m-%d %H:%M:%S UTC')
                expires_dt_local = datetime.datetime.fromtimestamp(expires_at_epoch).strftime('%Y-%m-%d %H:%M:%S')
            else:
                expires_dt_utc = "-"
                expires_dt_local = "-"

            rows.append({
                "service_name": service,
                "access_token": data.get("access_token", "")[:20] + "...",  # truncate
                "expires_utc": expires_dt_utc,
                "expires_local": expires_dt_local,
                "path_on_disk": data.get("path_on_disk"),
                "has_refresh_token": "✓" if data.get("has_associated_refresh_token") else "✗"
            })

        table_fields = [
            "service_name", "access_token", "expires_utc", "expires_local",
            "path_on_disk", "has_refresh_token"
        ]
        headers_map = {
            "service_name": "Service Name",
            "access_token": "Access token",
            "expires_utc": "Expires at (UTC)",
            "expires_local": "Expires at (Local)",
            "path_on_disk": "Path on disk",
            "has_refresh_token": "Has associated refresh token?"
        }

        format_output(
            rows,
            json_output=False,
            table_fields=table_fields,
            headers_map=headers_map,
            list_of_dicts=True
        )

@token.command()
@click.argument('service')
@click.pass_context
def inspect(ctx, service):
    """Inspect an existing access token"""
    session: OIDCSession = ctx.obj['session']
    details = session.inspect_access_token(service)

    if ctx.obj['json']:
        format_output(details, json_output=True)
    else:
        display_rows = []
        for key, value in details.items():
            # Convert epoch fields
            if key in {'nbf', 'exp', 'iat'} and isinstance(value, (int, float)):
                value = datetime.datetime.utcfromtimestamp(value).strftime('%Y-%m-%d %H:%M:%S UTC')

            # Format list fields (newline-separated)
            if isinstance(value, list):
                value = "\n".join(str(v) for v in value)

            display_rows.append({"key": key, "value": value})

        table_fields = ["key", "value"]
        headers_map = {"key": "Key", "value": "Value"}

        format_output(
            display_rows,
            json_output=False,
            table_fields=table_fields,
            headers_map=headers_map,
            list_of_dicts=True
        )

@token.command()
@click.pass_context
def request(ctx):
    """Request a new access token using the device flow"""
    session: OIDCSession = ctx.obj['session']
    new_token = session.start_device_flow()
