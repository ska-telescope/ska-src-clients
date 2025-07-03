import click
import datetime

from ska_src_clients.common.utility import format_output
from ska_src_clients.session.oidc import OIDCSession

@click.group(help="Token operations.")
def token():
    """Token operations."""

@token.command(help="Exchange a token for use at another API.")
@click.argument('target_api_name')
@click.pass_context
def exchange(ctx, target_api_name):
    session: OIDCSession = ctx.obj['session']
    result = session.exchange_token(target_api_name)

    if ctx.obj['json']:
        format_output({"exchanged": result}, json_output=True)
    else:
        if result:
            click.secho(f"✔ Token exchanged successfully for service '{target_api_name}'", fg='green')
        else:
            click.secho(f"✖ Token exchange failed for service '{target_api_name}'", fg='red')

@token.command(help="Get an existing token.")
@click.argument('api_name')
@click.pass_context
def get(ctx, api_name):
    session: OIDCSession = ctx.obj['session']
    token = session.get_access_token(api_name)
    format_output(token, ctx.obj['json'])

@token.command(help="List available access tokens.")
@click.pass_context
def ls(ctx):
    session: OIDCSession = ctx.obj['session']
    result = session.list_access_tokens()

    reformatted = []
    for service, data in result.items():
        expires_at_epoch = data.get("expires_at")
        if expires_at_epoch:
            expires_dt_utc = datetime.datetime.utcfromtimestamp(expires_at_epoch).strftime('%Y-%m-%d %H:%M:%S UTC')
            expires_dt_local = datetime.datetime.fromtimestamp(expires_at_epoch).strftime('%Y-%m-%d %H:%M:%S')
        else:
            expires_dt_utc = "-"
            expires_dt_local = "-"

        reformatted.append({
            "service_name": service,
            "access_token": data.get("access_token", "")[:20] + "...",  # truncate
            "expires_utc": expires_dt_utc,
            "expires_local": expires_dt_local,
            "path_on_disk": data.get("path_on_disk"),
            "has_refresh_token": "✓" if data.get("has_associated_refresh_token") else "✗"
        })

    format_output(
        reformatted if not ctx.obj["json"] else result,
        table_field_names=["API Name", "Access Token", "Expires at (UTC)",
                           "Expires at (Local)", "Path on Disk", "Has Associated Refresh Token?"],
        json_output = ctx.obj["json"]
    )

@token.command(help="Inspect an existing access token.")
@click.argument('api_name')
@click.pass_context
def inspect(ctx, api_name):
    session: OIDCSession = ctx.obj['session']
    result = session.inspect_access_token(api_name)

    reformatted = []
    for key, value in result.items():
        if key in {'nbf', 'exp', 'iat'} and isinstance(value, (int, float)):
            value = datetime.datetime.utcfromtimestamp(value).strftime('%Y-%m-%d %H:%M:%S UTC')
        if isinstance(value, list):
            value = "\n".join(str(v) for v in value)
        reformatted.append({"key": key, "value": value})

    format_output(
        reformatted if not ctx.obj['json'] else result,
        json_output=ctx.obj['json']
    )

@token.command(help="Request a new access token using the device flow.")
@click.pass_context
def request(ctx):
    session: OIDCSession = ctx.obj['session']
    new_token = session.start_device_flow()
