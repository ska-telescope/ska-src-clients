import glob
import json
import logging
import os
import requests
import time
from functools import wraps
from urllib.parse import urlparse, urlunparse

import jwt
import plotly.graph_objects as go

from ska_src_clients.common.exceptions import NoAccessTokenFoundForService


def get_authenticated_requests_session(session, service_name):
    """ Get a requests session with one that has a populated access token. """
    # do we have any tokens? if not, login
    if not session.access_tokens and not session.refresh_tokens:
        session.start_device_flow()

    access_token = session.get_access_token(service_name)
    if not access_token:
        # attempt to exchange if we can't find an access token for the service
        if not session.exchange_token(service_name):
            raise NoAccessTokenFoundForService(service_name)
        access_token = session.get_access_token(service_name)

    # make requests session and populate authorization
    requests_session = requests.Session()
    requests_session.headers.update({
        "Authorization": "Bearer {}".format(access_token)
    })
    return requests_session


def parts_to_url(prefix, host, port, path):
    """ Converts parts to a URL. """
    return urlunparse({
        'prefix': prefix,
        'host': host,
        'port': port,
        'path': path
    })


def plot_scatter_world_map(fig, data, latitude_key, longitude_key, value_key, label_key, size_offset=15):
    """ Make a scatterplot against a world map. """
    # calculate relative marker size
    normalized_sizes = [
        (entry[value_key]/max([entry[value_key] for entry in data])) * size_offset for entry in data]

    # construct the plot
    for index, entry in enumerate(data):
        fig.add_trace(go.Scattergeo(
            name="",
            lat=[entry[latitude_key]],
            lon=[entry[longitude_key]],
            text=entry[label_key],
            mode='markers',
            marker=dict(
                size=normalized_sizes[index],
                opacity=0.8,
                color='red',
                line=dict(width=0.3, color='black')
            ),
            showlegend=False
        ))
    fig.update_layout(
        geo=dict(
            scope='world',
            projection_type='equirectangular',
            showland=True,
            landcolor='rgb(230, 230, 230)',
            showcountries=True,
            countrycolor='rgb(160, 160, 160)',
            showocean=True,
            oceancolor='rgb(200, 230, 255)',
            showlakes=False
        ),
        hovermode='x unified',
        hoverlabel=dict(bgcolor='rgba(255,255,255,1)')
    )
    return fig


def remove_expired_tokens(func):
    """ Decorator to remove expired tokens. """
    @wraps(func)
    def wrapper(*args, **kwargs):
        if hasattr(args[0], 'session'):     # Check if self has a session instance
            session = args[0].session
        else:                               # Failing that, is self a session instance itself?
            session = args[0]

        from ska_src_clients.session.oidc import OIDCSession
        if isinstance(session, OIDCSession):
            logging.debug("Removing expired tokens.")
            # remove expired access tokens in memory
            valid_access_tokens = {}
            for aud, attributes in dict(session.access_tokens).items():
                if attributes.get('expires_at') >= time.time():
                    valid_access_tokens[aud] = attributes
                else:
                    logging.debug("Removing access token with audience {}".format(aud))
            session.access_tokens = valid_access_tokens

            # remove expired refresh tokens in memory
            valid_refresh_tokens = []
            refresh_tokens = list(session.refresh_tokens)
            for attributes in refresh_tokens:
                if attributes.get('expires_at') >= time.time():
                    valid_refresh_tokens.append(attributes)
            session.refresh_tokens = valid_refresh_tokens

            # on disk, we check if all tokens in a file have expired, only then are the files deleted
            for token_path in glob.glob(os.path.join(session.stored_token_directory, "*.token")):
                with open(token_path, 'r') as token_file:
                    token = json.loads(token_file.read())

                # check refresh token
                refresh_token_has_expired = False
                refresh_token = token.get('refresh_token')
                if refresh_token:
                    refresh_token_decoded = jwt.decode(refresh_token, options={"verify_signature": False})
                    if refresh_token_decoded.get('exp') < time.time():
                        refresh_token_has_expired = True

                # check access token
                access_token_has_expired = False
                access_token = token.get('access_token')
                if access_token:
                    access_token_decoded = jwt.decode(access_token, options={"verify_signature": False})
                    if access_token_decoded.get('exp') < time.time():
                        access_token_has_expired = True

                # both refresh and access tokens are expired so delete this token on disk
                if all([refresh_token_has_expired, access_token_has_expired]):
                    logging.debug("Removing token with path: {}".format(token_path))
                    os.remove(token_path)

        return func(*args, **kwargs)
    return wrapper


def url_to_parts(url):
    """ Converts a string URL into consituent parts. """
    parsed = urlparse(url)
    return {
        'prefix': parsed.scheme,
        'host': parsed.hostname,
        'port': parsed.port,
        'path': parsed.path
    }

