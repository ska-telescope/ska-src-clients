import glob
import json
import logging
import os
import requests
import time
from functools import wraps
from urllib.parse import urlparse, urlunparse

import jwt

from ska_src_clients.common.exceptions import NoAccessTokenFoundForService


def get_authenticated_requests_session(session, service_name):
    """ Get a requests session with one that has a populated access token. """
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
            # remove access tokens in memory
            valid_access_tokens = {}
            for aud, attributes in dict(session.access_tokens).items():
                if attributes.get('expires_at') >= time.time():
                    valid_access_tokens[aud] = attributes
                else:
                    logging.debug("Removing access token with audience {}".format(aud))
            session.access_tokens = valid_access_tokens

            # remove refresh tokens in memory
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

