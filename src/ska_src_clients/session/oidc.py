import datetime
import glob
import json
import os
import random
import time
from functools import wraps

import jwt
from prettytable import PrettyTable

from ska_src_authn_api.client.authentication import AuthenticationClient
from ska_src_clients.common.exceptions import handle_client_exceptions


def check_authentication_api_aliveness(func):
    """ Decorator to check authentication API is alive. """
    @wraps(func)
    def wrapper(*args, **kwargs):
        instance = args[0]
        instance.authentication_client.ping().raise_for_status()
        return func(*args, **kwargs)
    return wrapper


def remove_expired_tokens(func):
    """ Decorator to remove expired tokens. """
    @wraps(func)
    def wrapper(*args, **kwargs):
        instance = args[0]
        access_tokens = dict(instance.access_tokens)
        for aud, attributes in access_tokens.items():
            if attributes.get('expires_at') < time.time():
                instance.access_tokens.pop(aud)
                if attributes.get('path_on_disk'):
                    os.remove(attributes.get('path_on_disk'))
        return func(*args, **kwargs)
    return wrapper


class OIDCSession:
    def __init__(self, authn_api_url):
        self.stored_token_directory = "/tmp/srcnet/user"
        self.access_tokens = {}
        self.authentication_client = AuthenticationClient(authn_api_url)

    @handle_client_exceptions
    def _add_token_to_internal_cache(self, token, path_on_disk=None):
        access_token = token.get('access_token')
        access_token_decoded = jwt.decode(access_token, options={"verify_signature": False})
        if 'aud' in access_token_decoded:
            self.access_tokens[access_token_decoded.get('aud')] = {
                'access_token': access_token,
                'expires_at': token.get('expires_at'),
                'path_on_disk': path_on_disk if path_on_disk else 'INTERNAL'
            }

    @handle_client_exceptions
    def _save_token_to_disk(self, token):
        os.makedirs(self.stored_token_directory, exist_ok=True)

        access_token = token.get('access_token')
        access_token_decoded = jwt.decode(access_token, options={"verify_signature": False})
        if 'aud' in access_token_decoded:
            with open(os.path.join(self.stored_token_directory, "{}.token".format(
                    access_token_decoded.get('aud'))), 'w') as f:
                f.write(json.dumps(token))

    @handle_client_exceptions
    @remove_expired_tokens
    def load_tokens_from_disk(self):
        for entry in glob.glob(os.path.join(self.stored_token_directory, "*.token")):
            try:
                with open(entry, 'r') as f:
                    self._add_token_to_internal_cache(json.loads(f.read()), path_on_disk=f.name)
            except json.decoder.JSONDecodeError:
                os.remove(entry)

    @handle_client_exceptions
    @check_authentication_api_aliveness
    @remove_expired_tokens
    def get_login_url(self):
        login_response = self.authentication_client.login()
        login_response.raise_for_status()

        return login_response.json().get('authorization_uri')

    @handle_client_exceptions
    @check_authentication_api_aliveness
    @remove_expired_tokens
    def exchange_token(self, service, version, store_to_disk=True):
        if self.access_tokens.get(service):
            print("A valid access token for service {} already exists.".format(service))
            return
        # select any valid token randomly
        if self.access_tokens:
            token_to_exchange = random.choice(list(self.access_tokens.values())).get('access_token')

        token_exchange_response = self.authentication_client.exchange_token(service=service, token=token_to_exchange)
        token_exchange_response.raise_for_status()
        token = token_exchange_response.json()

        self._add_token_to_internal_cache(token)
        if store_to_disk:
            self._save_token_to_disk(token)


    @handle_client_exceptions
    @remove_expired_tokens
    def get_token(self, service):
        return self.access_tokens.get(service, {}).get('access_token')


    @handle_client_exceptions
    @remove_expired_tokens
    def list_tokens(self, truncate_access_token_chars=50):
        table = PrettyTable()
        table.align = 'l'
        table.field_names = ["Service", "Token", "Expires at (UTC)", "Expires at (Local)", "Path on disk"]
        for aud, attributes in self.access_tokens.items():
            access_token = attributes.get('access_token')
            expires_at = attributes.get('expires_at')
            path_on_disk = attributes.get('path_on_disk')
            table.add_row([
                aud,
                access_token if not truncate_access_token_chars else "{}...".format(
                     access_token[0:truncate_access_token_chars]),
                datetime.datetime.fromtimestamp(expires_at, datetime.timezone.utc),
                datetime.datetime.fromtimestamp(expires_at),
                path_on_disk])
        print(table)

    @handle_client_exceptions
    @check_authentication_api_aliveness
    @remove_expired_tokens
    def request_token(self, code, state, store_to_disk=True):
        token_response = self.authentication_client.token(code=code, state=state)
        token_response.raise_for_status()
        token = token_response.json().get('token')

        self._add_token_to_internal_cache(token)
        if store_to_disk:
            self._save_token_to_disk(token)


