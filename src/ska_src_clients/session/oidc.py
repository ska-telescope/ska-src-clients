import datetime
import glob
import json
import os
import pprint
import random
from functools import wraps

import jwt
from prettytable import PrettyTable

from ska_src_clients.common.exceptions import handle_client_exceptions
from ska_src_clients.common.utility import remove_expired_tokens
from ska_src_clients.session.session import Session


def check_authentication_api_aliveness(func):
    """ Decorator to check authentication API is alive. """
    @wraps(func)
    def wrapper(*args, **kwargs):
        instance = args[0]
        instance.get_client_by_service_name('authn-api').ping()
        return func(*args, **kwargs)
    return wrapper


class OIDCSession(Session):
    def __init__(self, config):
        super().__init__(config)
        self.stored_token_directory = "/tmp/srcnet/user"
        self.access_tokens = {}

    def _add_token_to_internal_cache(self, token, path_on_disk=None):
        access_token = token.get('access_token')
        access_token_decoded = jwt.decode(access_token, options={"verify_signature": False})
        if 'aud' in access_token_decoded:
            self.access_tokens[access_token_decoded.get('aud')] = {
                'access_token': access_token,
                'expires_at': token.get('expires_at'),
                'path_on_disk': path_on_disk if path_on_disk else 'INTERNAL'
            }

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
        login_response = self.get_client_by_service_name('authn-api').login()
        return login_response.json().get('authorization_uri')

    @handle_client_exceptions
    @check_authentication_api_aliveness
    @remove_expired_tokens
    def exchange_token(self, service, version, store_to_disk=True):
        if self.access_tokens.get(service):
            print("A valid access token for service {} already exists.".format(service))
            return
        # select any valid token randomly
        if not self.access_tokens:
            print("No access tokens exist to exchange. Login first.")
            return

        token_to_exchange = random.choice(list(self.access_tokens.values())).get('access_token')

        token_exchange_response = self.get_client_by_service_name('authn-api').exchange_token(
            service=service, token=token_to_exchange)
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
    def inspect_token(self, service):
        access_token_for_service = self.access_tokens.get(service, {}).get('access_token')
        if not access_token_for_service:
            print("No access token exists for service {}".format(service))
            return

        access_token_decoded = jwt.decode(access_token_for_service, options={"verify_signature": False})
        pprint.pprint(access_token_decoded)

    @handle_client_exceptions
    @check_authentication_api_aliveness
    @remove_expired_tokens
    def request_token(self, code, state, store_to_disk=True):
        token_response = self.get_client_by_service_name('authn-api').token(code=code, state=state)
        token = token_response.json().get('token')

        self._add_token_to_internal_cache(token)
        if store_to_disk:
            self._save_token_to_disk(token)


