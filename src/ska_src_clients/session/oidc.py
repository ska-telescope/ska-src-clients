import glob
import json
import logging
import os
import random
import sys
import textwrap
import time
from functools import wraps
from uuid import uuid4

import jwt
import qrcode

from ska_src_clients.common.exceptions import handle_client_exceptions
from ska_src_clients.common.utility import remove_expired_tokens
from ska_src_clients.session.session import Session


def check_authentication_api_aliveness(func):
    """Decorator to check authentication API is alive."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        instance = args[0]
        instance.client_factory.get_authn_client().ping()
        return func(*args, **kwargs)

    return wrapper


class OIDCSession(Session):
    def __init__(self, config):
        super().__init__(config)
        self.stored_token_directory = "/tmp/srcnet/user"
        self.access_tokens = {}
        self.refresh_tokens = []

    def _add_tokens_to_internal_cache(self, token, path_on_disk=None):
        """Add tokens to the internal application cache.

        :param str token: The token to add.
        :param str path_on_disk: The path to the token on disk.
        """
        logging.debug("Adding tokens to internal cache.")
        access_token = token.get("access_token")
        access_token_decoded = jwt.decode(access_token, options={"verify_signature": False})
        if "aud" in access_token_decoded:
            self.access_tokens[access_token_decoded.get("aud")] = {
                "token": access_token,
                "expires_at": access_token_decoded.get("exp"),
                "path_on_disk": path_on_disk if path_on_disk else "INTERNAL",
            }

        # Keep the association between refresh and access
        # tokens as when refreshing the associated access token is
        # (by default in the client) invalidated.
        #
        refresh_token = token.get("refresh_token")
        if refresh_token:
            refresh_token_decoded = jwt.decode(refresh_token, options={"verify_signature": False})
            self.refresh_tokens.append(
                {
                    "token": refresh_token,
                    "associated_access_token": access_token,
                    "expires_at": refresh_token_decoded.get("exp"),
                    "path_on_disk": path_on_disk if path_on_disk else "INTERNAL",
                }
            )

    def _save_tokens_to_disk(self, token):
        """Save a token to disk.

        :param str token: The token to save.
        :return: Returns the path to the token on disk.
        :rtype: str
        """
        logging.debug("Saving tokens to disk.")
        os.makedirs(self.stored_token_directory, exist_ok=True)

        token_path_on_disk = os.path.join(self.stored_token_directory, "{}.token".format(str(uuid4())))
        with open(token_path_on_disk, "w") as f:
            f.write(json.dumps(token))

        return token_path_on_disk

    @handle_client_exceptions
    def load_tokens_from_disk(self):
        """Load OIDC tokens from disk into the internal cache."""
        logging.debug("Loading tokens from disk.")
        for entry in glob.glob(os.path.join(self.stored_token_directory, "*.token")):
            try:
                with open(entry, "r") as f:
                    self._add_tokens_to_internal_cache(json.loads(f.read()), path_on_disk=f.name)
            except json.decoder.JSONDecodeError:
                os.remove(entry)

    @handle_client_exceptions
    @check_authentication_api_aliveness
    @remove_expired_tokens
    def get_device_authorization_response(self):
        """Start a device code flow.

        :return: A device flow authorization response.
        :rtype: str
        """
        login_response = self.client_factory.get_authn_client().login(flow="device")
        return login_response.json()

    @handle_client_exceptions
    @check_authentication_api_aliveness
    @remove_expired_tokens
    def exchange_token(
        self,
        service_name,
        version="latest",
        store_to_disk=True,
        by_refresh=True,
    ):
        """Exchange an access token for a service.

        This can be done either by using a refresh_token
        grant or by directly exchanging the access token with a
        token exchange grant.

        Note that to use the authentication's exchange endpoint
        an active access token must also be present. This is
        required for user group permissions checks.
        If the by_refresh flag is set and no active access token is
        available in the environment a new one will be requested.

        :param str service_name: The service name to exchange a token for.
        :param str version: The version of the service to exchange
        a token for.
        :param bool store_to_disk: Store the tokens to disk
        for persistence.
        :param bool by_refresh: Exchange tokens using the
        refresh_token grant.
        :return: Success flag
        :rtype: bool
        """
        logging.debug("Exchanging token for service {}".format(service_name))
        token = None
        if by_refresh:
            logging.debug(" - Attempting exchange using refresh grant")
            if self.refresh_tokens:
                logging.debug(" - Refresh token found")
                # First check if we have BOTH a valid refresh and access token combination, if so, this exchange can be
                # resolved with a call to the token exchange endpoint with these two tokens.
                #
                found_matching_access_token = False
                for refresh_token_idx, refresh_token in enumerate(self.refresh_tokens):
                    for aud, access_token in self.access_tokens.items():
                        if access_token.get("token") == refresh_token.get("associated_access_token"):
                            logging.debug(" - Found a valid matching access token, " + "proceeding with exchange")
                            token_exchange_response = self.client_factory.get_authn_client().exchange_token(
                                service=service_name,
                                version=version,
                                refresh_token=refresh_token.get("token"),
                                access_token=access_token.get("token"),
                            )
                            token_exchange_response.raise_for_status()
                            token = token_exchange_response.json()
                            # need to remove previous (now invalid) access & refresh token from caches
                            self.refresh_tokens.pop(refresh_token_idx)
                            aud_to_pop = []
                            for aud, attributes in self.access_tokens.items():
                                if attributes.get("token") == refresh_token.get("associated_access_token"):
                                    aud_to_pop.append(aud)
                            for aud in aud_to_pop:
                                self.access_tokens.pop(aud)

                            # and also on disk
                            os.remove(refresh_token.get("path_on_disk"))

                            found_matching_access_token = True
                            break
                    if found_matching_access_token:
                        break

                # If we didn't find a valid access token then only a refresh token must exist. As such we will need to
                # refresh the access token first using this.
                #
                if not found_matching_access_token:
                    logging.debug(" - Unable to find a valid matching access token, proceeding with refresh")
                    for refresh_token_idx, refresh_token in enumerate(self.refresh_tokens):
                        try:
                            token_refresh_response = self.client_factory.get_authn_client().refresh_token(refresh_token=refresh_token.get("token"))
                            token_refresh_response.raise_for_status()
                        except Exception as e:
                            logging.exception(e)
                            continue
                        refreshed_token = token_refresh_response.json()

                        # need to remove previous (now invalid) refresh token from caches
                        self.refresh_tokens.pop(refresh_token_idx)

                        # and on disk
                        os.remove(refresh_token.get("path_on_disk"))

                        # Finally, exchange this refreshed token.
                        logging.debug(" - Exchanging refresh token")
                        token_exchange_response = self.client_factory.get_authn_client().exchange_token(
                            service=service_name,
                            version=version,
                            refresh_token=refreshed_token.get("refresh_token"),
                            access_token=refreshed_token.get("access_token"),
                        )
                        token_exchange_response.raise_for_status()
                        token = token_exchange_response.json()
                        break
            else:
                logging.critical("Exchange requested by refresh but no valid refresh tokens exist.")
        else:
            logging.debug(" - Attempting direct access token exchange")
            if not self.access_tokens:
                logging.critical("Exchange requested but no valid access tokens exist.")
            else:
                # select any valid token randomly
                random_access_token = random.choice(list(self.access_tokens.values()))
                access_token_to_exchange = random_access_token.get("token")
                token_exchange_response = self.client_factory.get_authn_client().exchange_token(
                    service=service_name,
                    access_token=access_token_to_exchange,
                )
                token_exchange_response.raise_for_status()
                token = token_exchange_response.json()

        if token:
            token_path_on_disk = None
            if store_to_disk:
                token_path_on_disk = self._save_tokens_to_disk(token)
            self._add_tokens_to_internal_cache(token, path_on_disk=token_path_on_disk)
            self.get_access_token(service_name)
            return True
        return False

    @handle_client_exceptions
    @remove_expired_tokens
    def get_access_token(self, service_name):
        """Get an access token for a service from the environment.

        :param str service_name: The service name to get a token for.
        :return: An access token.
        :rtype: str
        """
        return self.access_tokens.get(service_name, {}).get("token")

    @handle_client_exceptions
    @remove_expired_tokens
    def list_access_tokens(self, truncate_access_token_chars=50):
        """List available access tokens in the environment.

        :param int truncate_access_token_chars: Truncate the access token to a set number of characters.
        :return: A tabulated list of access tokens.
        :rtype: str
        """
        tokens = {}
        for aud, attributes in self.access_tokens.items():
            access_token = attributes.get("token")
            _ = attributes.get("expires_at")
            _ = attributes.get("path_on_disk")
            has_associated_refresh_token = False
            for token in self.refresh_tokens:
                if access_token == token.get("associated_access_token"):
                    has_associated_refresh_token = True

            tokens[aud] = {
                "access_token": attributes.get("token"),
                "expires_at": attributes.get("expires_at"),
                "path_on_disk": attributes.get("path_on_disk"),
                "has_associated_refresh_token": has_associated_refresh_token,
            }
        return tokens

    @handle_client_exceptions
    @check_authentication_api_aliveness
    @remove_expired_tokens
    def inspect_access_token(self, service_name):
        """Introspect an access token.

        :param str service_name: The service name to exchange a token for.
        :return: An instrospected access token.
        :rtype: str
        """
        access_token_for_service = self.access_tokens.get(service_name, {})
        if not access_token_for_service:
            logging.critical("No access token exists for service {}".format(service_name))
            return
        access_token_decoded = jwt.decode(
            access_token_for_service.get("token"),
            options={"verify_signature": False},
        )
        return access_token_decoded

    @handle_client_exceptions
    @check_authentication_api_aliveness
    @remove_expired_tokens
    def request_token(self, device_code, store_to_disk=True):
        """Complete a device flow.

        :param str device_code: The device code.
        :param bool store_to_disk: Store the token to disk for persistence.
        :return: Either the error code as a string or True.
        :rtype: Union[bool, dict]
        """
        token_response = self.client_factory.get_authn_client().token(device_code=device_code).json()
        token = token_response.get("token")
        if token:
            token_path_on_disk = None
            if store_to_disk:
                token_path_on_disk = self._save_tokens_to_disk(token)
            self._add_tokens_to_internal_cache(token, path_on_disk=token_path_on_disk)
            return True
        return token_response.get("error")

    @handle_client_exceptions
    def start_device_flow(self, max_polling_attempts=60, wait_between_polling_s=5):
        device_authorization_response = self.get_device_authorization_response()

        # make an ascii qr code for the complete verification uri
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=10,
            border=4,
        )
        qr.add_data(device_authorization_response.get("verification_uri_complete"))

        # add instructional text for user if they don't want to use qr code
        user_instruction_text = (
            "Scan the QR code, or using a browser on another device, visit "
            + "{verification_uri} and enter code {user_code}".format(
                verification_uri=device_authorization_response.get("verification_uri"),
                user_code=device_authorization_response.get("user_code"),
            )
        )

        wrapped_string = textwrap.fill(user_instruction_text, width=50)

        print()
        print("-" * 50)
        print()
        print(wrapped_string)
        qr.print_ascii()
        print("-" * 50)
        print()

        # poll for user to complete authorisation process
        success = False
        max_attempts = max_polling_attempts
        for attempt in range(0, max_attempts):
            try:
                # the following will raise before the break if the authorization is still pending
                self.request_token(device_code=device_authorization_response.get("device_code"))
                success = True
                break
            except Exception as e:
                logging.exception(e)
                _, ex_value, _ = sys.exc_info()
                logging.debug(ex_value)
            print(
                "Polling for token... ({attempt}/{max_attempts})".format(attempt=attempt + 1, max_attempts=max_attempts),
                end="\r",
            )
            time.sleep(wait_between_polling_s)
        print()
        print()
        if success:
            print("Successfully polled for token. You are now logged in.")
        else:
            print("Failed to poll for token. Please try again.")
        print()
