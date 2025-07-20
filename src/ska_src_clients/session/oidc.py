import glob
import json
import logging
import os
import qrcode
import random
import sys
import textwrap
import time
from functools import wraps
from uuid import uuid4

import jwt

from ska_src_clients.common.exceptions import handle_client_exceptions
from ska_src_clients.common.utility import remove_expired_tokens
from ska_src_clients.session.session import Session


def check_authentication_api_aliveness(func):
    """ Decorator to check authentication API is alive. """
    @wraps(func)
    def wrapper(*args, **kwargs):
        instance = args[0]
        try:
            # Add timeout to the ping call to prevent hanging
            import signal
            
            def timeout_handler(signum, frame):
                raise TimeoutError("Authentication API ping timed out")
            
            # Set a 5 second timeout for the ping
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(5)
            
            try:
                instance.client_factory.get_authn_client().ping()
                signal.alarm(0)  # Cancel the alarm
            except TimeoutError:
                signal.alarm(0)  # Cancel the alarm
                logging.warning("Authentication API ping timed out - proceeding anyway")
            except Exception as e:
                signal.alarm(0)  # Cancel the alarm
                logging.warning(f"Authentication API ping failed: {e} - proceeding anyway")
            finally:
                # Restore the original signal handler
                signal.signal(signal.SIGALRM, old_handler)
                
        except Exception as e:
            logging.warning(f"Could not check authentication API aliveness: {e} - proceeding anyway")
        
        return func(*args, **kwargs)
    return wrapper


class OIDCSession(Session):
    def __init__(self, config):
        super().__init__(config)
        self.stored_token_directory = config.get("general", {}).get("stored_token_directory", "/tmp/srcnet/user")
        self.access_tokens = {}
        self.refresh_tokens = []

    def _add_tokens_to_internal_cache(self, token, path_on_disk=None):
        """ Add tokens to the internal application cache.

        :param str token: The token to add.
        :param str path_on_disk: The path to the token on disk.
        """
        logging.debug("Adding tokens from file {} to internal cache.".format(path_on_disk))
        access_token = token.get('access_token')
        access_token_decoded = jwt.decode(access_token, options={"verify_signature": False})
        if 'aud' in access_token_decoded:
            access_token_audience = access_token_decoded.get('aud')
            # As only one access token is kept per audience, we should keep the one with the latest expiry time
            # otherwise it's possible that an invalid token (but with valid refresh) is preferred here over a valid
            # one just by virtue of being last in the list. This can cause problems if it is then disregarded later
            # when expired tokens are removed as part of the remove_expired_tokens decorator.
            #
            if self.access_tokens.get(access_token_audience, False):
                this_access_token_expires_at = access_token_decoded.get('exp')
                existing_access_token_expires_at = self.access_tokens[access_token_audience].get('expires_at')
                if existing_access_token_expires_at > this_access_token_expires_at:
                    return

            self.access_tokens[access_token_audience] = {
                'token': access_token,
                'expires_at': access_token_decoded.get('exp'),
                'path_on_disk': path_on_disk if path_on_disk else 'INTERNAL'
            }

            # Keep the association between refresh and access tokens as when refreshing the associated access
            # token is (by default in the client) invalidated.
            #
            refresh_token = token.get('refresh_token')
            if refresh_token:
                refresh_token_decoded = jwt.decode(refresh_token, options={"verify_signature": False})
                self.refresh_tokens.append({
                    'token': refresh_token,
                    'associated_access_token': access_token,
                    'expires_at': refresh_token_decoded.get('exp'),
                    'path_on_disk': path_on_disk if path_on_disk else 'INTERNAL'
                })

    def _save_tokens_to_disk(self, token):
        """ Save a token to disk.

        :param str token: The token to save.
        :return: Returns the path to the token on disk.
        :rtype: str
        """
        logging.debug("Saving tokens to disk.")
        os.makedirs(self.stored_token_directory, exist_ok=True)

        token_path_on_disk = os.path.join(self.stored_token_directory, "{}.token".format(str(uuid4())))
        with open(token_path_on_disk, 'w') as f:
            f.write(json.dumps(token))

        return token_path_on_disk

    @handle_client_exceptions
    def load_tokens_from_disk(self):
        """ Load OIDC tokens from disk into the internal cache. """
        logging.debug("Loading tokens from disk.")
        for entry in glob.glob(os.path.join(self.stored_token_directory, "*.token")):
            try:
                with open(entry, 'r') as f:
                    self._add_tokens_to_internal_cache(json.loads(f.read()), path_on_disk=f.name)
            except json.decoder.JSONDecodeError:
                os.remove(entry)

    @handle_client_exceptions
    @check_authentication_api_aliveness
    @remove_expired_tokens
    def get_device_authorization_response(self):
        """ Start a device code flow.

        :return: A device flow authorization response.
        :rtype: str
        """
        # Add timeout protection to prevent hanging
        import threading
        import queue
        import requests
        
        def login_with_timeout():
            # Get the auth API URL
            auth_api_url = self.get_api_url_by_service_name("authn-api")
            
            # Make direct HTTP request with timeout
            login_url = f"{auth_api_url}/login/device"
            headers = {"Content-Type": "application/json"}
            data = {
                "redirect_uri": ""
            }
            
            response = requests.get(login_url, headers=headers, timeout=5)
            response.raise_for_status()
            return response.json()
        
        # Start the request in a separate thread with timeout
        result_queue = queue.Queue()
        exception_queue = queue.Queue()
        
        def login_wrapper():
            try:
                result = login_with_timeout()
                result_queue.put(result)
            except Exception as e:
                exception_queue.put(e)
        
        login_thread = threading.Thread(target=login_wrapper)
        login_thread.daemon = True
        login_thread.start()
        
        # Wait for result with timeout
        try:
            login_thread.join(timeout=10)  # 10 second timeout for login request
            
            if login_thread.is_alive():
                # Thread is still running, timeout occurred
                raise Exception("Authentication server is currently unavailable. Login request timed out.")
            
            # Check for exceptions first
            try:
                exception = exception_queue.get_nowait()
                raise exception
            except queue.Empty:
                pass
            
            # Get the result
            login_response = result_queue.get_nowait()
            
        except queue.Empty:
            raise Exception("Authentication server is currently unavailable. Login request timed out.")
        
        return login_response

    @handle_client_exceptions
    @check_authentication_api_aliveness
    @remove_expired_tokens
    def exchange_token(self, service_name, version='latest', store_to_disk=True, by_refresh=True):
        """ Exchange an access token for a service.

        This can be done either by using a refresh_token grant or by directly exchanging the access token with a
        token exchange grant.

        Note that to use the authentication's exchange endpoint an active access token must also be present. This is
        required for user group permissions checks. If the by_refresh flag is set and no active access token is
        available in the environment a new one will be requested.

        :param str service_name: The service name to exchange a token for.
        :param str version: The version of the service to exchange a token for.
        :param bool store_to_disk: Store the tokens to disk for persistence.
        :param bool by_refresh: Exchange tokens using the refresh_token grant.
        :return: Success flag
        :rtype: bool
        """
        logging.debug("Exchanging token for service {}".format(service_name))
        token = None
        
        # Add timeout protection to prevent hanging
        import threading
        import queue
        import requests
        
        def exchange_with_timeout():
            nonlocal token
            try:
                # Get the auth API URL
                auth_api_url = self.get_api_url_by_service_name("authn-api")
                
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
                                if access_token.get('token') == refresh_token.get('associated_access_token'):
                                    logging.debug(" - Found a valid matching access token, proceeding with exchange")
                                    
                                    # Make direct HTTP request with timeout
                                    exchange_url = f"{auth_api_url}/token/exchange/{service_name}"
                                    params = {
                                        "version": version,
                                        "try_use_cache": "true",
                                        "access_token": access_token.get('token'),
                                        "refresh_token": refresh_token.get('token')
                                    }
                                    
                                    response = requests.get(exchange_url, params=params, timeout=5)
                                    response.raise_for_status()
                                    token = response.json()
                                    
                                    # need to remove previous (now invalid) access & refresh token from caches
                                    self.refresh_tokens.pop(refresh_token_idx)
                                    aud_to_pop = []
                                    for aud, attributes in self.access_tokens.items():
                                        if attributes.get('token') == refresh_token.get('associated_access_token'):
                                            aud_to_pop.append(aud)
                                    for aud in aud_to_pop:
                                        self.access_tokens.pop(aud)

                                    # and also on disk
                                    path_on_disk = refresh_token.get('path_on_disk')
                                    if path_on_disk and os.path.exists(path_on_disk):
                                        os.remove(path_on_disk)

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
                                    # Make direct HTTP request with timeout for token refresh
                                    refresh_url = f"{auth_api_url}/token"
                                    params = {
                                        "grant_type": "refresh_token",
                                        "refresh_token": refresh_token.get('token')
                                    }
                                    
                                    response = requests.get(refresh_url, params=params, timeout=5)
                                    response.raise_for_status()
                                    refreshed_token = response.json()

                                    # need to remove previous (now invalid) refresh token from caches
                                    self.refresh_tokens.pop(refresh_token_idx)

                                    # and on disk
                                    path_on_disk = refresh_token.get('path_on_disk')
                                    if path_on_disk and os.path.exists(path_on_disk):
                                        os.remove(path_on_disk)

                                    # Finally, exchange this refreshed token.
                                    logging.debug(" - Exchanging refresh token")
                                    exchange_url = f"{auth_api_url}/token/exchange/{service_name}"
                                    params = {
                                        "version": version,
                                        "try_use_cache": "true",
                                        "access_token": refreshed_token.get('access_token'),
                                        "refresh_token": refreshed_token.get('refresh_token')
                                    }
                                    
                                    response = requests.get(exchange_url, params=params, timeout=5)
                                    response.raise_for_status()
                                    token = response.json()
                                    break
                                except Exception as e:
                                    continue
                    else:
                        logging.critical("Exchange requested by refresh but no valid refresh tokens exist.")
                else:
                    logging.debug(" - Attempting direct access token exchange")
                    if not self.access_tokens:
                        logging.critical("Exchange requested but no valid access tokens exist.")
                    else:
                        # select any valid token randomly
                        random_access_token = random.choice(list(self.access_tokens.values()))
                        access_token_to_exchange = random_access_token.get('token')
                        
                        # Make direct HTTP request with timeout
                        exchange_url = f"{auth_api_url}/token/exchange/{service_name}"
                        params = {
                            "version": "latest",
                            "try_use_cache": "true",
                            "access_token": access_token_to_exchange
                        }
                        
                        response = requests.get(exchange_url, params=params, timeout=5)
                        response.raise_for_status()
                        token = response.json()
            except Exception as e:
                # Re-raise the exception to be caught by the timeout wrapper
                raise e
        
        # Start the exchange in a separate thread with timeout
        result_queue = queue.Queue()
        exception_queue = queue.Queue()
        
        def exchange_wrapper():
            try:
                exchange_with_timeout()
                result_queue.put(True)
            except Exception as e:
                exception_queue.put(e)
        
        exchange_thread = threading.Thread(target=exchange_wrapper)
        exchange_thread.daemon = True
        exchange_thread.start()
        
        # Wait for result with timeout
        try:
            exchange_thread.join(timeout=10)  # 10 second timeout for token exchange
            
            if exchange_thread.is_alive():
                # Thread is still running, timeout occurred
                raise Exception("Authentication server is currently unavailable. Token exchange timed out.")
            
            # Check for exceptions first
            try:
                exception = exception_queue.get_nowait()
                raise exception
            except queue.Empty:
                pass
            
        except queue.Empty:
            raise Exception("Authentication server is currently unavailable. Token exchange timed out.")

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
        """ Get an access token for a service from the environment.

        :param str service_name: The service name to get a token for.
        :return: An access token.
        :rtype: str
        """
        return self.access_tokens.get(service_name, {}).get('token')

    @handle_client_exceptions
    @remove_expired_tokens
    def list_access_tokens(self, truncate_access_token_chars=50):
        """ List available access tokens in the environment.

        :param int truncate_access_token_chars: Truncate the access token to a set number of characters.
        :return: A tabulated list of access tokens.
        :rtype: str
        """
        tokens = {}
        for aud, attributes in self.access_tokens.items():
            access_token = attributes.get('token')
            expires_at = attributes.get('expires_at')
            path_on_disk = attributes.get('path_on_disk')
            has_associated_refresh_token = False
            for token in self.refresh_tokens:
                if access_token == token.get('associated_access_token'):
                    has_associated_refresh_token = True

            tokens[aud] = {
                'access_token': access_token,
                'expires_at': expires_at,
                'path_on_disk': path_on_disk,
                'has_associated_refresh_token': has_associated_refresh_token
            }
        return tokens

    @handle_client_exceptions
    @check_authentication_api_aliveness
    @remove_expired_tokens
    def inspect_access_token(self, service_name):
        """ Introspect an access token.

        :param str service_name: The service name to exchange a token for.
        :return: An instrospected access token.
        :rtype: str
        """
        access_token_for_service = self.access_tokens.get(service_name, {})
        if not access_token_for_service:
            logging.critical("No access token exists for service {}".format(service_name))
            return
        access_token_decoded = jwt.decode(access_token_for_service.get('token'), options={"verify_signature": False})
        return access_token_decoded

    @handle_client_exceptions
    @remove_expired_tokens
    def delete_access_token(self, service_name):
        """ Delete an access token for a service.

        :param str service_name: The service name to delete a token for.
        :return: Success flag indicating if the token was deleted.
        :rtype: bool
        """
        logging.debug("Deleting access token for service {}".format(service_name))
        
        # Find the token in access_tokens
        token_to_delete = None
        for aud, attributes in self.access_tokens.items():
            if aud == service_name:
                token_to_delete = attributes
                break
        
        if not token_to_delete:
            logging.warning("No access token found for service {}".format(service_name))
            return False
        
        # Remove from memory
        self.access_tokens.pop(service_name, None)
        
        # Remove associated refresh tokens
        token_path = token_to_delete.get('path_on_disk')
        if token_path and token_path != 'INTERNAL':
            # Remove refresh tokens associated with this access token
            refresh_tokens_to_remove = []
            for refresh_token in self.refresh_tokens:
                if refresh_token.get('associated_access_token') == token_to_delete.get('token'):
                    refresh_tokens_to_remove.append(refresh_token)
            
            for refresh_token in refresh_tokens_to_remove:
                self.refresh_tokens.remove(refresh_token)
            
            # Remove from disk
            try:
                if os.path.exists(token_path):
                    os.remove(token_path)
                    logging.debug("Removed token file from disk: {}".format(token_path))
            except Exception as e:
                logging.error("Failed to remove token file from disk: {}".format(e))
        
        logging.debug("Successfully deleted access token for service {}".format(service_name))
        return True

    @handle_client_exceptions
    @check_authentication_api_aliveness
    @remove_expired_tokens
    def request_token(self, device_code, store_to_disk=True):
        """ Complete a device flow.

        :param str device_code: The device code.
        :param bool store_to_disk: Store the token to disk for persistence.
        :return: Either the error code as a string or True.
        :rtype: Union[bool, dict]
        """
        # Add timeout protection to prevent hanging
        import threading
        import queue
        import requests
        
        def request_with_timeout():
            # Get the auth API URL
            auth_api_url = self.get_api_url_by_service_name("authn-api")
            
            # Make direct HTTP request with timeout
            token_url = f"{auth_api_url}/token"
            params = {
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                "device_code": device_code
            }
            
            response = requests.get(token_url, params=params, timeout=5)
            response.raise_for_status()
            return response.json()
        
        # Start the request in a separate thread with timeout
        result_queue = queue.Queue()
        exception_queue = queue.Queue()
        
        def request_wrapper():
            try:
                result = request_with_timeout()
                result_queue.put(result)
            except Exception as e:
                exception_queue.put(e)
        
        request_thread = threading.Thread(target=request_wrapper)
        request_thread.daemon = True
        request_thread.start()
        
        # Wait for result with timeout
        try:
            request_thread.join(timeout=10)  # 10 second timeout for token request
            
            if request_thread.is_alive():
                # Thread is still running, timeout occurred
                raise Exception("Authentication server is currently unavailable. Token request timed out.")
            
            # Check for exceptions first
            try:
                exception = exception_queue.get_nowait()
                raise exception
            except queue.Empty:
                pass
            
            # Get the result
            token_response = result_queue.get_nowait()
            
        except queue.Empty:
            raise Exception("Authentication server is currently unavailable. Token request timed out.")
        
        token = token_response.get('token')
        if token:
            token_path_on_disk = None
            if store_to_disk:
                token_path_on_disk = self._save_tokens_to_disk(token)
            self._add_tokens_to_internal_cache(token, path_on_disk=token_path_on_disk)
            return True
        return token_response.get('error')

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
        qr.add_data(device_authorization_response.get('verification_uri_complete'))

        # add instructional text for user if they don't want to use qr code
        user_instruction_text = ("Scan the QR code, or using a browser on another device, visit " +
                                 "{verification_uri} and enter code {user_code}".format(
                                     verification_uri=device_authorization_response.get('verification_uri'),
                                     user_code=device_authorization_response.get('user_code')))

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
                self.request_token(device_code=device_authorization_response.get('device_code'))
                success = True
                break
            except Exception as e:
                ex_type, ex_value, ex_traceback = sys.exc_info()
                logging.debug(ex_value)
            print("Polling for token... ({attempt}/{max_attempts})".format(
                attempt=attempt + 1, max_attempts=max_attempts), end='\r')
            time.sleep(wait_between_polling_s)
        print()
        print()
        if success:
            print("Successfully polled for token. You are now logged in.")
        else:
            print("Failed to poll for token. Please try again.")
        print()


