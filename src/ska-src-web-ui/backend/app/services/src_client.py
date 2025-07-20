import sys
import os
import logging
from typing import Optional, Dict, Any, List
import json
import datetime

# Add the ska-src-clients to the Python path
# This assumes ska-src-clients is installed or available in the environment
try:
    from ska_src_clients.session.oidc import OIDCSession
    from ska_src_clients.api import DataAPI, SiteAPI, MetadataAPI
    from ska_src_clients.common.utility import load_config
    from ska_src_clients.common.exceptions import CustomException, NoAccessTokenFoundForService
except ImportError:
    # Fallback for development - adjust the path to point to the correct location
    sys.path.append(os.path.join(os.path.dirname(__file__), '../../../ska-src-clients/src'))
    from ska_src_clients.session.oidc import OIDCSession
    from ska_src_clients.api import DataAPI, SiteAPI, MetadataAPI
    from ska_src_clients.common.utility import load_config
    from ska_src_clients.common.exceptions import CustomException, NoAccessTokenFoundForService


class SRCClientService:
    """Service class to interact with SKA SRC CLI tools."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize the SRC client service."""
        if config_path:
            self.config = load_config([config_path])
        else:
            # Use default config paths when no specific path is provided
            self.config = load_config()
        if not self.config:
            raise ValueError("No valid config file found")
        
        self.session = OIDCSession(config=self.config)
        self.session.load_tokens_from_disk()
        
        # Initialize API clients
        self.data_api = DataAPI(session=self.session)
        self.site_api = SiteAPI(session=self.session)
        self.metadata_api = MetadataAPI(session=self.session)
    
    def get_config_value(self, path: str) -> Optional[str]:
        """Get a configuration value by dot-separated path."""
        try:
            keys = path.split('.')
            value = self.config
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return None
    
    def start_device_flow(self) -> Dict[str, Any]:
        """Start OIDC device flow authentication."""
        try:
            response = self.session.get_device_authorization_response()
            return {
                "device_code": response.get("device_code"),
                "user_code": response.get("user_code"),
                "verification_uri": response.get("verification_uri"),
                "verification_uri_complete": response.get("verification_uri_complete"),
                "expires_in": response.get("expires_in"),
                "interval": response.get("interval"),
                "message": response.get("message", "Please complete authentication in your browser")
            }
        except Exception as e:
            logging.error(f"Error starting device flow: {e}")
            raise
    
    def request_token(self, max_polling_attempts: int = 60, wait_between_polling_s: int = 5) -> Dict[str, Any]:
        """Request a new access token using the device flow with polling."""
        try:
            # Start the device flow
            device_authorization_response = self.session.get_device_authorization_response()
            
            # Return the initial response with device flow details
            return {
                "success": True,
                "message": "Device flow started. Please complete authentication in your browser.",
                "device_code": device_authorization_response.get("device_code"),
                "user_code": device_authorization_response.get("user_code"),
                "verification_uri": device_authorization_response.get("verification_uri"),
                "verification_uri_complete": device_authorization_response.get("verification_uri_complete"),
                "expires_in": device_authorization_response.get("expires_in"),
                "interval": device_authorization_response.get("interval")
            }
        except Exception as e:
            logging.error(f"Error starting token request: {e}")
            return {
                "success": False,
                "message": f"Failed to start token request: {str(e)}"
            }
    
    def check_token_completion(self, device_code: str) -> Dict[str, Any]:
        """Check if a token request has been completed (single check, no polling)."""
        try:
            # Try to request the token - this will succeed if authorization is complete
            result = self.session.request_token(device_code=device_code)
            
            logging.debug(f"Token completion check result: {result} (type: {type(result)})")
            
            if result is True:
                return {
                    "success": True,
                    "message": "Token request completed successfully. You are now logged in."
                }
            elif isinstance(result, str):
                # Error occurred - check if it's an authorization pending error
                logging.debug(f"Token completion check error: {result}")
                if "authorization_pending" in result.lower() or "slow_down" in result.lower():
                    return {
                        "success": False,
                        "message": "Authorization still pending. Please complete authentication in your browser."
                    }
                else:
                    return {
                        "success": False,
                        "message": f"Token request failed: {result}"
                    }
            else:
                # Unexpected result
                logging.debug(f"Token completion check unexpected result: {result}")
                return {
                    "success": False,
                    "message": "Authorization still pending. Please complete authentication in your browser."
                }
        except Exception as e:
            # Check if this is a 500 error or other fatal error
            error_str = str(e).lower()
            if "500" in error_str or "internal server error" in error_str or "server error" in error_str:
                # Before treating as fatal, check if we actually got a token
                try:
                    tokens = self.list_tokens()
                    if tokens:
                        logging.info(f"Token completion check: Found {len(tokens)} tokens despite server error")
                        response = {
                            "success": True,
                            "message": "Token request completed successfully. You are now logged in."
                        }
                        logging.info(f"Returning success response: {response}")
                        return response
                except Exception as token_check_error:
                    logging.debug(f"Could not check for existing tokens: {token_check_error}")
                
                logging.error(f"Token completion check (fatal error): {e}")
                return {
                    "success": False,
                    "fatal": True,
                    "message": f"Authentication failed due to server error. Please try again: {str(e)}"
                }
            else:
                # If an exception is raised, it usually means authorization is still pending
                # This is the expected behavior when the user hasn't completed authentication yet
                logging.debug(f"Token completion check (pending): {e}")
                return {
                    "success": False,
                    "message": "Authorization still pending. Please complete authentication in your browser."
                }

    def complete_token_request(self, device_code: str) -> Dict[str, Any]:
        """Complete a token request by polling for completion."""
        try:
            # Poll for token completion with timeout protection
            success = False
            max_attempts = 60
            wait_between_polling_s = 5
            
            for attempt in range(0, max_attempts):
                try:
                    # Add timeout to each polling attempt
                    import threading
                    import queue
                    
                    result_queue = queue.Queue()
                    exception_queue = queue.Queue()
                    
                    def poll_with_timeout():
                        try:
                            result = self.session.request_token(device_code=device_code)
                            result_queue.put(result)
                        except Exception as e:
                            exception_queue.put(e)
                    
                    # Start polling in a separate thread with timeout
                    poll_thread = threading.Thread(target=poll_with_timeout)
                    poll_thread.daemon = True
                    poll_thread.start()
                    
                    # Wait for result with timeout
                    try:
                        poll_thread.join(timeout=10)  # 10 second timeout for each poll
                        
                        if poll_thread.is_alive():
                            # Thread is still running, timeout occurred
                            logging.debug(f"Polling attempt {attempt + 1} timed out")
                            continue
                        
                        # Check for exceptions first
                        try:
                            exception = exception_queue.get_nowait()
                            raise exception
                        except queue.Empty:
                            pass
                        
                        # Get the result
                        result = result_queue.get_nowait()
                        
                    except queue.Empty:
                        logging.debug(f"Polling attempt {attempt + 1} timed out")
                        continue
                    
                    if result is True:
                        success = True
                        break
                    elif isinstance(result, str):
                        # Error occurred
                        return {
                            "success": False,
                            "message": f"Token request failed: {result}"
                        }
                except Exception as e:
                    logging.debug(f"Polling attempt {attempt + 1}: {e}")
                
                # Wait before next attempt
                import time
                time.sleep(wait_between_polling_s)
            
            if success:
                return {
                    "success": True,
                    "message": "Token request completed successfully. You are now logged in."
                }
            else:
                return {
                    "success": False,
                    "message": "Token request timed out. Please try again."
                }
        except Exception as e:
            logging.error(f"Error completing token request: {e}")
            return {
                "success": False,
                "message": f"Failed to complete token request: {str(e)}"
            }
    
    def exchange_token(self, service_name: str, version: str = "latest", file_name: Optional[str] = None) -> bool:
        """Exchange token for a specific service."""
        try:
            # If file_name is provided, we need to ensure that token is loaded and active
            if file_name:
                # Load the specific token file and make it active for exchange
                token_path = os.path.join(self.session.stored_token_directory, file_name)
                if os.path.exists(token_path):
                    # Load the token from disk and add it to the session's internal cache
                    with open(token_path, 'r') as f:
                        token_data = json.load(f)
                    
                    # Clear existing tokens and add only the specified token
                    self.session.access_tokens.clear()
                    self.session.refresh_tokens.clear()
                    
                    # Add the token to the session's internal cache
                    self.session._add_tokens_to_internal_cache(token_data, path_on_disk=token_path)
                    logging.info(f"Loaded token from {file_name} for exchange")
                else:
                    logging.error(f"Token file {file_name} not found")
                    raise FileNotFoundError(f"Token file {file_name} not found")
            else:
                logging.warning("No file_name provided, using existing tokens")
            
            result = self.session.exchange_token(service_name, version=version)
            
            # After exchange, reload all tokens from disk so other operations can access them
            self.session.load_tokens_from_disk()
            return result
        except Exception as e:
            logging.error(f"Error exchanging token for {service_name}: {e}")
            raise
    
    def list_tokens(self) -> List[Dict[str, Any]]:
        """List all tokens on disk, each with its file name as file_name."""
        import glob
        tokens = []
        for token_path in glob.glob(os.path.join(self.session.stored_token_directory, "*.token")):
            try:
                with open(token_path, 'r') as f:
                    token_data = json.load(f)
                access_token = token_data.get('access_token', '')
                # Decode JWT to get audience and expiration
                import jwt
                try:
                    decoded = jwt.decode(access_token, options={"verify_signature": False})
                    service_name = decoded.get('aud', 'unknown')
                    expires_at_epoch = decoded.get('exp')
                    if expires_at_epoch:
                        expires_dt_utc = datetime.datetime.utcfromtimestamp(expires_at_epoch).strftime('%Y-%m-%d %H:%M:%S UTC')
                        expires_dt_local = datetime.datetime.fromtimestamp(expires_at_epoch).strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        expires_dt_utc = "-"
                        expires_dt_local = "-"
                except Exception:
                    service_name = 'unknown'
                    expires_dt_utc = expires_dt_local = "-"
                tokens.append({
                    "service_name": service_name,
                    "access_token": access_token[:20] + "...",
                    "expires_utc": expires_dt_utc,
                    "expires_local": expires_dt_local,
                    "path_on_disk": token_path,
                    "has_refresh_token": bool(token_data.get('refresh_token')),
                    "file_name": os.path.basename(token_path)
                })
            except Exception as e:
                logging.error(f"Error reading token file {token_path}: {e}")
        return tokens

    def delete_token_by_file(self, file_name: str) -> bool:
        """Delete a token by its file name."""
        token_path = os.path.join(self.session.stored_token_directory, file_name)
        if os.path.exists(token_path):
            try:
                os.remove(token_path)
                return True
            except Exception as e:
                logging.error(f"Error deleting token file {token_path}: {e}")
                return False
        else:
            return False

    def has_valid_tokens(self) -> bool:
        """Check if there are any valid tokens available."""
        try:
            tokens = self.list_tokens()
            return len(tokens) > 0
        except Exception as e:
            logging.debug(f"Error checking tokens: {e}")
            return False
    
    def inspect_token(self, service_name: str) -> Dict[str, Any]:
        """Inspect a specific access token."""
        try:
            result = self.session.inspect_access_token(service_name)
            return result
        except Exception as e:
            logging.error(f"Error inspecting token for {service_name}: {e}")
            raise

    def delete_token(self, service_name: str) -> bool:
        """Delete a specific access token."""
        try:
            result = self.session.delete_access_token(service_name)
            return result
        except Exception as e:
            logging.error(f"Error deleting token for {service_name}: {e}")
            raise
    
    def download_data(self, namespace: str, name: str, sort: str = "nearest_by_ip", 
                     ip_address: str = "", no_verify: bool = False, output: Optional[str] = None) -> Dict[str, Any]:
        """Download data by namespace and name."""
        # Check if we have a data-management-api token before making the API call
        try:
            tokens = self.list_tokens()
            has_data_management_token = any(token.get('service_name') == 'data-management-api' for token in tokens)
            if not has_data_management_token:
                raise NoAccessTokenFoundForService("data-management-api")
        except Exception as e:
            logging.debug(f"Error checking data-management-api tokens: {e}")
            raise NoAccessTokenFoundForService("data-management-api")
        
        try:
            result = self.data_api.download(namespace, name, sort, ip_address, not no_verify, output)
            return result
        except Exception as e:
            logging.error(f"Error downloading data: {e}")
            raise
    
    def locate_data(self, namespace: str, name: str, sort: str = "nearest_by_ip", 
                   ip_address: str = "") -> Dict[str, Any]:
        """Locate data by namespace and name."""
        # Check if we have a data-management-api token before making the API call
        try:
            tokens = self.list_tokens()
            has_data_management_token = any(token.get('service_name') == 'data-management-api' for token in tokens)
            if not has_data_management_token:
                raise NoAccessTokenFoundForService("data-management-api")
        except Exception as e:
            logging.debug(f"Error checking data-management-api tokens: {e}")
            raise NoAccessTokenFoundForService("data-management-api")
        
        try:
            result = self.data_api.locate(namespace, name, sort, ip_address)
            return result
        except Exception as e:
            logging.error(f"Error locating data: {e}")
            raise
    
    def list_files(self, namespace: str, name: str, detail: bool = False, 
                  filters: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        """List files in a namespace."""
        # Check if we have a data-management-api token before making the API call
        try:
            tokens = self.list_tokens()
            has_data_management_token = any(token.get('service_name') == 'data-management-api' for token in tokens)
            if not has_data_management_token:
                raise NoAccessTokenFoundForService("data-management-api")
        except Exception as e:
            logging.debug(f"Error checking data-management-api tokens: {e}")
            raise NoAccessTokenFoundForService("data-management-api")
        
        try:
            result = self.data_api.list_files_in_namespace(namespace, name, detail, filters, limit)
            return result
        except Exception as e:
            logging.error(f"Error listing files: {e}")
            raise
    
    def upload_for_ingest(self, path: str, ingest_service_id: str, namespace: str, 
                         metadata_suffix: str = ".meta", extra_metadata: str = "{}", 
                         debug: bool = False) -> Dict[str, Any]:
        """Upload data for ingest."""
        # Check if we have a data-management-api token before making the API call
        try:
            tokens = self.list_tokens()
            has_data_management_token = any(token.get('service_name') == 'data-management-api' for token in tokens)
            if not has_data_management_token:
                raise NoAccessTokenFoundForService("data-management-api")
        except Exception as e:
            logging.debug(f"Error checking data-management-api tokens: {e}")
            raise NoAccessTokenFoundForService("data-management-api")
        
        try:
            result = self.data_api.upload_for_ingest(
                path, ingest_service_id, namespace, metadata_suffix, extra_metadata, debug
            )
            return result
        except Exception as e:
            logging.error(f"Error uploading for ingest: {e}")
            raise
    
    def move_request(self, to_storage_area_id: str, dids: List[str], lifetime: str, 
                    parent_namespace: Optional[str] = None) -> Dict[str, Any]:
        """Make a data movement request."""
        # Check if we have a data-management-api token before making the API call
        try:
            tokens = self.list_tokens()
            has_data_management_token = any(token.get('service_name') == 'data-management-api' for token in tokens)
            if not has_data_management_token:
                raise NoAccessTokenFoundForService("data-management-api")
        except Exception as e:
            logging.debug(f"Error checking data-management-api tokens: {e}")
            raise NoAccessTokenFoundForService("data-management-api")
        
        try:
            result = self.data_api.move_request(to_storage_area_id, dids, lifetime, parent_namespace)
            return result
        except Exception as e:
            logging.error(f"Error making move request: {e}")
            raise
    
    def move_status(self, job_id: str) -> Dict[str, Any]:
        """Get the status of a data movement request."""
        # Check if we have a data-management-api token before making the API call
        try:
            tokens = self.list_tokens()
            has_data_management_token = any(token.get('service_name') == 'data-management-api' for token in tokens)
            if not has_data_management_token:
                raise NoAccessTokenFoundForService("data-management-api")
        except Exception as e:
            logging.debug(f"Error checking data-management-api tokens: {e}")
            raise NoAccessTokenFoundForService("data-management-api")
        
        try:
            result = self.data_api.move_status(job_id)
            return result
        except Exception as e:
            logging.error(f"Error getting move status: {e}")
            raise
    
    def stage_request(self, to_storage_area_id: str, dids: List[str], lifetime: str, 
                     parent_namespace: Optional[str] = None) -> Dict[str, Any]:
        """Make a data staging request."""
        # Check if we have a data-management-api token before making the API call
        try:
            tokens = self.list_tokens()
            has_data_management_token = any(token.get('service_name') == 'data-management-api' for token in tokens)
            if not has_data_management_token:
                raise NoAccessTokenFoundForService("data-management-api")
        except Exception as e:
            logging.debug(f"Error checking data-management-api tokens: {e}")
            raise NoAccessTokenFoundForService("data-management-api")
        
        try:
            result = self.data_api.stage_request(to_storage_area_id, dids, lifetime, parent_namespace)
            return result
        except Exception as e:
            logging.error(f"Error making stage request: {e}")
            raise
    
    def stage_status(self, job_id: str) -> Dict[str, Any]:
        """Get the status of a data staging request."""
        # Check if we have a data-management-api token before making the API call
        try:
            tokens = self.list_tokens()
            has_data_management_token = any(token.get('service_name') == 'data-management-api' for token in tokens)
            if not has_data_management_token:
                raise NoAccessTokenFoundForService("data-management-api")
        except Exception as e:
            logging.debug(f"Error checking data-management-api tokens: {e}")
            raise NoAccessTokenFoundForService("data-management-api")
        
        try:
            result = self.data_api.stage_status(job_id)
            return result
        except Exception as e:
            logging.error(f"Error getting stage status: {e}")
            raise
    
    def list_namespaces(self) -> List[Dict[str, Any]]:
        """List available namespaces."""
        # Check if we have a data-management-api token before making the API call
        try:
            tokens = self.list_tokens()
            has_data_management_token = any(token.get('service_name') == 'data-management-api' for token in tokens)
            if not has_data_management_token:
                raise NoAccessTokenFoundForService("data-management-api")
        except Exception as e:
            logging.debug(f"Error checking data-management-api tokens: {e}")
            raise NoAccessTokenFoundForService("data-management-api")
        
        try:
            result = self.data_api.list_namespaces()
            return result
        except Exception as e:
            logging.error(f"Error listing namespaces: {e}")
            raise
    
    def get_site(self, site_id: str) -> Dict[str, Any]:
        """Get information about a specific site."""
        try:
            result = self.site_api.get_site(site_id)
            return result
        except Exception as e:
            logging.error(f"Error getting site: {e}")
            raise
    
    def list_sites(self, node_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """List sites."""
        # Check if we have a site-capabilities-api token before making the API call
        try:
            tokens = self.list_tokens()
            has_site_capabilities_token = any(token.get('service_name') == 'site-capabilities-api' for token in tokens)
            if not has_site_capabilities_token:
                raise NoAccessTokenFoundForService("site-capabilities-api")
        except Exception as e:
            logging.debug(f"Error checking site-capabilities-api tokens: {e}")
            raise NoAccessTokenFoundForService("site-capabilities-api")
        
        try:
            # Call site_api without timeout parameter (it's not supported)
            result = self.site_api.list_sites(node_name=node_name)
            return result
        except Exception as e:
            logging.error(f"Error listing sites: {e}")
            raise
    
    def get_compute(self, compute_id: str) -> Dict[str, Any]:
        """Get compute details by ID."""
        try:
            result = self.site_api.get_compute(compute_id)
            return result
        except Exception as e:
            logging.error(f"Error getting compute: {e}")
            raise
    
    def list_compute(self, node_name: Optional[str] = None, site_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """List compute resources."""
        try:
            result = self.site_api.list_compute(node_name=node_name, site_name=site_name)
            return result
        except Exception as e:
            logging.error(f"Error listing compute: {e}")
            raise
    
    def get_service(self, service_id: str) -> Dict[str, Any]:
        """Get service details by ID."""
        try:
            result = self.site_api.get_service(service_id)
            return result
        except Exception as e:
            logging.error(f"Error getting service: {e}")
            raise
    
    def list_services(self, service_type: Optional[str] = None, node_name: Optional[str] = None,
                     site_name: Optional[str] = None, scope: str = "all") -> List[Dict[str, Any]]:
        """List services."""
        # Check if we have a site-capabilities-api token before making the API call
        try:
            tokens = self.list_tokens()
            has_site_capabilities_token = any(token.get('service_name') == 'site-capabilities-api' for token in tokens)
            if not has_site_capabilities_token:
                raise NoAccessTokenFoundForService("site-capabilities-api")
        except Exception as e:
            logging.debug(f"Error checking site-capabilities-api tokens: {e}")
            raise NoAccessTokenFoundForService("site-capabilities-api")
        
        try:
            result = self.site_api.list_services(service_type=service_type, node_name=node_name,
                                               site_name=site_name, scope=scope)
            return result
        except Exception as e:
            logging.error(f"Error listing services: {e}")
            raise
    
    def enable_service(self, service_id: str) -> bool:
        """Enable a service by ID."""
        try:
            result = self.site_api.enable_service(service_id)
            return result
        except Exception as e:
            logging.error(f"Error enabling service: {e}")
            raise
    
    def disable_service(self, service_id: str) -> bool:
        """Disable a service by ID."""
        try:
            result = self.site_api.disable_service(service_id)
            return result
        except Exception as e:
            logging.error(f"Error disabling service: {e}")
            raise 

    def list_services_enriched(self, service_type: Optional[str] = None, node_name: Optional[str] = None,
                              site_name: Optional[str] = None, scope: str = "all") -> List[Dict[str, Any]]:
        """
        List services, enriched with site/node info and extra details (host, port, path, etc).
        Only enriches the first 20 services for development. Fetches details in parallel.
        """
        # Check if we have a site-capabilities-api token before making the API call
        try:
            tokens = self.list_tokens()
            has_site_capabilities_token = any(token.get('service_name') == 'site-capabilities-api' for token in tokens)
            if not has_site_capabilities_token:
                raise NoAccessTokenFoundForService("site-capabilities-api")
        except Exception as e:
            logging.debug(f"Error checking site-capabilities-api tokens: {e}")
            raise NoAccessTokenFoundForService("site-capabilities-api")
        
        try:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            services = self.site_api.list_services(service_type=service_type, node_name=node_name,
                                                  site_name=site_name, scope=scope)
            sites = self.site_api.list_sites()
            
            # Build a comprehensive lookup for node/site by various possible keys
            node_lookup = {}
            for site in sites:
                # Add all possible keys that could match
                if 'node' in site:
                    node_lookup[site['node']] = site
                if 'name' in site:
                    node_lookup[site['name']] = site
                if 'id' in site:
                    node_lookup[site['id']] = site
                # Also add lowercase versions for case-insensitive matching
                if 'node' in site:
                    node_lookup[site['node'].lower()] = site
                if 'name' in site:
                    node_lookup[site['name'].lower()] = site
                if 'id' in site:
                    node_lookup[site['id'].lower()] = site
            
            logging.debug(f"Built node lookup with {len(node_lookup)} entries")
            logging.debug(f"Available lookup keys: {list(node_lookup.keys())}")
            
            enriched = []
            # Remove the development limit: process all services
            services_to_enrich = services
            # Prepare for parallel fetching
            def fetch_details(svc_id):
                try:
                    return self.site_api.get_service(svc_id)
                except Exception as e:
                    logging.warning(f"Could not get details for service {svc_id}: {e}")
                    return {}
            
            def check_service_status(service_data):
                """Check if a service is up by pinging its endpoint."""
                try:
                    host = service_data.get('host')
                    port = service_data.get('port')
                    path = service_data.get('path', '/')
                    prefix = service_data.get('prefix', 'https')
                    
                    if not host or not port:
                        return 'unknown'
                    
                    # Construct the URL
                    url = f"{prefix}://{host}:{port}{path}"
                    
                    # Make a quick HEAD request to check if service is up
                    import requests
                    import urllib3
                    # Suppress SSL warnings
                    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                    
                    response = requests.head(url, timeout=5, verify=False)
                    if response.status_code < 500:  # Consider 2xx, 3xx, 4xx as "up"
                        return 'up'
                    else:
                        return 'down'
                except Exception as e:
                    logging.debug(f"Service status check failed for {host}:{port}: {e}")
                    return 'down'
            
            details_map = {}
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_id = {executor.submit(fetch_details, svc.get('id')): svc for svc in services_to_enrich if svc.get('id')}
                for future in as_completed(future_to_id):
                    svc = future_to_id[future]
                    details = future.result()
                    if details:
                        details_map[svc['id']] = details
                        
            # Check service statuses in parallel
            status_map = {}
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_service = {}
                for svc in services_to_enrich:
                    svc_id = svc.get('id')
                    if svc_id and svc_id in details_map:
                        details = details_map[svc_id]
                        future = executor.submit(check_service_status, details)
                        future_to_service[future] = svc_id
                
                for future in as_completed(future_to_service):
                    svc_id = future_to_service[future]
                    status = future.result()
                    status_map[svc_id] = status
                        
            for svc in services_to_enrich:
                svc_enriched = dict(svc)
                
                # Debug: Log the service structure
                logging.debug(f"Service {svc.get('id')} raw data: {svc}")
                
                # Try multiple possible field names for site/node identification
                possible_site_keys = [
                    svc.get('node'),
                    svc.get('site'), 
                    svc.get('site_name'),
                    svc.get('site_id'),
                    svc.get('parent_site'),
                    svc.get('location'),
                    svc.get('parent_node_name'),
                    svc.get('parent_site_name')
                ]
                
                # Also try from details if available
                svc_id = svc.get('id')
                if svc_id and svc_id in details_map:
                    details = details_map[svc_id]
                    logging.debug(f"Service {svc_id} details: {details}")
                    possible_site_keys.extend([
                        details.get('node'),
                        details.get('site'),
                        details.get('site_name'),
                        details.get('site_id'),
                        details.get('parent_site'),
                        details.get('location'),
                        details.get('parent_node_name'),
                        details.get('parent_site_name')
                    ])
                
                # Remove None values and try to find a match
                possible_site_keys = [key for key in possible_site_keys if key is not None]
                
                logging.debug(f"Service {svc.get('id')} possible site keys: {possible_site_keys}")
                
                site_info = None
                matched_key = None
                
                # Try exact match first
                for key in possible_site_keys:
                    if key in node_lookup:
                        site_info = node_lookup[key]
                        matched_key = key
                        break
                
                # If no exact match, try case-insensitive match
                if not site_info:
                    for key in possible_site_keys:
                        if key.lower() in node_lookup:
                            site_info = node_lookup[key.lower()]
                            matched_key = key
                            break
                
                if site_info:
                    svc_enriched['site'] = site_info.get('name')
                    svc_enriched['site_name'] = site_info.get('name')
                    svc_enriched['node'] = site_info.get('node')
                    logging.debug(f"Service {svc.get('id')} matched to site {site_info.get('name')} via key '{matched_key}'")
                else:
                    # Log what we tried to match
                    logging.debug(f"Service {svc.get('id')} could not be matched to any site. Tried keys: {possible_site_keys}")
                    # Use the first available key as fallback
                    fallback_site = possible_site_keys[0] if possible_site_keys else 'Unknown'
                    svc_enriched['site'] = fallback_site
                    svc_enriched['site_name'] = fallback_site
                    svc_enriched['node'] = fallback_site
                
                if svc_id and svc_id in details_map:
                    details = details_map[svc_id]
                    for k in ['host', 'port', 'path', 'prefix', 'assoc_storage_id', 'parent_compute_id']:
                        if k in details:
                            svc_enriched[k] = details[k]
                    
                    # Add real-time status
                    if svc_id in status_map:
                        svc_enriched['status'] = status_map[svc_id]
                        svc_enriched['real_time_status'] = True
                    else:
                        svc_enriched['status'] = svc.get('status', 'unknown')
                        svc_enriched['real_time_status'] = False
                else:
                    svc_enriched['status'] = svc.get('status', 'unknown')
                    svc_enriched['real_time_status'] = False
                    
                enriched.append(svc_enriched)
            return enriched
        except Exception as e:
            logging.error(f"Error listing enriched services: {e}")
            raise 