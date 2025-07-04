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
    from ska_src_clients.common.exceptions import CustomException
except ImportError:
    # Fallback for development - adjust the path to point to the correct location
    sys.path.append(os.path.join(os.path.dirname(__file__), '../../../ska-src-clients/src'))
    from ska_src_clients.session.oidc import OIDCSession
    from ska_src_clients.api import DataAPI, SiteAPI, MetadataAPI
    from ska_src_clients.common.utility import load_config
    from ska_src_clients.common.exceptions import CustomException


class SRCClientService:
    """Service class to interact with SKA SRC CLI tools."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize the SRC client service."""
        self.config = load_config([config_path] if config_path else None)
        if not self.config:
            raise ValueError("No valid config file found")
        
        self.session = OIDCSession(config=self.config)
        self.session.load_tokens_from_disk()
        
        # Initialize API clients
        self.data_api = DataAPI(session=self.session)
        self.site_api = SiteAPI(session=self.session)
        self.metadata_api = MetadataAPI(session=self.session)
    
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
    
    def complete_token_request(self, device_code: str) -> Dict[str, Any]:
        """Complete a token request by polling for completion."""
        try:
            # Poll for token completion
            success = False
            max_attempts = 60
            wait_between_polling_s = 5
            
            for attempt in range(0, max_attempts):
                try:
                    # This will raise an exception if authorization is still pending
                    result = self.session.request_token(device_code=device_code)
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
    
    def exchange_token(self, service_name: str, version: str = "latest") -> bool:
        """Exchange token for a specific service."""
        try:
            result = self.session.exchange_token(service_name, version=version)
            return result
        except Exception as e:
            logging.error(f"Error exchanging token for {service_name}: {e}")
            raise
    
    def list_tokens(self) -> List[Dict[str, Any]]:
        """List all available access tokens."""
        try:
            result = self.session.list_access_tokens()
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
                    "access_token": data.get("access_token", "")[:20] + "...",
                    "expires_utc": expires_dt_utc,
                    "expires_local": expires_dt_local,
                    "path_on_disk": data.get("path_on_disk"),
                    "has_refresh_token": bool(data.get("has_associated_refresh_token"))
                })
            
            return reformatted
        except Exception as e:
            logging.error(f"Error listing tokens: {e}")
            raise
    
    def inspect_token(self, service_name: str) -> Dict[str, Any]:
        """Inspect a specific access token."""
        try:
            result = self.session.inspect_access_token(service_name)
            return result
        except Exception as e:
            logging.error(f"Error inspecting token for {service_name}: {e}")
            raise
    
    def download_data(self, namespace: str, name: str, sort: str = "nearest_by_ip", 
                     ip_address: str = "", no_verify: bool = False, output: Optional[str] = None) -> Dict[str, Any]:
        """Download data by namespace and name."""
        try:
            result = self.data_api.download(namespace, name, sort, ip_address, not no_verify, output)
            return result
        except Exception as e:
            logging.error(f"Error downloading data: {e}")
            raise
    
    def locate_data(self, namespace: str, name: str, sort: str = "nearest_by_ip", 
                   ip_address: str = "") -> Dict[str, Any]:
        """Locate data by namespace and name."""
        try:
            result = self.data_api.locate(namespace, name, sort, ip_address)
            return result
        except Exception as e:
            logging.error(f"Error locating data: {e}")
            raise
    
    def list_files(self, namespace: str, name: str, detail: bool = False, 
                  filters: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        """List files in a namespace."""
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
        try:
            result = self.data_api.move_request(to_storage_area_id, dids, lifetime, parent_namespace)
            return result
        except Exception as e:
            logging.error(f"Error making move request: {e}")
            raise
    
    def move_status(self, job_id: str) -> Dict[str, Any]:
        """Get the status of a data movement request."""
        try:
            result = self.data_api.move_status(job_id)
            return result
        except Exception as e:
            logging.error(f"Error getting move status: {e}")
            raise
    
    def stage_request(self, to_storage_area_id: str, dids: List[str], lifetime: str, 
                     parent_namespace: Optional[str] = None) -> Dict[str, Any]:
        """Make a data staging request."""
        try:
            result = self.data_api.stage_request(to_storage_area_id, dids, lifetime, parent_namespace)
            return result
        except Exception as e:
            logging.error(f"Error making stage request: {e}")
            raise
    
    def stage_status(self, job_id: str) -> Dict[str, Any]:
        """Get the status of a data staging request."""
        try:
            result = self.data_api.stage_status(job_id)
            return result
        except Exception as e:
            logging.error(f"Error getting stage status: {e}")
            raise
    
    def list_namespaces(self) -> List[Dict[str, Any]]:
        """List available namespaces."""
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
        try:
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