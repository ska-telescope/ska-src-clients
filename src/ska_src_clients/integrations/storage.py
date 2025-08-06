import os
import requests
import urllib3
import time
import random
from webdav3.client import Client as WebDAVClient

from abc import ABC
from ..common.exceptions import StorageUploadFailed, StorageDownloadFailed, StorageListFailed


class StorageInterface(ABC):
    """ Abstract base class for storage interfaces. """
    def list(self, remote_path):
        pass

    def mkdir(self, remote_path):
        pass

    def upload(self, from_local_path, to_remote_path):
        pass

    def download(self, progress, progress_args, from_remote_path, to_local_path):
        pass


class WebDAVStorageClient(StorageInterface):
    """ A WebDAV storage protocol using webdav3.client library. """
    
    def __init__(self, prefix, host, port, path, access_token=None, verify=True, **kwargs):
        if prefix == 'davs':
            prefix = 'https'
        
        # Use the original hostname directly since we have proper host mapping
        # For WebDAV, we need to construct the URL without the path
        webdav_url = "{prefix}://{host}:{port}".format(
            prefix=prefix,
            host=host,  # Use original hostname (storm1.local, storm2.local)
            port=port)
        
        print(f"DEBUG: WebDAV URL: {webdav_url}")
        print(f"DEBUG: Host: {host}")
        print(f"DEBUG: Base path: {path}")
        print(f"DEBUG: SSL verify: {verify}")
        print(f"DEBUG: Access token provided: {access_token is not None}")
        if access_token:
            print(f"DEBUG: Access token preview: {access_token[:20]}...")
        
        # Configure SSL certificates for the WebDAV client
        ssl_config = self._configure_ssl_certificates(verify)
        
        # Configure webdavclient3
        webdav_options = {
            'webdav_hostname': webdav_url,
            'webdav_login': None,  # We'll use token authentication
            'webdav_password': None,
            'webdav_timeout': 300,  # Increased timeout to 5 minutes for large files
            'webdav_verbose': True,
            'webdav_verify': verify,  # Use the verify parameter
        }
        
        # Add SSL configuration if available
        if ssl_config:
            webdav_options.update(ssl_config)
            print(f"DEBUG: SSL configuration applied: {ssl_config}")
        
        # Create the client first
        self.client = WebDAVClient(webdav_options)
        
        # Configure SSL context on the session if available
        if hasattr(self.client, 'session') and self.client.session:
            self._configure_session_ssl(self.client.session, verify)
        
        # Try to set the Authorization header directly on the session
        if access_token and hasattr(self.client, 'session'):
            self.client.session.headers.update({
                'Authorization': f'Bearer {access_token}'
            })
            print(f"DEBUG: Set Authorization header on session: Bearer {access_token[:20]}...")
        elif access_token:
            print(f"DEBUG: WARNING: Could not set Authorization header - no session attribute found")
        
        self.base_path = path.lstrip('/')
    
    def _configure_ssl_certificates(self, verify):
        """Configure SSL certificates for the WebDAV client."""
        ssl_config = {}
        
        try:
            import os
            
            # Check for environment variables set by the main application
            ssl_cert_file = os.environ.get('SSL_CERT_FILE')
            ssl_cert_dir = os.environ.get('SSL_CERT_DIR')
            requests_ca_bundle = os.environ.get('REQUESTS_CA_BUNDLE')
            
            print(f"DEBUG: SSL_CERT_FILE: {ssl_cert_file}")
            print(f"DEBUG: SSL_CERT_DIR: {ssl_cert_dir}")
            print(f"DEBUG: REQUESTS_CA_BUNDLE: {requests_ca_bundle}")
            
            # Use the most specific certificate configuration available
            if requests_ca_bundle and os.path.exists(requests_ca_bundle):
                ssl_config['webdav_verify'] = requests_ca_bundle
                print(f"DEBUG: Using REQUESTS_CA_BUNDLE: {requests_ca_bundle}")
            elif ssl_cert_file and os.path.exists(ssl_cert_file):
                ssl_config['webdav_verify'] = ssl_cert_file
                print(f"DEBUG: Using SSL_CERT_FILE: {ssl_cert_file}")
            elif ssl_cert_dir and os.path.exists(ssl_cert_dir):
                # For directory-based certificates, we need to handle this differently
                # as webdav3 doesn't directly support capath
                print(f"DEBUG: SSL_CERT_DIR available: {ssl_cert_dir}")
                # We'll handle this in the session configuration
            else:
                print(f"DEBUG: No SSL certificate configuration found, using verify={verify}")
                ssl_config['webdav_verify'] = verify
            
        except Exception as e:
            print(f"DEBUG: Error configuring SSL certificates: {e}")
            ssl_config['webdav_verify'] = verify
        
        return ssl_config
    
    def _configure_session_ssl(self, session, verify):
        """Configure SSL context on the requests session."""
        try:
            import ssl
            import os
            
            # Create a custom SSL context
            ssl_context = ssl.create_default_context()
            
            # Set certificate verification
            if not verify:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                print("DEBUG: SSL verification disabled")
            else:
                # Try to load certificates from environment variables
                ssl_cert_file = os.environ.get('SSL_CERT_FILE')
                ssl_cert_dir = os.environ.get('SSL_CERT_DIR')
                
                if ssl_cert_file and os.path.exists(ssl_cert_file):
                    ssl_context.load_verify_locations(cafile=ssl_cert_file)
                    print(f"DEBUG: Loaded SSL certificates from file: {ssl_cert_file}")
                elif ssl_cert_dir and os.path.exists(ssl_cert_dir):
                    ssl_context.load_verify_locations(capath=ssl_cert_dir)
                    print(f"DEBUG: Loaded SSL certificates from directory: {ssl_cert_dir}")
                else:
                    print("DEBUG: Using default SSL context")
            
            # Apply the SSL context to the session
            if hasattr(session, 'mount'):
                from urllib3.util.ssl_ import create_urllib3_context
                adapter = session.adapters.get('https://')
                if adapter:
                    adapter.poolmanager.connection_pool_kw['ssl_context'] = ssl_context
                    print("DEBUG: Applied custom SSL context to session")
            
        except Exception as e:
            print(f"DEBUG: Error configuring session SSL: {e}")
            # Fall back to default behavior
            pass

    def download(self, progress, progress_args, from_remote_path, to_local_path):
        try:
            print(f"DEBUG: Downloading from {from_remote_path} to {to_local_path}")
            self.client.download_sync(remote_path=from_remote_path, local_path=to_local_path)
        except Exception as e:
            print(f"DEBUG: Download failed: {e}")
            raise StorageDownloadFailed(e)

    def list(self, remote_path):
        try:
            print(f"DEBUG: Listing remote path: {remote_path}")
            result = self.client.list(remote_path)
            print(f"DEBUG: List result: {result}")
            return result
        except Exception as e:
            print(f"DEBUG: List failed: {e}")
            raise StorageListFailed(e)

    def mkdir(self, remote_path):
        """ Make directory at a remote path.
        
        :param str remote_path: The remote path.
        """
        print(f"DEBUG: mkdir called with remote_path: {remote_path}")
        
        # Construct the full path by combining base_path with remote_path
        full_path = os.path.join(self.base_path, remote_path.lstrip('/')).replace('\\', '/')
        print(f"DEBUG: Full path for mkdir: {full_path}")
        
        # Create directories recursively
        path_parts = full_path.strip('/').split('/')
        current_path = ""
        
        for part in path_parts:
            if current_path:
                current_path = f"{current_path}/{part}"
            else:
                current_path = part
            
            print(f"DEBUG: Creating directory part: {current_path}")
            self._create_directory(current_path)
    
    def _create_directory(self, remote_path):
        """Create a directory using webdavclient3. If that fails, skip directory creation."""
        try:
            print(f"DEBUG: Attempting to create directory: {remote_path}")
            self.client.mkdir(remote_path)
            print(f"DEBUG: Successfully created directory: {remote_path}")
        except Exception as e:
            print(f"DEBUG: Directory creation failed: {e}")
            
            # If it's a 409 Conflict, the directory already exists - that's fine
            if "409" in str(e) or "Conflict" in str(e) or "already exists" in str(e).lower():
                print(f"DEBUG: Directory {remote_path} already exists, continuing...")
                return
            
            # For any other error, log it but continue (don't fail the upload)
            print(f"DEBUG: Directory {remote_path} creation failed, but continuing upload attempt")
            return

    def upload(self, from_local_path, to_remote_path):
        try:
            print(f"DEBUG: Uploading from {from_local_path} to {to_remote_path}")
            
            # Construct the full path by combining base_path with to_remote_path
            full_remote_path = os.path.join(self.base_path, to_remote_path.lstrip('/')).replace('\\', '/')
            print(f"DEBUG: Full remote path for upload: {full_remote_path}")
            
            # Ensure the target directory exists
            target_dir = os.path.dirname(full_remote_path)
            if target_dir:
                # For the target directory, we need to use the relative path for mkdir
                relative_target_dir = os.path.dirname(to_remote_path)
                if relative_target_dir:
                    self.mkdir(relative_target_dir)
            
            # Upload the file
            self.client.upload_sync(remote_path=full_remote_path, local_path=from_local_path)
            print(f"DEBUG: Successfully uploaded {from_local_path} to {full_remote_path}")
        except Exception as e:
            print(f"DEBUG: Upload failed: {e}")
            raise StorageUploadFailed(e)
