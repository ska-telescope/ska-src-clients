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
        webdav_url = "{prefix}://{host}:{port}/{path}".format(
            prefix=prefix,
            host=host,  # Use original hostname (storm1.local, storm2.local)
            port=port,
            path=path.lstrip('/'))
        
        print(f"DEBUG: WebDAV URL: {webdav_url}")
        print(f"DEBUG: Host: {host}")
        print(f"DEBUG: Base path: {path}")
        print(f"DEBUG: SSL verify: {verify}")
        print(f"DEBUG: Access token provided: {access_token is not None}")
        if access_token:
            print(f"DEBUG: Access token preview: {access_token[:20]}...")
        
        # Configure webdavclient3
        webdav_options = {
            'webdav_hostname': webdav_url,
            'webdav_login': None,  # We'll use token authentication
            'webdav_password': None,
            'webdav_timeout': 300,  # Increased timeout to 5 minutes for large files
            'webdav_verbose': True,
            'webdav_verify': verify,  # Use the verify parameter
        }
        
        # Create the client first
        self.client = WebDAVClient(webdav_options)
        
        # Try to set the Authorization header directly on the session
        if access_token and hasattr(self.client, 'session'):
            self.client.session.headers.update({
                'Authorization': f'Bearer {access_token}'
            })
            print(f"DEBUG: Set Authorization header on session: Bearer {access_token[:20]}...")
        elif access_token:
            print(f"DEBUG: WARNING: Could not set Authorization header - no session attribute found")
        
        self.base_path = path.lstrip('/')

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
        
        # Create directories recursively
        path_parts = remote_path.strip('/').split('/')
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
            
            # Ensure the target directory exists
            target_dir = os.path.dirname(to_remote_path)
            if target_dir:
                self.mkdir(target_dir)
            
            # Upload the file
            self.client.upload_sync(remote_path=to_remote_path, local_path=from_local_path)
            print(f"DEBUG: Successfully uploaded {from_local_path} to {to_remote_path}")
        except Exception as e:
            print(f"DEBUG: Upload failed: {e}")
            raise StorageUploadFailed(e)
