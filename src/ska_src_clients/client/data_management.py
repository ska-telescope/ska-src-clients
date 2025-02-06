import os

from abc import ABC
from webdav3.client import Client
from webdav3.exceptions import WebDavException

from ska_src_clients.common.exceptions import StorageDownloadFailed, StorageListFailed, StorageUploadFailed


class DataManagementClient(ABC):
    def list(self, remote_path):
        """ List files at a remote path.

        :param str remote_path: The remote path.
        """
        raise NotImplementedError

    def mkdir(self, remote_path):
        """ Make directory at a remote path.

        :param str remote_path: The remote path.
        """
        raise NotImplementedError

    def upload(self, from_local_path, to_remote_path):
        """ Upload files from a local path to a remote path.

        :param str from_local_path: The local path to upload from.
        :param str to_remote_path: The remote path to upload to.
        """
        raise NotImplementedError


class WebDAVClient3(DataManagementClient):
    """ A WebDAV client based on the webdav-client-python3 library. """
    def __init__(self, prefix, host, port, path, access_token=None, verify=True, **kwargs):
        # Translate davs prefix -> https
        if prefix == 'davs':
            prefix = 'https'
        options = {
            'webdav_hostname': "{prefix}://{host}:{port}/{path}".format(
                prefix=prefix,
                host=host,
                port=port,
                path=path.lstrip('/')),
            'webdav_token': access_token
        }
        self.client = Client(options)
        self.client.verify = verify

    def download(self, progress, progress_args, from_remote_path, to_local_path):
        """ Download files from a remote path to a local path.

         :param progress: Pass a callback function to track file download progress. The function must take current and
            total as positional arguments and will be called back each time a new file chunk has been successfully
            transmitted.
        :param progress_args: Optional args to be passed to progress callable.
        :param str from_remote_path: The remote path to download from.
        :param str to_local_path: The local path to download to.
        """
        try:
            self.client.download_sync(progress=progress, progress_args=progress_args, remote_path=from_remote_path,
                                      local_path=to_local_path)
        except WebDavException as e:
            raise StorageDownloadFailed(e)

    def list(self, remote_path):
        """ List files at a remote path.

        :param str remote_path: The remote path.
        """
        try:
            self.client.list(remote_path)
        except WebDavException as e:
            raise StorageListFailed(e)

    def mkdir(self, remote_path):
        """ Make directory at a remote path.

        :param str remote_path: The remote path.
        """
        try:
            self.client.mkdir(remote_path=remote_path)
        except WebDavException as e:
            raise StorageListFailed(e)

    def upload(self, from_local_path, to_remote_path):
        """ Upload files from a local path to a remote path.

        :param str from_local_path: The local path to upload from.
        :param str to_remote_path: The remote path to upload to.
        """
        try:
            if os.path.isdir(from_local_path):
                from_local_path = os.path.join(from_local_path, '')     # adds trailing slash
                to_remote_path = os.path.join(to_remote_path, '')       # adds trailing slash
            self.client.upload_sync(local_path=from_local_path, remote_path=to_remote_path)
        except WebDavException as e:
            raise StorageUploadFailed(e)
