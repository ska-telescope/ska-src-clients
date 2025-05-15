import importlib
import json
import logging
import os
import random
import tempfile
import uuid

from ska_src_clients.common.exceptions import ExtraMetadataKeyConflict, MetadataKeyConflict, \
                                               CustomException, handle_client_exceptions
from ska_src_clients.common.utility import url_to_parts
from ska_src_clients.plan.plan import UploadPlan
from ska_src_clients.api.api import API


class DataAPI(API):
    """ Data API class. """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @handle_client_exceptions
    def move_request(self, to_storage_area_uuid: str | None,
             dids: list | None,
             lifetime: str | None,
             parent_namespace: str | None) -> str:
        """Make a data movement request.

        :param to_storage_area_uuid: The storage area uuid to move data to.
        :param int lifetime: The lifetime of the data in seconds.
        :param str parent_namespace: The parent container namespace. Defaults to using the first DID's namespace
            (Rucio only).
        :param list dids: The list of DIDs to move.

        :return: A requests response.
        :rtype: requests.models.Response
        """
        dids_formatted = [
            {'namespace': entry.split(':')[0].strip(), 'name': entry.split(':')[1].strip()}
            for entry in dids
        ]
        dm_client = self.session.client_factory.get_data_management_client(is_authenticated=True)
        return dm_client.make_data_movement_request(to_storage_area_uuid=to_storage_area_uuid,
                                                    lifetime=lifetime,
                                                    parent_namespace=parent_namespace,
                                                    dids=dids_formatted).json()

    @handle_client_exceptions
    def move_status(self, job_id: str | None) -> str:
        """Get the status of a data movement request.

        :param job_id: The job id.
        :return: A requests response.
        :rtype: requests.models.Response
        """
        dm_client = self.session.client_factory.get_data_management_client(is_authenticated=True)
        return dm_client.get_status_data_movement_request(job_id).json()

    @handle_client_exceptions
    def stage_request(self, to_storage_area_uuid: str | None,
             dids: list | None,
             lifetime: str | None,
             parent_namespace: str | None) -> str:
        """Make a data staging request.

        :param to_storage_area_uuid: The storage area uuid to stage data in.
        :param int lifetime: The lifetime of the data in seconds.
        :param str parent_namespace: The parent container namespace. Defaults to using the first DID's namespace
            (Rucio only).
        :param list dids: The list of DIDs to stage.

        :return: A requests response.
        :rtype: requests.models.Response
        """
        dids_formatted = [
            {'namespace': entry.split(':')[0].strip(), 'name': entry.split(':')[1].strip()}
            for entry in dids
        ]
        dm_client = self.session.client_factory.get_data_management_client(is_authenticated=True)
        return dm_client.make_data_stage_request(to_storage_area_uuid=to_storage_area_uuid,
                                                 lifetime=lifetime,
                                                 parent_namespace=parent_namespace,
                                                 dids=dids_formatted).json()

    @handle_client_exceptions
    def stage_status(self, job_id: str | None) -> str:
        """Get the status of a data staging request.

        :param job_id: The job id.
        :return: A requests response.
        :rtype: requests.models.Response
        """
        dm_client = self.session.client_factory.get_data_management_client(is_authenticated=True)
        return dm_client.get_status_data_stage_request(job_id).json()

    @handle_client_exceptions
    def download(self, namespace, name, sort='nearest_by_ip', ip_address=None,  verify=True, output_filename=None):
        """ Locate replicas of data identifier, sort by some algorithm and download.

        :param str namespace: The data identifier's namespace.
        :param str name: The data identifier's name.
        :param str sort: The sorting algorithm to use (random || nearest_by_ip)
        :param str ip_address: The ip address to geolocate the nearest replica to. Leave blank to use the requesting
            client ip (sort == nearest_by_ip only)
        :param bool verify: Verify a server's SSL certificate.
        :param str output_filename: The output filename (defaults to remote filename).
        """
        # query DM API to get a list of data replicas for this namespace/name and pick the first
        dm_client = self.session.client_factory.get_data_management_client(is_authenticated=True)
        replica_locations = dm_client.locate_replicas_of_file(
            namespace=namespace, name=name, sort=sort, ip_address=ip_address).json()

        # pick the first replica from the first site in the response (ordered if sorting algorithm is used)
        position = 0
        replicas = replica_locations[position].get('replicas')
        associated_storage_area_id = replica_locations[position].get('associated_storage_area_id')

        # pick a random replica from this site (only relevant if multiple exist)
        access_url = random.choice(replicas)

        # split the access_url into prefix, host, port, path and filename
        access_url_parts = url_to_parts(access_url)
        remote_filename = os.path.basename(access_url_parts.get('path'))
        access_url_parts['path'] = os.path.dirname(access_url_parts.get('path'))   # change path to not include filename

        # get the storage read access token for the associated storage area
        access_token = dm_client.get_download_token_for_namespace_name(storage_area_uuid=associated_storage_area_id,
            namespace=namespace, name=name).json().get('access_token')

        # instantiate a client for this protocol depending on prefix
        selected_dm_client_attr = self.session.config.get('data-management').get('clients').get(
            access_url_parts.get('prefix'))
        package_name = selected_dm_client_attr.get('package_name')
        module_name = selected_dm_client_attr.get('module_name')
        class_name = selected_dm_client_attr.get('class_name')
        module = importlib.import_module("{package_name}.{module_name}".format(
            package_name=package_name,
            module_name=module_name
        ))
        selected_dm_client = getattr(module, class_name)(**access_url_parts, access_token=access_token, verify=verify)

        # download data using this access token
        if not output_filename:
            output_filename = remote_filename

        # before we call download, create callable for progress updates
        def progress_update(current, total, name, *args):
            print("{}KB downloaded".format(round(os.path.getsize(name)/1024)), end='\r')

        selected_dm_client.download(
            progress=progress_update, progress_args=(output_filename,), from_remote_path=remote_filename,
            to_local_path=output_filename)
        print('\n')

    @handle_client_exceptions
    def list_files_in_namespace(self, namespace, name, detail, filters, limit):
        """ List files in a namespace.

        :param str namespace: The data namespace.
        :param str name: The data identifier name (wildcards allowed).
        :param bool detail:
        :param dict filters: Filters (Rucio only).
        :param int limit: Number of identifiers to return in result
        """
        client = self.session.client_factory.get_data_management_client(is_authenticated=True)
        return client.list_files_in_namespace(namespace=namespace, name=name, detail=detail, filters=filters,
                                              limit=limit).json()

    @handle_client_exceptions
    def list_namespaces(self):
        """ List namespace. """
        client = self.session.client_factory.get_data_management_client(is_authenticated=True)
        return client.list_namespaces().json()

    @handle_client_exceptions
    def list_rules_for_namespace(self, namespace, datetime_from, datetime_to, limit):
        """ List rules for a namespace. """
        client = self.session.client_factory.get_data_management_client(is_authenticated=True)
        return client.list_rules_for_namespace(namespace=namespace, datetime_from=datetime_from,
                                               datetime_to=datetime_to, limit=limit).json()

    @handle_client_exceptions
    def list_rules_for_data_identifier(self, namespace, name):
        """ List rules for a data identifier. """
        client = self.session.client_factory.get_data_management_client(is_authenticated=True)
        return client.list_rules_for_data_identifier(namespace=namespace, name=name).json()

    @handle_client_exceptions
    def locate(self, namespace, name, sort='nearest_by_ip', ip_address=None):
        """ Locate replicas of data identifier, sort by some algorithm and download.

        :param str namespace: The data identifier's namespace.
        :param str name: The data identifier's name.
        :param str sort: The sorting algorithm to use (random || nearest_by_ip)
        :param str ip_address: The ip address to geolocate the nearest replica to. Leave blank to use the requesting
            client ip (sort == nearest_by_ip only)
        """
        # query DM API to get a list of data replicas for this namespace/name and pick the first
        client = self.session.client_factory.get_data_management_client(is_authenticated=True)
        replicas_with_site = client.locate_replicas_of_file(
            namespace=namespace, name=name, sort=sort, ip_address=ip_address)
        replicas = []
        for entry in replicas_with_site.json():
            replicas = replicas + entry.get('replicas', [])
        return replicas

    @handle_client_exceptions
    def upload_for_ingest(self, path, ingest_service_id, namespace, metadata_suffix='meta', extra_metadata={},
                          protocol_prefix='https', verify=True, debug=False):
        """ Upload data for ingestion.

        :param str path: Local path to data directory to be uploaded.
        :param str ingest_service_id: The ingest service id
        :param str namespace: The data namespace.
        :param str metadata_suffix: The expected metadata suffix.
        :param str extra_metadata: Extra metadata to apply to each file (JSON).
        :param str protocol_prefix: The protocol prefix to use.
        :param bool verify: Verify a server's SSL certificate.
        :param bool debug: Debug mode?
        """
        reserved_metadata_keys = ['namespace', 'ingest_service_id']

        # load extra metadata and check for conflicts
        extra_metadata = json.loads(extra_metadata)
        for key in reserved_metadata_keys:
            if extra_metadata.get(key):
                raise ExtraMetadataKeyConflict(key)

        # get the ingestion storage area host and path
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        data_ingest_service = client.get_service(service_id=ingest_service_id).json()
        associated_storage_area = client.get_storage_area(data_ingest_service.get('associated_storage_area_id')).json()
        associated_storage = client.get_storage(associated_storage_area.get('associated_storage_id')).json()

        base_path = associated_storage.get('base_path')
        relative_path = associated_storage_area.get('relative_path')
        remote_path = os.path.join(base_path, relative_path.lstrip('/'))
        host = associated_storage.get('host')

        # select a storage access protocol (either by choice or randomly)
        selected_protocol = None
        if protocol_prefix:
            for supported_protocol in associated_storage.get('supported_protocols', []):
                if supported_protocol.get('prefix') == protocol_prefix:
                    selected_protocol = supported_protocol
                    break
        else:
            selected_protocol = random.choice(associated_storage.get('supported_protocols', []))
        prefix = selected_protocol.get('prefix')
        port = selected_protocol.get('port')

        # get a token
        client = self.session.client_factory.get_data_management_client(is_authenticated=True)
        access_token = client.get_upload_token_ingest_for_namespace(
            data_ingest_service_uuid=ingest_service_id,
            namespace=namespace).json().get('access_token')

        # instantiate a client for this protocol depending on prefix.
        # this uses the storage area's absolute path (should be the complete path to the staging area)
        selected_dm_client_attr = self.session.config.get('data-management').get('clients').get(prefix)
        package_name = selected_dm_client_attr.get('package_name')
        module_name = selected_dm_client_attr.get('module_name')
        class_name = selected_dm_client_attr.get('class_name')
        module = importlib.import_module("{package_name}.{module_name}".format(
            package_name=package_name,
            module_name=module_name
        ))
        selected_dm_client = getattr(module, class_name)(prefix=prefix, host=host, port=port, path=remote_path,
                                                         access_token=access_token, verify=verify)

        # make an upload plan (so it can be saved, continued etc.)
        plan = UploadPlan()

        # add step for making namespace directory in staging area
        plan.append_step(section_name='upload', fqn=selected_dm_client.mkdir, arguments={
            'remote_path': namespace
        })
        
        # add step to make unique directory for ingest
        ingest_directory_id = str(uuid.uuid4())
        remote_ingest_directory = os.path.join(namespace, ingest_directory_id)
        plan.append_step(section_name='upload', fqn=selected_dm_client.mkdir, arguments={
            'remote_path': remote_ingest_directory
        })
        logging.info("Ingest directory: {}".format(remote_ingest_directory))

        # go through each directory and add steps to upload data and metadata to plan
        for (root, dirs, files) in os.walk(path, topdown=True):
            if os.path.relpath(root, path) == '.':
                this_root_relpath = remote_ingest_directory
            else:
                this_root_relpath = os.path.join(remote_ingest_directory, os.path.relpath(root, path))
                # add step for making this directory (if it doesn't exist)
                plan.append_step(section_name='upload', fqn=selected_dm_client.mkdir, arguments={
                    'remote_path': this_root_relpath
                })

            # separate out data and metadata files
            files_fullpaths = set([os.path.join(root, f) for f in files])
            files_metadata = set([f for f in files_fullpaths if f.endswith(metadata_suffix)])
            files_data = set([f for f in files_fullpaths - files_metadata])

            # add steps for uploading data
            for file in files_data:
                plan.append_step(section_name='upload', fqn=selected_dm_client.upload, arguments={
                    'from_local_path': file,
                    'to_remote_path': os.path.join(this_root_relpath, os.path.basename(file))
                })

            # add steps for uploading metadata (uses temporary file to add extra metadata if requested)
            for file in files_metadata:
                # load metadata file from disk and check for key conflicts
                with open(file, 'r') as metadata_file:
                    metadata = json.loads(metadata_file.read())
                    for key in reserved_metadata_keys:
                        if metadata.get(key):
                            raise MetadataKeyConflict(key)

                # construct temporary metadata file with extra metadata
                with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp_metadata_file:
                    tmp_metadata_file.write(json.dumps(
                        {
                            'namespace': namespace,
                            'ingest_service_id': ingest_service_id,
                            **metadata,
                            **extra_metadata
                        }
                    ))
                    plan.append_step(section_name='upload', fqn=selected_dm_client.upload, arguments={
                        'from_local_path': tmp_metadata_file.name,
                        'to_remote_path': os.path.join(this_root_relpath, os.path.basename(file))
                    })
                    plan.append_step(section_name='upload', fqn=os.unlink, arguments={
                        'path': tmp_metadata_file.name,
                    })

        if debug:
            plan.describe()
        plan.run(section_name='upload')
