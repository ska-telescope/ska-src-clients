import importlib
import json
import logging
import os
import tempfile
import uuid

from ska_src_clients.common.exceptions import ExtraMetadataKeyConflict, MetadataKeyConflict
from ska_src_clients.plan.plan import UploadPlan
from ska_src_clients.api.api import API


class DataAPI(API):
    """ Data API class. """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def list_ingest_services(self):
        """ Retrieve a list of ingest services (type==Data Ingest Area) from the site-capabilities API. """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        ingest_services = {}
        for entry in client.list_services().json():
            site_name = entry.get('site_name')
            services = entry.get('services', [])
            for service in services:
                if service.get('type', '') == "Data Ingest Area":
                    ingest_services[site_name] = {
                        'id': service.get('id'),
                        'prefix': service.get('prefix'),
                        'host': service.get('host'),
                        'port': service.get('port'),
                        'path': service.get('path')
                    }
        return ingest_services

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

    def list_namespaces(self):
        """ List namespace. """
        client = self.session.client_factory.get_data_management_client(is_authenticated=True)
        return client.list_namespaces().json()

    def upload(self, path, ingest_service_id, namespace, metadata_suffix, extra_metadata, debug=False):
        """ Upload data into a namespace.

        :param str path: Local path to data directory to be uploaded.
        :param str ingest_service_id: The ingest service id
        :param str namespace: The data namespace.
        :param str metadata_suffix: The expected metadata suffix.
        :param str extra_metadata: Extra metadata to apply to each file (JSON).
         """
        reserved_metadata_keys = ['namespace', 'ingest_service_id']

        # load extra metadata and check for conflicts
        extra_metadata = json.loads(extra_metadata)
        for key in reserved_metadata_keys:
            if extra_metadata.get(key):
                raise ExtraMetadataKeyConflict(key)

        # get the ingestion service protocols
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        service = client.get_service(service_id=ingest_service_id).json()

        # depending on prefix, select a suitable data management client
        prefix = service.get('prefix')

        # get a token
        client = self.session.client_factory.get_data_management_client(is_authenticated=True)
        access_token = client.get_upload_token_staging_for_namespace(
            data_ingest_service_uuid=ingest_service_id,
            namespace=namespace).json().get('access_token')

        # instantiate a client for this protocol depending on prefix
        selected_dm_client_attr = self.session.config.get('data-management').get('clients').get(prefix)
        package_name = selected_dm_client_attr.get('package_name')
        module_name = selected_dm_client_attr.get('module_name')
        class_name = selected_dm_client_attr.get('class_name')
        module = importlib.import_module("{package_name}.{module_name}".format(
            package_name=package_name,
            module_name=module_name
        ))
        selected_dm_client = getattr(module, class_name)(**service, access_token=access_token)

        # make an upload plan (so it can be saved, continued etc.)
        plan = UploadPlan()

        # add step for making namespace directory in root of ingest area
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

            # add steps for uploading metadata
            for file in files_metadata:
                # load metadata file from disk and check for conflicts
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
