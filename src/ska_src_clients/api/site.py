import fnmatch
import functools
import logging
from fastapi.exceptions import HTTPException

from ska_src_clients.api.api import API
from ska_src_clients.common.exceptions import handle_client_exceptions


class SiteAPI(API):
    """ Site API class. """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @handle_client_exceptions
    def disable_service(self, service_uuid):
        """ Disable a service by service uuid.

        :param str service_uuid: The service uuid.
        """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.set_service_disabled(service_uuid=service_uuid)

    @handle_client_exceptions
    def enable_service(self, service_id):
        """ Enable a service by service uuid.

        :param str service_uuid: The service uuid.

        """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.set_service_enabled(service_uuid=service_uuid)

    @handle_client_exceptions
    def get_add_node_www_url(self):
        """ Get the add site www URL. """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.get_add_node_www_url()

    @handle_client_exceptions
    def get_edit_node_www_url(self, node_name):
        """ Get the edit site www URL.

        :param str site_name: The site name.
        """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.get_edit_node_www_url(node_name=node_name)

    @handle_client_exceptions
    def get_site(self, site_id):
        """ Get description of a site.

        :param str site_id: The site uuid.
        """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.get_site_from_id(site_id=site_id).json()

    @handle_client_exceptions
    def get_compute(self, compute_id):
        """ Get description of a compute element.

        :param str compute_id: The compute element uuid.
        """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.get_compute(compute_id=compute_id).json()

    @handle_client_exceptions
    def get_service(self, service_id):
        """ Get description of a service.
        
        :param str service_id: The service uuid.
        """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.get_service(service_id=service_id).json()

    @handle_client_exceptions
    def get_storage(self, storage_id):
        """ Get description of a storage resource.

        :param str storage_id: The storage uuid.
        """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.get_storage(storage_id=storage_id).json()

    @handle_client_exceptions
    def get_storage_area(self, storage_area_id):
        """ Get description of a storage area resource.

        :param str storage_area_id: The storage area uuid.
        """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.get_storage_area(storage_area_id=storage_area_id).json()

    @handle_client_exceptions
    def list_compute(self, node_name=None, site_name=None):
        """ List compute elements across all sites. """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        query_params = {}
        if node_name:
            query_params["node_names"] = [node_name]
        if site_name:
            query_params["site_names"] = [site_name]
        return client.list_compute(**query_params).json()

    @handle_client_exceptions
    def list_services(self, service_type=None, node_name=None, site_name=None, scope='all'):
        """ List services across all sites, with optional filters.

        :param str service_type: (Optional) Filter services by type (e.g., 'jupyterhub', 'soda_sync').
        :param str site_name: (Optional) Filter services belonging to a specific site.
        :param str node_name: (Optional) Filter services belonging to a specific node.
        :param str scope: (Optional) Filter by service scope ('all'|'local'|'global').
        """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)

        query_params = {"service_scope": scope}
        if service_type:
            query_params["service_types"] = [service_type]
        if node_name:
            query_params["node_names"] = [node_name]
        if site_name:
            query_params["site_names"] = [site_name]

        return client.list_services(**query_params).json()

    @handle_client_exceptions
    def list_service_types(self):
        """ List supported service types. """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        # services can be "core" or associated with a compute element, use combined result
        return client.list_service_types().json()

    @handle_client_exceptions
    def list_sites(self):
        """ List sites. """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.list_sites().json()

    @handle_client_exceptions
    def list_storages(self, node_name=None, site_name=None):
        """ List storages across all sites. """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        query_params = {}
        if node_name:
            query_params["node_names"] = [node_name]
        if site_name:
            query_params["site_names"] = [site_name]
        return client.list_storages(**query_params).json()

    @handle_client_exceptions
    def list_storage_areas(self, node_name=None, site_name=None):
        """ List storage areas across all sites. """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        query_params = {}
        if node_name:
            query_params["node_names"] = [node_name]
        if site_name:
            query_params["site_names"] = [site_name]
        return client.list_storage_areas(**query_params).json()

    @handle_client_exceptions
    def list_storage_areas_topojson(self):
        """ List storage areas across all sites in topojson format. """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.list_storage_areas_topojson().json()
