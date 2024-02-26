import fnmatch

from ska_src_clients.api.api import API


class SiteAPI(API):
    """ Site API class. """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_add_site_www_url(self):
        """ Get the add site www URL. """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.get_add_site_www_url()

    def get_edit_site_www_url(self, site_name):
        """ Get the edit site www URL.

        :param str site_name: The site name.
        """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.get_edit_site_www_url(site_name=site_name)

    def get_compute(self, compute_id):
        """ Get description of a compute element.

        :param str compute_id: The compute element uuid.
        """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.get_compute(compute_id=compute_id).json()

    def get_service(self, service_id):
        """ Get description of a service.
        
        :param str service_id: The service uuid.
        """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.get_service(service_id=service_id).json()

    def get_storage(self, storage_id):
        """ Get description of a storage resource.

        :param str storage_id: The storage uuid.
        """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.get_storage(storage_id=storage_id).json()

    def get_storage_area(self, storage_area_id):
        """ Get description of a storage area resource.

        :param str storage_area_id: The storage area uuid.
        """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.get_storage_area(storage_area_id=storage_area_id).json()

    def list_compute(self):
        """ List compute elements across all sites. """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.list_compute().json()

    def list_services(self, service_type=None):
        """ List services across all sites.

        :param str service_type: Service type to filter results by.
        """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        if service_type:
            filtered_response = client.list_services().json()
            for entry in filtered_response:
                filtered_site_services = []
                for service in entry.get('services'):
                    if fnmatch.fnmatch(service.get('type'), service_type):
                        filtered_site_services.append(service)
                entry['services'] = filtered_site_services
        else:
            return client.list_services().json()
        return filtered_response

    def list_service_types(self):
        """ List supported service types. """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        # services can be "core" or associated with a compute element, use combined result
        return {
            'compute': client.list_service_types_compute().json(),
            'core': client.list_service_types_core().json()
        }

    def list_sites(self):
        """ List sites. """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.list_sites().json()

    def list_storages(self):
        """ List storages across all sites. """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.list_storages().json()

    def list_storage_areas(self):
        """ List storage areas across all sites. """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.list_storage_areas().json()

    def list_storage_areas_topojson(self):
        """ List storage areas across all sites in topojson format. """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.list_storage_areas_topojson().json()
