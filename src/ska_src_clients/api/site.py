from ska_src_clients.api.api import API


class SiteAPI(API):
    """ Site API class. """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def list_services(self):
        """ List services across all sites. """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.list_services().json()

    def list_storages(self):
        """ List storages across all sites. """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.list_storages().json()
