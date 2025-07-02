from ska_src_clients.api.api import API
from ska_src_clients.common.exceptions import handle_client_exceptions

class NodeAPI(API):
    """ Node API class. """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @handle_client_exceptions
    def get_add_node_www_url(self):
        """ Get the add node www URL. """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.get_add_node_www_url()

    @handle_client_exceptions
    def get_edit_node_www_url(self, node_name):
        """ Get the edit node www URL.

        :param str node_name: The node name.
        """
        client = self.session.client_factory.get_site_capabilities_client(is_authenticated=True)
        return client.get_edit_node_www_url(node_name=node_name)