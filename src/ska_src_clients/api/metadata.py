from ska_src_clients.api.api import API


class MetadataAPI(API):
    """ Metadata API class. """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def get_metadata(self, namespace, name, plugin):
        """ Get metadata.

        :param str namespace: The data namespace.
        :param str name: The data identifier name.
        :param str plugin: The name of the plugin to use (Rucio only).
        """
        client = self.session.client_factory.get_data_management_client(is_authenticated=True)
        return client.get_metadata(namespace=namespace, name=name, plugin=plugin).json()

    def set_metadata(self, namespace, name, metadata):
        """ Set metadata.

        :param str namespace: The data namespace.
        :param str name: The data identifier name.
        :param dict metadata: Dictionary of metadata to be added.
        """
        client = self.session.client_factory.get_data_management_client(is_authenticated=True)
        return client.set_metadata(namespace=namespace, name=name, metadata=metadata).json()

