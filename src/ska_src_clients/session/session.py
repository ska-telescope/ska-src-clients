from ska_src_clients.client.client import ClientFactory
from ska_src_clients.common.exceptions import InvalidAPIName, NoAPIUrlFound


class Session:
    def __init__(self, config):
        self.apis = config['apis']
        self.client_factory = ClientFactory(session=self)

    def get_api_url_by_service_name(self, name):
        if name not in self.apis:
            raise InvalidAPIName(name)
        this_client_api_url = self.apis.get(name).get('url')
        if this_client_api_url:
            return this_client_api_url
        raise NoAPIUrlFound(name)
