from ska_src_clients.api.api import API
from ska_src_clients.common.exceptions import handle_client_exceptions


class StatusAPI(API):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @handle_client_exceptions
    def health(self, service):
        """ Get the health of a service.

        :param str service: The API service name.
        """
        client = self.session.client_factory.get_client_from_service_name(service)
        try:
            health = client.health()
        except Exception as e:
            if 500 <= e.status_code < 600:
                return None
        return health.json()

    @handle_client_exceptions
    def ping(self, service):
        """ Ping a service.

        :param str service: The API service name.
        """
        client = self.session.client_factory.get_client_from_service_name(service)
        try:
            ping = client.ping()
        except Exception as e:
            if 500 <= e.status_code < 600:
                return {
                    'status': "DOWN"
                }
        return ping.json()
