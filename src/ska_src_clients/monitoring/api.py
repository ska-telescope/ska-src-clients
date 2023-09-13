import pprint

from ska_src_clients.common.utility import remove_expired_tokens


class APIMonitoring:
    def __init__(self, session):
        self.session = session

    @remove_expired_tokens
    def get_service_healths(self, names):
        if isinstance(names, list):
            healths = {}
            for name in names:
                healths[name] = self.session.get_client_by_service_name(name).health().json()
            pprint.pprint(healths)
        else:
            pprint.pprint(self.session.get_client_by_service_name(names).health().json())

    @remove_expired_tokens
    def ping_services(self, names):
        if isinstance(names, list):
            pings = {}
            for name in names:
                pings[name] = self.session.get_client_by_service_name(name).ping().json()
            pprint.pprint(pings)
        else:
            pprint.pprint(self.session.get_client_by_service_name(names).ping().json())
