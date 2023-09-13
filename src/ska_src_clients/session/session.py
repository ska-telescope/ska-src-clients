from ska_src_clients.common.exceptions import InvalidClientName
from ska_src_authn_api.client.authentication import AuthenticationClient
from ska_src_data_management_api.client.data_management import DataManagementClient
from ska_src_permissions_api.client.permissions import PermissionsClient
from ska_src_site_capabilities_api.client.site_capabilities import SiteCapabilitiesClient


class Session:
    def __init__(self, config):
        self.clients = {}
        if 'AUTHN_API_URL' in config['general']:
            self.clients['authn-api'] = AuthenticationClient(
                api_url=config['general']['AUTHN_API_URL'])
        if 'DATA_MANAGEMENT_API_URL' in config['general']:
            self.clients['data-management-api'] = DataManagementClient(
                api_url=config['general']['DATA_MANAGEMENT_API_URL'])
        if 'PERMISSIONS_API_URL' in config['general']:
            self.clients['permissions-api'] = PermissionsClient(
                api_url=config['general']['PERMISSIONS_API_URL'])
        if 'SITE_CAPABILITIES_API_URL' in config['general']:
            self.clients['site-capabilities-api'] = SiteCapabilitiesClient(
                api_url=config['general']['SITE_CAPABILITIES_API_URL'])

    def get_client_by_service_name(self, name):
        if name not in self.clients:
            raise InvalidClientName(name)
        return self.clients.get(name)
