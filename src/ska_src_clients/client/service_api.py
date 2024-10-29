from ska_src_authn_api.client.authentication import AuthenticationClient
from ska_src_data_management_api.client.data_management import DataManagementClient
from ska_src_permissions_api.client.permissions import PermissionsClient
from ska_src_site_capabilities_api.client.site_capabilities import SiteCapabilitiesClient

from ska_src_clients.common.utility import get_authenticated_requests_session, remove_expired_tokens


class ServiceAPIClientFactory:
    def __init__(self, session):
        self.session = session

    def _get_client(self, service_name, client, is_authenticated=False):
        api_url = self.session.get_api_url_by_service_name(service_name)

        # create a requests session (either authenticated or unauthenticated)
        if is_authenticated:
            authenticated_requests_session = get_authenticated_requests_session(session=self.session, service_name=service_name)
            instance = client(api_url=api_url, session=authenticated_requests_session)
        else:
            instance = client(api_url=api_url, session=None)

        # add token decorators to all client functions if the session uses tokens
        if getattr(self.session, "stored_token_directory"):
            for name, value in vars(instance).items():
                if callable(value):
                    setattr(instance, name, remove_expired_tokens(value))
        return instance

    def get_authn_client(self, is_authenticated=False):
        """Get an authentication client."""
        return self._get_client(
            service_name="authn-api",
            client=AuthenticationClient,
            is_authenticated=is_authenticated,
        )

    def get_client_from_service_name(self, service_name, is_authenticated=False):
        """Get a client from a service name.

        :param str service_name: The service name to get a client for.
        :param bool is_authenticated: Get an un/authenticated client.
        :return: A client for the service.
        """
        if service_name == "authn-api":
            return self.get_authn_client(is_authenticated=is_authenticated)
        elif service_name == "data-management-api":
            return self.get_data_management_client(is_authenticated=is_authenticated)
        elif service_name == "permissions-api":
            return self.get_permissions_client(is_authenticated=is_authenticated)
        elif service_name == "site-capabilities-api":
            return self.get_site_capabilities_client(is_authenticated=is_authenticated)

    def get_data_management_client(self, is_authenticated=False):
        """Get a data management client."""
        return self._get_client(
            service_name="data-management-api",
            client=DataManagementClient,
            is_authenticated=is_authenticated,
        )

    def get_permissions_client(self, is_authenticated=False):
        """Get a permissions client."""
        return self._get_client(
            service_name="permissions-api",
            client=PermissionsClient,
            is_authenticated=is_authenticated,
        )

    def get_site_capabilities_client(self, is_authenticated=False):
        """Get a site capabilities client."""
        return self._get_client(
            service_name="site-capabilities-api",
            client=SiteCapabilitiesClient,
            is_authenticated=is_authenticated,
        )
