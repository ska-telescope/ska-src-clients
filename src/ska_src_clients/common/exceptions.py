import requests
import traceback
from functools import wraps


def handle_client_exceptions(func):
    """ Decorator to handle client exceptions. """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.HTTPError as e:
            detail = f"HTTP error occurred: {e}, response: {e.response.text}"
            raise Exception(detail)
        except CustomException as e:
            raise Exception(message=e.message)
        except Exception as e:
            detail = "General error occurred: {}, traceback: {}".format(
                repr(e), ''.join(traceback.format_tb(e.__traceback__)))
            raise Exception(detail)
    return wrapper


class CustomException(Exception):
    """ Class that all custom exceptions must inherit in order for exception to be caught by the
    handle_exceptions decorator.
    """
    pass


class InvalidAPIName(CustomException):
    def __init__(self, api_name):
        self.message = "The api name {} is not in the configuration file.".format(api_name)
        super().__init__(self.message)


class NoAPIUrlFound(CustomException):
    def __init__(self, api_name):
        self.message = "Could not find the api url for the api with name {}.".format(api_name)
        super().__init__(self.message)


class NoAccessTokenFoundForService(CustomException):
    def __init__(self, service_name):
        self.message = "Could not find an access token for the service with name {}.".format(service_name)
        super().__init__(self.message)

