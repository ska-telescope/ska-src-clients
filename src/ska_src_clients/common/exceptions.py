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


class InvalidClientName(CustomException):
    def __init__(self, client_name):
        self.message = "The client name {} is not understood.".format(*client_name)
        super().__init__(self.message)

