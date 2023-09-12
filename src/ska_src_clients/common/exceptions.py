import json
import requests
import traceback
from functools import wraps

from fastapi import HTTPException, status


def handle_client_exceptions(func):
    """ Decorator to handle client exceptions. """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.HTTPError as e:
            detail = f"HTTP error occurred: {e}, response: {e.response.text}"
            raise Exception(detail)
        except HTTPException as e:
            raise e
        except CustomException as e:
            raise Exception(message=e.message)
        except CustomHTTPException as e:
            raise HTTPException(status_code=e.http_error_status, detail=e.message)
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


class CustomHTTPException(Exception):
    """ Class that all custom HTTP exceptions must inherit in order for exception to be caught by
    the handle_exceptions decorator.
    """
    pass
