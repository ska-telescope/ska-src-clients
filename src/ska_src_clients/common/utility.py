import os
import time
from functools import wraps

def remove_expired_tokens(func):
    """ Decorator to remove expired tokens. """
    @wraps(func)
    def wrapper(*args, **kwargs):
        if hasattr(args[0], 'session'):     # Check if self has a session instance
            instance = args[0].session
        else:                               # Failing that, is self a session instance itself?
            instance = args[0]
        access_tokens = dict(instance.access_tokens)
        for aud, attributes in access_tokens.items():
            if attributes.get('expires_at') < time.time():
                instance.access_tokens.pop(aud)
                if attributes.get('path_on_disk'):
                    os.remove(attributes.get('path_on_disk'))
        return func(*args, **kwargs)
    return wrapper
