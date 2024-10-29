from abc import ABC


class API(ABC):
    """Base API class."""

    def __init__(self, session):
        self.session = session
