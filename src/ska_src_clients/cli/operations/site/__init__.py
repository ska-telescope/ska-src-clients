from .compute import compute
from .service import service
from .site import site
from .storage import storage
from .storage_area import storage_area

site.add_command(compute)
site.add_command(service)
site.add_command(storage)
site.add_command(storage_area)