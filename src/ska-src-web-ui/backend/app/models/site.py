from pydantic import BaseModel
from typing import Optional, List, Dict, Any


class SiteInfo(BaseModel):
    """Information about a site."""
    id: str
    parent_node_name: Optional[str] = None
    name: str
    description: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    country: Optional[str] = None
    primary_contact_email: Optional[str] = None
    secondary_contact_email: Optional[str] = None
    is_force_disabled: bool = False


class ComputeInfo(BaseModel):
    """Information about compute resources."""
    id: str
    name: str
    parent_node_name: Optional[str] = None
    parent_site_name: Optional[str] = None
    description: Optional[str] = None
    hardware_type: Optional[str] = None
    hardware_capabilities: Optional[str] = None
    is_force_disabled: bool = False


class ServiceInfo(BaseModel):
    """Information about services."""
    id: str
    name: str
    parent_node_name: Optional[str] = None
    parent_site_name: Optional[str] = None
    parent_compute_id: Optional[str] = None
    type: str
    scope: str
    associated_storage_area_id: Optional[str] = None
    prefix: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    path: Optional[str] = None
    is_force_disabled: bool = False


class StorageInfo(BaseModel):
    """Information about storage resources."""
    id: str
    name: str
    parent_node_name: Optional[str] = None
    parent_site_name: Optional[str] = None
    description: Optional[str] = None
    storage_type: Optional[str] = None
    capacity: Optional[str] = None
    is_force_disabled: bool = False


class StorageAreaInfo(BaseModel):
    """Information about storage areas."""
    id: str
    name: str
    parent_node_name: Optional[str] = None
    parent_site_name: Optional[str] = None
    description: Optional[str] = None
    storage_type: Optional[str] = None
    capacity: Optional[str] = None
    is_force_disabled: bool = False


class SiteListRequest(BaseModel):
    """Request to list sites."""
    node: Optional[str] = None


class ComputeListRequest(BaseModel):
    """Request to list compute resources."""
    node: Optional[str] = None
    site: Optional[str] = None


class ServiceListRequest(BaseModel):
    """Request to list services."""
    type: Optional[str] = None
    node: Optional[str] = None
    site: Optional[str] = None
    scope: str = "all"


class StorageListRequest(BaseModel):
    """Request to list storage resources."""
    node: Optional[str] = None
    site: Optional[str] = None


class ServiceToggleRequest(BaseModel):
    """Request to enable/disable a service."""
    service_id: str
    enable: bool


class SiteResponse(BaseModel):
    """Generic site operation response."""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None 