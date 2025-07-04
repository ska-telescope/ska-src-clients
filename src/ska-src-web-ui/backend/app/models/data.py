from pydantic import BaseModel
from typing import Optional, List, Dict, Any


class DataDownloadRequest(BaseModel):
    """Request to download data."""
    namespace: str
    name: str
    sort: str = "nearest_by_ip"
    ip_address: str = ""
    no_verify: bool = False
    output: Optional[str] = None


class DataLocateRequest(BaseModel):
    """Request to locate data."""
    namespace: str
    name: str
    sort: str = "nearest_by_ip"
    ip_address: str = ""


class DataListRequest(BaseModel):
    """Request to list files in namespace."""
    namespace: str
    name: str
    detail: bool = False
    filters: Optional[str] = None
    limit: int = 100


class DataUploadRequest(BaseModel):
    """Request to upload data for ingest."""
    path: str
    ingest_service_id: str
    namespace: str
    extra_metadata: str = "{}"
    metadata_suffix: str = ".meta"
    debug: bool = False


class DataMoveRequest(BaseModel):
    """Request to move data."""
    to_storage_area_id: str
    dids: List[str]
    lifetime: str
    parent_namespace: Optional[str] = None


class DataStageRequest(BaseModel):
    """Request to stage data."""
    to_storage_area_id: str
    dids: List[str]
    lifetime: str
    parent_namespace: Optional[str] = None


class JobStatusRequest(BaseModel):
    """Request to get job status."""
    job_id: str


class DataResponse(BaseModel):
    """Generic data operation response."""
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
    job_id: Optional[str] = None


class FileInfo(BaseModel):
    """Information about a file."""
    name: str
    size: Optional[int] = None
    checksum: Optional[str] = None
    path: Optional[str] = None


class NamespaceInfo(BaseModel):
    """Information about a namespace."""
    name: str
    description: Optional[str] = None
    files: Optional[List[FileInfo]] = None 