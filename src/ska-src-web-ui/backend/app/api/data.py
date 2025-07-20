from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from typing import List, Optional
import logging
import tempfile
import os

from app.models.data import (
    DataDownloadRequest, DataLocateRequest, DataListRequest, DataUploadRequest,
    DataMoveRequest, DataStageRequest, JobStatusRequest, DataResponse
)
from app.services.src_client import SRCClientService
from app.core.config import settings

router = APIRouter(prefix="/data", tags=["data"])

# Global service instance - in production, use dependency injection
_src_service: SRCClientService = None
_current_config_path: str = None

def get_src_service() -> SRCClientService:
    """Get the SRC client service instance."""
    global _src_service, _current_config_path
    
    # Use the configured config path or default
    config_path = getattr(settings, 'srcnet_config_path', None)
    
    logging.debug(f"get_src_service: current_config_path={_current_config_path}, new_config_path={config_path}")
    
    # If service doesn't exist or config path changed, create new service
    if _src_service is None or _current_config_path != config_path:
        logging.info(f"Creating new SRC service with config_path={config_path}")
        try:
            _src_service = SRCClientService(config_path=config_path)
            _current_config_path = config_path
            logging.info(f"SRC service created successfully with config_path={config_path}")
        except Exception as e:
            logging.error(f"Failed to initialize SRC service: {e}")
            raise HTTPException(status_code=500, detail="Failed to initialize SRC service")
    else:
        logging.debug(f"Using existing SRC service with config_path={_current_config_path}")
    return _src_service


@router.post("/download", response_model=DataResponse)
async def download_data(
    request: DataDownloadRequest,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Download data by namespace and name."""
    try:
        result = src_service.download_data(
            request.namespace, request.name, request.sort, 
            request.ip_address, request.no_verify, request.output
        )
        return DataResponse(
            success=True,
            message="Download completed successfully",
            data=result
        )
    except Exception as e:
        logging.error(f"Error downloading data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/locate", response_model=DataResponse)
async def locate_data(
    request: DataLocateRequest,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Locate data by namespace and name."""
    try:
        result = src_service.locate_data(
            request.namespace, request.name, request.sort, request.ip_address
        )
        return DataResponse(
            success=True,
            message="Data location retrieved successfully",
            data=result
        )
    except Exception as e:
        logging.error(f"Error locating data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/list", response_model=DataResponse)
async def list_files(
    request: DataListRequest,
    src_service: SRCClientService = Depends(get_src_service)
):
    """List files in a namespace."""
    try:
        result = src_service.list_files(
            request.namespace, request.name, request.detail, 
            request.filters, request.limit
        )
        return DataResponse(
            success=True,
            message="Files listed successfully",
            data=result
        )
    except Exception as e:
        logging.error(f"Error listing files: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    namespace: str = None,
    ingest_service_id: str = None,
    metadata_suffix: str = ".meta",
    extra_metadata: str = "{}",
    debug: bool = False,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Upload a file for ingest."""
    if not namespace or not ingest_service_id:
        raise HTTPException(status_code=400, detail="namespace and ingest_service_id are required")
    
    try:
        # Create temporary directory for upload
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, file.filename)
            
            # Save uploaded file
            with open(file_path, "wb") as buffer:
                content = await file.read()
                buffer.write(content)
            
            # Upload for ingest
            result = src_service.upload_for_ingest(
                temp_dir, ingest_service_id, namespace, 
                metadata_suffix, extra_metadata, debug
            )
            
            return DataResponse(
                success=True,
                message="File uploaded successfully",
                data=result
            )
    except Exception as e:
        logging.error(f"Error uploading file: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/move", response_model=DataResponse)
async def move_data(
    request: DataMoveRequest,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Make a data movement request."""
    try:
        result = src_service.move_request(
            request.to_storage_area_id, request.dids, 
            request.lifetime, request.parent_namespace
        )
        return DataResponse(
            success=True,
            message="Data movement request submitted successfully",
            data=result,
            job_id=result.get("job_id") if result else None
        )
    except Exception as e:
        logging.error(f"Error making move request: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/move/status", response_model=DataResponse)
async def move_status(
    request: JobStatusRequest,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Get the status of a data movement request."""
    try:
        result = src_service.move_status(request.job_id)
        return DataResponse(
            success=True,
            message="Move status retrieved successfully",
            data=result
        )
    except Exception as e:
        logging.error(f"Error getting move status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stage", response_model=DataResponse)
async def stage_data(
    request: DataStageRequest,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Make a data staging request."""
    try:
        result = src_service.stage_request(
            request.to_storage_area_id, request.dids, 
            request.lifetime, request.parent_namespace
        )
        return DataResponse(
            success=True,
            message="Data staging request submitted successfully",
            data=result,
            job_id=result.get("job_id") if result else None
        )
    except Exception as e:
        logging.error(f"Error making stage request: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stage/status", response_model=DataResponse)
async def stage_status(
    request: JobStatusRequest,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Get the status of a data staging request."""
    try:
        result = src_service.stage_status(request.job_id)
        return DataResponse(
            success=True,
            message="Stage status retrieved successfully",
            data=result
        )
    except Exception as e:
        logging.error(f"Error getting stage status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/namespaces", response_model=DataResponse)
async def list_namespaces(
    src_service: SRCClientService = Depends(get_src_service)
):
    """List available namespaces."""
    try:
        result = src_service.list_namespaces()
        return DataResponse(
            success=True,
            message="Namespaces listed successfully",
            data=result
        )
    except Exception as e:
        logging.error(f"Error listing namespaces: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "data"} 