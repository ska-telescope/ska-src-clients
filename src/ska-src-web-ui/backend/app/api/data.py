from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from typing import List, Optional
import logging
import tempfile
import os
import json

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
            data={"files": result}
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
    protocol: str = None,
    host: str = None,
    port: str = None,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Upload a file for ingest with complete flow reproduction."""
    if not namespace or not ingest_service_id:
        raise HTTPException(status_code=400, detail="namespace and ingest_service_id are required")
    
    try:
        logging.info(f"Starting upload process for file: {file.filename}")
        logging.info(f"Parameters: namespace={namespace}, ingest_service_id={ingest_service_id}, metadata_suffix={metadata_suffix}")
        
        # Step 1: Validate and prepare metadata
        try:
            extra_metadata_dict = json.loads(extra_metadata) if extra_metadata else {}
            reserved_keys = ['namespace', 'ingest_service_id']
            for key in reserved_keys:
                if key in extra_metadata_dict:
                    raise HTTPException(status_code=400, detail=f"Reserved key '{key}' cannot be used in extra_metadata")
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON in extra_metadata")
        
        # Step 2: Check token availability
        try:
            tokens = src_service.list_tokens()
            has_data_management_token = any(token.get('service_name') == 'data-management-api' for token in tokens)
            if not has_data_management_token:
                raise HTTPException(
                    status_code=401, 
                    detail="Data Management API token required. Please exchange a token for data-management-api first."
                )
            logging.info("Data Management API token found")
        except Exception as e:
            logging.error(f"Token validation failed: {e}")
            raise HTTPException(status_code=401, detail="Token validation failed")
        
        # Step 3: Create temporary directory and save file
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, file.filename)
            
            # Save uploaded file
            logging.info(f"Saving uploaded file to temporary location: {file_path}")
            with open(file_path, "wb") as buffer:
                content = await file.read()
                buffer.write(content)
            
            logging.info(f"File saved successfully. Size: {len(content)} bytes")
            
            # Step 4: Attempt upload with enhanced error handling
            try:
                logging.info("Starting upload_for_ingest process")
                logging.info(f"Override parameters: protocol={protocol}, host={host}, port={port}")
                result = src_service.upload_for_ingest(
                    path=temp_dir, 
                    ingest_service_id=ingest_service_id, 
                    namespace=namespace, 
                    metadata_suffix=metadata_suffix, 
                    extra_metadata=extra_metadata, 
                    protocol_prefix=protocol, 
                    host_override=host,
                    port_override=port,
                    debug=debug
                )
                
                logging.info("Upload completed successfully")
                return DataResponse(
                    success=True,
                    message="File uploaded successfully",
                    data={
                        "filename": file.filename,
                        "namespace": namespace,
                        "ingest_service_id": ingest_service_id,
                        "file_size": len(content),
                        "upload_result": result
                    }
                )
                
            except Exception as upload_error:
                error_str = str(upload_error).lower()
                
                # Handle specific OAuth token scope errors
                if "invalid_scope" in error_str or "oauth" in error_str:
                    logging.error(f"OAuth token scope error during upload: {upload_error}")
                    raise HTTPException(
                        status_code=401,
                        detail="OAuth token scope error. The data management API token needs additional Rucio scopes for upload operations. Please try exchanging a new token for data-management-api."
                    )
                
                # Handle token exchange errors
                elif "token" in error_str and ("exchange" in error_str or "unauthorized" in error_str):
                    logging.error(f"Token exchange error during upload: {upload_error}")
                    raise HTTPException(
                        status_code=401,
                        detail="Token exchange failed. Please ensure you have a valid token for data-management-api and try again."
                    )
                
                # Handle storage/network errors
                elif any(keyword in error_str for keyword in ["connection", "timeout", "network", "storage"]):
                    logging.error(f"Storage/network error during upload: {upload_error}")
                    raise HTTPException(
                        status_code=503,
                        detail="Storage service unavailable. Please check your connection and try again."
                    )
                
                # Handle validation errors
                elif any(keyword in error_str for keyword in ["validation", "invalid", "bad request"]):
                    logging.error(f"Validation error during upload: {upload_error}")
                    raise HTTPException(
                        status_code=400,
                        detail=f"Upload validation failed: {str(upload_error)}"
                    )
                
                # Generic error handling
                else:
                    logging.error(f"Unexpected error during upload: {upload_error}")
                    raise HTTPException(
                        status_code=500,
                        detail=f"Upload failed: {str(upload_error)}"
                    )
                    
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        logging.error(f"Unexpected error in upload endpoint: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


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
            data={"namespaces": result}
        )
    except Exception as e:
        logging.error(f"Error listing namespaces: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "data"} 