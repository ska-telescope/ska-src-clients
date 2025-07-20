from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional
import logging

from app.services.src_client import SRCClientService
from app.core.config import settings

router = APIRouter(prefix="/storage", tags=["storage"])

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

@router.get("/areas")
def list_storage_areas(
    node_name: Optional[str] = Query(None, description="Filter by node name"),
    site_name: Optional[str] = Query(None, description="Filter by site name"),
    parent_node_name: Optional[str] = Query(None, description="Filter by parent node name"),
    src_service: SRCClientService = Depends(get_src_service)
):
    """List storage areas."""
    try:
        logging.info(f"Storage areas list request - node_name: {node_name}, site_name: {site_name}, parent_node_name: {parent_node_name}")
        
        # Use parent_node_name if provided, otherwise fall back to node_name
        filter_node = parent_node_name or node_name
        
        result = src_service.site_api.list_storage_areas(node_name=filter_node, site_name=site_name)
        logging.info(f"Storage areas list result - returned {len(result)} storage areas")
        
        return {
            "success": True,
            "message": "Storage areas listed successfully",
            "data": result
        }
    except Exception as e:
        logging.error(f"Error listing storage areas: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/")
def list_storage(
    node_name: Optional[str] = Query(None, description="Filter by node name"),
    site_name: Optional[str] = Query(None, description="Filter by site name"),
    parent_node_name: Optional[str] = Query(None, description="Filter by parent node name"),
    src_service: SRCClientService = Depends(get_src_service)
):
    """List storage resources."""
    try:
        logging.info(f"Storage list request - node_name: {node_name}, site_name: {site_name}, parent_node_name: {parent_node_name}")
        
        # Use parent_node_name if provided, otherwise fall back to node_name
        filter_node = parent_node_name or node_name
        
        result = src_service.site_api.list_storages(node_name=filter_node, site_name=site_name)
        logging.info(f"Storage list result - returned {len(result)} storage resources")
        
        return {
            "success": True,
            "message": "Storage resources listed successfully",
            "data": result
        }
    except Exception as e:
        logging.error(f"Error listing storage: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{storage_id}")
def get_storage(storage_id: str, src_service: SRCClientService = Depends(get_src_service)):
    """Get storage details by unique ID."""
    try:
        result = src_service.site_api.get_storage(storage_id)
        return {
            "success": True,
            "message": "Storage details retrieved successfully",
            "data": result
        }
    except Exception as e:
        logging.error(f"Error getting storage: {e}")
        raise HTTPException(status_code=404, detail=str(e)) 