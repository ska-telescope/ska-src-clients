from fastapi import APIRouter, HTTPException, Depends, Query
from typing import List, Optional
import logging

from app.models.site import (
    SiteListRequest, ComputeListRequest, ServiceListRequest, StorageListRequest,
    ServiceToggleRequest, SiteResponse
)
from app.services.src_client import SRCClientService
from app.core.config import settings

router = APIRouter(prefix="/site", tags=["site"])

# Global service instance - in production, use dependency injection
_src_service: SRCClientService = None

def get_src_service() -> SRCClientService:
    """Get the SRC client service instance."""
    global _src_service
    if _src_service is None:
        try:
            _src_service = SRCClientService(config_path=settings.srcnet_config_path)
        except Exception as e:
            logging.error(f"Failed to initialize SRC service: {e}")
            raise HTTPException(status_code=500, detail="Failed to initialize SRC service")
    return _src_service


@router.get("/{site_id}", response_model=SiteResponse)
async def get_site(
    site_id: str,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Get information about a specific site."""
    try:
        result = src_service.get_site(site_id)
        return SiteResponse(
            success=True,
            message="Site information retrieved successfully",
            data=result
        )
    except Exception as e:
        logging.error(f"Error getting site: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=SiteResponse)
async def list_sites(
    node: Optional[str] = Query(None, description="Filter by node name"),
    src_service: SRCClientService = Depends(get_src_service)
):
    """List sites."""
    try:
        result = src_service.list_sites(node_name=node)
        return SiteResponse(
            success=True,
            message="Sites listed successfully",
            data=result
        )
    except Exception as e:
        logging.error(f"Error listing sites: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/compute/{compute_id}", response_model=SiteResponse)
async def get_compute(
    compute_id: str,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Get compute details by ID."""
    try:
        result = src_service.get_compute(compute_id)
        return SiteResponse(
            success=True,
            message="Compute information retrieved successfully",
            data=result
        )
    except Exception as e:
        logging.error(f"Error getting compute: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/compute/", response_model=SiteResponse)
async def list_compute(
    node: Optional[str] = Query(None, description="Filter by node name"),
    site: Optional[str] = Query(None, description="Filter by site name"),
    src_service: SRCClientService = Depends(get_src_service)
):
    """List compute resources."""
    try:
        result = src_service.list_compute(node_name=node, site_name=site)
        return SiteResponse(
            success=True,
            message="Compute resources listed successfully",
            data=result
        )
    except Exception as e:
        logging.error(f"Error listing compute: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/service/debug", response_model=SiteResponse)
async def debug_services(
    type: Optional[str] = Query(None, description="Filter by service type"),
    node: Optional[str] = Query(None, description="Filter by node name"),
    site: Optional[str] = Query(None, description="Filter by site name"),
    scope: str = Query("all", description="Filter by scope (global|local|all)"),
    src_service: SRCClientService = Depends(get_src_service)
):
    """Debug endpoint to inspect raw service data."""
    try:
        logging.info(f"Debug listing services with filters: type={type}, node={node}, site={site}, scope={scope}")
        
        # Get raw services
        raw_services = src_service.site_api.list_services(service_type=type, node_name=node, site_name=site, scope=scope)
        logging.info(f"Raw services count: {len(raw_services)}")
        
        # Get sites
        sites = src_service.site_api.list_sites()
        logging.info(f"Sites count: {len(sites)}")
        
        # Sample a few services to see their structure
        sample_services = raw_services[:3] if raw_services else []
        
        debug_data = {
            "raw_services_count": len(raw_services),
            "sites_count": len(sites),
            "sample_services": sample_services,
            "sites": sites,
            "filters_applied": {
                "type": type,
                "node": node,
                "site": site,
                "scope": scope
            }
        }
        
        return SiteResponse(
            success=True,
            message="Debug data retrieved successfully",
            data=debug_data
        )
    except Exception as e:
        logging.error(f"Error in debug endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/service/{service_id}", response_model=SiteResponse)
async def get_service(
    service_id: str,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Get service details by ID."""
    try:
        result = src_service.get_service(service_id)
        return SiteResponse(
            success=True,
            message="Service information retrieved successfully",
            data=result
        )
    except Exception as e:
        logging.error(f"Error getting service: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/service/", response_model=SiteResponse)
async def list_services(
    type: Optional[str] = Query(None, description="Filter by service type"),
    node: Optional[str] = Query(None, description="Filter by node name"),
    site: Optional[str] = Query(None, description="Filter by site name"),
    scope: str = Query("all", description="Filter by scope (global|local|all)"),
    src_service: SRCClientService = Depends(get_src_service)
):
    """List services with enriched information."""
    try:
        logging.info(f"Listing services with filters: type={type}, node={node}, site={site}, scope={scope}")
        result = src_service.list_services_enriched(
            service_type=type, node_name=node, site_name=site, scope=scope
        )
        logging.info(f"Returning {len(result)} services")
        logging.debug(f"Services data: {result}")
        return SiteResponse(
            success=True,
            message="Services listed successfully",
            data=result
        )
    except Exception as e:
        logging.error(f"Error listing services: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/service/{service_id}/toggle", response_model=SiteResponse)
async def toggle_service(
    service_id: str,
    request: ServiceToggleRequest,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Enable or disable a service."""
    try:
        if request.enable:
            result = src_service.enable_service(service_id)
            message = "Service enabled successfully" if result else "Failed to enable service"
        else:
            result = src_service.disable_service(service_id)
            message = "Service disabled successfully" if result else "Failed to disable service"
        
        return SiteResponse(
            success=result,
            message=message,
            data={"service_id": service_id, "enabled": request.enable}
        )
    except Exception as e:
        logging.error(f"Error toggling service: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "site"} 