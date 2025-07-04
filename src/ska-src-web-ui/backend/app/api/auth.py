from fastapi import APIRouter, HTTPException, Depends
from typing import List, Optional
import logging

from app.models.auth import (
    DeviceFlowRequest, DeviceFlowResponse, TokenRequestRequest, TokenRequestResponse,
    TokenExchangeRequest, TokenExchangeResponse, TokenListResponse, TokenInspectResponse
)
from app.services.src_client import SRCClientService
from app.core.config import settings
from ska_src_clients.common.exceptions import NoAccessTokenFoundForService

router = APIRouter(prefix="/auth", tags=["authentication"])

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


@router.post("/device-flow", response_model=DeviceFlowResponse)
async def start_device_flow(
    request: DeviceFlowRequest,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Start OIDC device flow authentication."""
    try:
        response = src_service.start_device_flow()
        return DeviceFlowResponse(**response)
    except Exception as e:
        logging.error(f"Error starting device flow: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/token/request", response_model=TokenRequestResponse)
async def request_token(
    request: TokenRequestRequest,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Request a new access token using device flow."""
    try:
        response = src_service.request_token(
            max_polling_attempts=request.max_polling_attempts,
            wait_between_polling_s=request.wait_between_polling_s
        )
        return TokenRequestResponse(**response)
    except Exception as e:
        logging.error(f"Error requesting token: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/token/complete/{device_code}")
async def complete_token_request(
    device_code: str,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Complete a token request by polling for completion."""
    try:
        result = src_service.complete_token_request(device_code)
        return result
    except Exception as e:
        logging.error(f"Error completing token request: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/token/check/{device_code}")
async def check_token_completion(
    device_code: str,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Check if a token request has been completed (single check, no polling)."""
    try:
        result = src_service.check_token_completion(device_code)
        logging.info(f"API endpoint returning result: {result}")
        return result
    except Exception as e:
        logging.error(f"Error checking token completion: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/token/exchange", response_model=TokenExchangeResponse)
async def exchange_token(
    request: TokenExchangeRequest,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Exchange token for a specific service."""
    try:
        success = src_service.exchange_token(request.service_name, request.version)
        return TokenExchangeResponse(
            success=success,
            message="Token exchanged successfully" if success else "Token exchange failed",
            service_name=request.service_name
        )
    except Exception as e:
        error_str = str(e).lower()
        if "authentication server is currently unavailable" in error_str:
            logging.error(f"Authentication server connectivity issue: {e}")
            raise HTTPException(
                status_code=503, 
                detail="Authentication server is currently unavailable. Please try again later."
            )
        else:
            logging.error(f"Error exchanging token: {e}")
            raise HTTPException(status_code=500, detail=str(e))


@router.get("/tokens", response_model=TokenListResponse)
async def list_tokens(
    src_service: SRCClientService = Depends(get_src_service)
):
    """List all available access tokens."""
    try:
        tokens = src_service.list_tokens()
        return TokenListResponse(tokens=tokens)
    except NoAccessTokenFoundForService:
        # Return empty list when no tokens are available
        return TokenListResponse(tokens=[])
    except Exception as e:
        logging.error(f"Error listing tokens: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tokens/{service_name}/inspect", response_model=TokenInspectResponse)
async def inspect_token(
    service_name: str,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Inspect a specific access token."""
    try:
        token_data = src_service.inspect_token(service_name)
        return TokenInspectResponse(token_data=token_data)
    except Exception as e:
        logging.error(f"Error inspecting token: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "auth"}


@router.get("/api-status")
async def check_api_status(
    src_service: SRCClientService = Depends(get_src_service)
):
    """Check the status of external APIs using their /health endpoints."""
    try:
        status = {
            "auth": {"status": "unknown", "error": None},
            "site-capabilities": {"status": "unknown", "error": None},
            "data-management": {"status": "unknown", "error": None}
        }
        
        # Check auth API status using /health endpoint
        try:
            # Get the auth client and check its health endpoint
            auth_client = src_service.session.client_factory.get_authn_client()
            # Most auth clients have a ping method that's equivalent to /health
            auth_client.ping()
            status["auth"]["status"] = "online"
        except Exception as e:
            status["auth"]["status"] = "offline"
            status["auth"]["error"] = str(e)
        
        # Check site-capabilities API status using /health endpoint
        try:
            # Get the site capabilities client and check its health endpoint
            site_client = src_service.session.client_factory.get_site_capabilities_client(is_authenticated=False)
            # Try to access the health endpoint
            import requests
            site_url = site_client.base_url if hasattr(site_client, 'base_url') else None
            if site_url:
                health_response = requests.get(f"{site_url}/health", timeout=5)
                if health_response.status_code == 200:
                    status["site-capabilities"]["status"] = "online"
                else:
                    status["site-capabilities"]["status"] = "offline"
                    status["site-capabilities"]["error"] = f"Health check returned {health_response.status_code}"
            else:
                # Fallback: try a simple API call
                site_client.list_sites()
                status["site-capabilities"]["status"] = "online"
        except Exception as e:
            status["site-capabilities"]["status"] = "offline"
            status["site-capabilities"]["error"] = str(e)
        
        # Check data-management API status using /health endpoint
        try:
            # Get the data management client and check its health endpoint
            data_client = src_service.session.client_factory.get_data_management_client(is_authenticated=False)
            # Try to access the health endpoint
            import requests
            data_url = data_client.base_url if hasattr(data_client, 'base_url') else None
            if data_url:
                health_response = requests.get(f"{data_url}/health", timeout=5)
                if health_response.status_code == 200:
                    status["data-management"]["status"] = "online"
                else:
                    status["data-management"]["status"] = "offline"
                    status["data-management"]["error"] = f"Health check returned {health_response.status_code}"
            else:
                # Fallback: try a simple API call
                data_client.list_namespaces()
                status["data-management"]["status"] = "online"
        except Exception as e:
            status["data-management"]["status"] = "offline"
            status["data-management"]["error"] = str(e)


        
        return status
    except Exception as e:
        logging.error(f"Error checking API status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Data Management API endpoints
@router.get("/data/namespaces")
async def list_namespaces(
    src_service: SRCClientService = Depends(get_src_service)
):
    """List available namespaces."""
    try:
        result = src_service.list_namespaces()
        return {"success": True, "data": result}
    except NoAccessTokenFoundForService:
        # Return authentication required error
        raise HTTPException(status_code=401, detail="Authentication required. Please request a token first.")
    except Exception as e:
        logging.error(f"Error listing namespaces: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/data/files/{namespace}/{name}")
async def list_files(
    namespace: str,
    name: str,
    detail: bool = False,
    filters: Optional[str] = None,
    limit: int = 100,
    src_service: SRCClientService = Depends(get_src_service)
):
    """List files in a namespace."""
    try:
        result = src_service.list_files(namespace, name, detail, filters, limit)
        return {"success": True, "data": result}
    except Exception as e:
        logging.error(f"Error listing files: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Site Capabilities API endpoints
@router.get("/site/services")
async def list_services(
    service_type: Optional[str] = None,
    node_name: Optional[str] = None,
    site_name: Optional[str] = None,
    scope: str = "all",
    src_service: SRCClientService = Depends(get_src_service)
):
    """List services (enriched with site/node/host/port/path info)."""
    try:
        result = src_service.list_services_enriched(service_type, node_name, site_name, scope)
        return {"success": True, "data": result}
    except NoAccessTokenFoundForService:
        # Return authentication required error
        raise HTTPException(status_code=401, detail="Authentication required. Please request a token first.")
    except Exception as e:
        logging.error(f"Error listing services: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/site/sites")
async def list_sites(
    node_name: Optional[str] = None,
    src_service: SRCClientService = Depends(get_src_service)
):
    """List sites."""
    try:
        result = src_service.list_sites(node_name)
        return {"success": True, "data": result}
    except NoAccessTokenFoundForService:
        # Return authentication required error
        raise HTTPException(status_code=401, detail="Authentication required. Please request a token first.")
    except Exception as e:
        logging.error(f"Error listing sites: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/site/compute")
async def list_compute(
    node_name: Optional[str] = None,
    site_name: Optional[str] = None,
    src_service: SRCClientService = Depends(get_src_service)
):
    """List compute resources."""
    try:
        result = src_service.list_compute(node_name, site_name)
        return {"success": True, "data": result}
    except Exception as e:
        logging.error(f"Error listing compute: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 