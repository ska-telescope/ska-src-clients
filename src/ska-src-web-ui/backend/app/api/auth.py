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
    import asyncio
    import requests
    from concurrent.futures import ThreadPoolExecutor
    import signal
    import functools
    
    def timeout_handler(signum, frame):
        raise TimeoutError("Operation timed out")
    
    def run_with_timeout(func, timeout_seconds=2):
        """Run a function with a timeout using signal."""
        import signal
        
        def timeout_handler(signum, frame):
            raise TimeoutError(f"Operation timed out after {timeout_seconds} seconds")
        
        # Set the signal handler
        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout_seconds)
        
        try:
            result = func()
            signal.alarm(0)  # Cancel the alarm
            return result
        except TimeoutError:
            raise
        except Exception as e:
            signal.alarm(0)  # Cancel the alarm
            raise e
        finally:
            # Restore the original signal handler
            signal.signal(signal.SIGALRM, old_handler)
    
    def check_auth_service():
        """Check auth service status with timeout."""
        try:
            response = requests.get("https://authn.srcnet.skao.int/api/v1/ping", timeout=2)
            return response.status_code == 200
        except Exception:
            return False
    
    def check_permissions_service():
        """Check permissions service status with timeout."""
        try:
            response = requests.get("https://permissions.srcnet.skao.int/api/v1/health", timeout=2)
            return response.status_code == 200
        except Exception:
            return False
    
    def check_site_capabilities_service():
        """Check site capabilities service status with timeout."""
        try:
            response = requests.get("https://site-capabilities.srcnet.skao.int/health", timeout=2)
            return response.status_code == 200
        except Exception:
            return False
    
    def check_data_management_service():
        """Check data management service status with timeout."""
        try:
            response = requests.get("https://data-management.srcnet.skao.int/health", timeout=2)
            return response.status_code == 200
        except Exception:
            return False
    
    def check_iam_service():
        """Check IAM service status with timeout."""
        try:
            response = requests.get("https://ska-iam.stfc.ac.uk/login#!/home", timeout=2)
            return response.status_code == 200
        except Exception:
            return False
    
    def check_fts_service():
        """Check FTS service status with timeout."""
        try:
            # Try the base FTS endpoint without the fragment identifier
            response = requests.get("https://fts3-ska.scd.rl.ac.uk:8449/fts3/ftsmon/", timeout=2, verify=False)
            # Accept any successful response (2xx, 3xx status codes)
            return 200 <= response.status_code < 400
        except Exception:
            return False
    
    def check_rucio_service():
        """Check Rucio service status with timeout."""
        try:
            response = requests.get("https://rucio.srcnet.skao.int/health", timeout=2)
            return response.status_code == 200
        except Exception:
            return False
    
    def check_gateway_service():
        """Check Gateway service status with timeout."""
        try:
            response = requests.get("https://gateway-test.srcdev.skao.int/", timeout=2)
            return response.status_code == 200
        except Exception:
            return False
    
    def check_gatekeeper_service():
        """Check Gatekeeper service status with timeout."""
        try:
            response = requests.get("https://gatekeeper.srcnet.skao.int/echo", timeout=2)
            return response.status_code == 200
        except Exception:
            return False
    
    def check_canfar_service():
        """Check CANFAR service status with timeout."""
        try:
            response = requests.get("https://canfar.srcnet.skao.int/science-portal/", timeout=2)
            return response.status_code == 200
        except Exception:
            return False
    
    def check_soda_service():
        """Check SODA service status with timeout."""
        try:
            response = requests.get("https://gatekeeper.srcnet.skao.int/soda/ska/dataset/soda", timeout=2)
            return response.status_code == 200
        except Exception:
            return False
    
    def check_prepare_data_service():
        """Check Prepare Data service status with timeout."""
        try:
            response = requests.get("https://gatekeeper.srcnet.skao.int/preparedata", timeout=2)
            return response.status_code == 200
        except Exception:
            return False
    
    async def check_service_async(service_name, check_func):
        """Check a service asynchronously with proper timeout handling."""
        try:
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor() as executor:
                future = loop.run_in_executor(executor, check_func)
                result = await asyncio.wait_for(future, timeout=3.0)
                return {"status": "online" if result else "offline", "error": None}
        except asyncio.TimeoutError:
            return {"status": "offline", "error": "Timeout after 3 seconds"}
        except Exception as e:
            return {"status": "offline", "error": str(e)}
    
    try:
        # Run all checks concurrently
        results = await asyncio.gather(
            check_service_async("auth", check_auth_service),
            check_service_async("permissions", check_permissions_service),
            check_service_async("site-capabilities", check_site_capabilities_service),
            check_service_async("data-management", check_data_management_service),
            check_service_async("iam", check_iam_service),
            check_service_async("fts", check_fts_service),
            check_service_async("rucio", check_rucio_service),
            check_service_async("gateway", check_gateway_service),
            check_service_async("gatekeeper", check_gatekeeper_service),
            check_service_async("canfar", check_canfar_service),
            check_service_async("soda", check_soda_service),
            check_service_async("prepare-data", check_prepare_data_service),
            return_exceptions=True
        )
        
        # Handle any exceptions from gather
        service_names = [
            "auth", "permissions", "site-capabilities", "data-management", "iam", "fts", 
            "rucio", "gateway", "gatekeeper", "canfar", "soda", "prepare-data"
        ]
        
        status_dict = {}
        for i, result in enumerate(results):
            service_name = service_names[i]
            if isinstance(result, Exception):
                status_dict[service_name] = {"status": "offline", "error": str(result)}
            else:
                status_dict[service_name] = result
        
        return status_dict
        
    except Exception as e:
        logging.error(f"Error checking API status: {e}")
        # Return all services as offline if there's a general error
        return {
            "auth": {"status": "offline", "error": "Failed to check status"},
            "permissions": {"status": "offline", "error": "Failed to check status"},
            "site-capabilities": {"status": "offline", "error": "Failed to check status"},
            "data-management": {"status": "offline", "error": "Failed to check status"},
            "iam": {"status": "offline", "error": "Failed to check status"},
            "fts": {"status": "offline", "error": "Failed to check status"},
            "rucio": {"status": "offline", "error": "Failed to check status"},
            "gateway": {"status": "offline", "error": "Failed to check status"},
            "gatekeeper": {"status": "offline", "error": "Failed to check status"},
            "canfar": {"status": "offline", "error": "Failed to check status"},
            "soda": {"status": "offline", "error": "Failed to check status"},
            "prepare-data": {"status": "offline", "error": "Failed to check status"}
        }


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