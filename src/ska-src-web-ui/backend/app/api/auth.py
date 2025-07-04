from fastapi import APIRouter, HTTPException, Depends
from typing import List
import logging

from app.models.auth import (
    DeviceFlowRequest, DeviceFlowResponse, TokenRequestRequest, TokenRequestResponse,
    TokenExchangeRequest, TokenExchangeResponse, TokenListResponse, TokenInspectResponse
)
from app.services.src_client import SRCClientService
from app.core.config import settings

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


@router.post("/token/complete/{device_code}", response_model=TokenRequestResponse)
async def complete_token_request(
    device_code: str,
    src_service: SRCClientService = Depends(get_src_service)
):
    """Complete a token request by polling for completion."""
    try:
        response = src_service.complete_token_request(device_code)
        return TokenRequestResponse(**response)
    except Exception as e:
        logging.error(f"Error completing token request: {e}")
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