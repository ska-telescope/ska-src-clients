from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class DeviceFlowRequest(BaseModel):
    """Request to start device flow authentication."""
    pass


class DeviceFlowResponse(BaseModel):
    """Response from device flow authentication."""
    device_code: str
    user_code: str
    verification_uri: str
    verification_uri_complete: str
    expires_in: int
    interval: int
    message: str


class TokenRequestRequest(BaseModel):
    """Request to start a complete token request flow."""
    max_polling_attempts: int = 60
    wait_between_polling_s: int = 5


class TokenRequestResponse(BaseModel):
    """Response from token request flow."""
    success: bool
    message: str
    device_code: Optional[str] = None
    user_code: Optional[str] = None
    verification_uri: Optional[str] = None
    verification_uri_complete: Optional[str] = None


class TokenExchangeRequest(BaseModel):
    """Request to exchange token for a service."""
    service_name: str
    version: str = "latest"


class TokenExchangeResponse(BaseModel):
    """Response from token exchange."""
    success: bool
    message: str
    service_name: Optional[str] = None


class TokenInfo(BaseModel):
    """Information about a stored token."""
    service_name: str
    access_token: str  # Truncated for security
    expires_utc: str
    expires_local: str
    path_on_disk: Optional[str] = None
    has_refresh_token: bool


class TokenListResponse(BaseModel):
    """Response containing list of available tokens."""
    tokens: List[TokenInfo]


class TokenInspectResponse(BaseModel):
    """Response containing detailed token information."""
    token_data: dict 