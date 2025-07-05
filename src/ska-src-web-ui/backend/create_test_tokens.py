#!/usr/bin/env python3
"""
Script to create artificial tokens for testing the SKA SRC Web UI
when Auth and Permissions APIs are down.
"""

import json
import os
import uuid
import time
import base64
from datetime import datetime, timedelta

def create_artificial_token(service_name, expires_in_hours=24):
    """Create an artificial token for testing."""
    # Create a future expiration time
    expires_at = int(time.time()) + (expires_in_hours * 3600)
    issued_at = int(time.time())
    
    # Create JWT header
    header = {
        "alg": "HS256",
        "typ": "JWT"
    }
    
    # Create JWT payload
    payload = {
        "aud": service_name,
        "exp": expires_at,
        "iat": issued_at,
        "jti": str(uuid.uuid4()),
        "sub": "test-user",
        "iss": "https://authn.srcnet.skao.int"
    }
    
    # Create a proper JWT token (header.payload.signature)
    # Note: This is a fake signature, but it has the right format
    header_b64 = base64.urlsafe_b64encode(json.dumps(header).encode()).rstrip(b'=').decode()
    payload_b64 = base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b'=').decode()
    fake_signature = base64.urlsafe_b64encode(b"fake_signature_for_testing").rstrip(b'=').decode()
    
    fake_access_token = f"{header_b64}.{payload_b64}.{fake_signature}"
    
    # Create a fake refresh token with same format
    refresh_payload = {
        "aud": service_name,
        "exp": expires_at + (7 * 24 * 3600),  # 7 days later
        "iat": issued_at,
        "jti": str(uuid.uuid4()),
        "sub": "test-user",
        "iss": "https://authn.srcnet.skao.int"
    }
    
    refresh_payload_b64 = base64.urlsafe_b64encode(json.dumps(refresh_payload).encode()).rstrip(b'=').decode()
    fake_refresh_signature = base64.urlsafe_b64encode(b"fake_refresh_signature").rstrip(b'=').decode()
    fake_refresh_token = f"{header_b64}.{refresh_payload_b64}.{fake_refresh_signature}"
    
    # Create the token data structure
    token_data = {
        "access_token": fake_access_token,
        "refresh_token": fake_refresh_token,
        "token_type": "Bearer",
        "expires_in": expires_in_hours * 3600
    }
    
    return token_data

def main():
    """Create test tokens for different services."""
    # Ensure the token directory exists
    token_dir = "/tmp/srcnet/user"
    os.makedirs(token_dir, exist_ok=True)
    
    # Define services to create tokens for
    services = [
        "authn-api",
        "permissions-api", 
        "site-capabilities-api",
        "data-management-api"
    ]
    
    print("Creating artificial tokens for testing...")
    
    for service in services:
        # Create token data
        token_data = create_artificial_token(service)
        
        # Generate a unique filename
        filename = f"{uuid.uuid4()}.token"
        filepath = os.path.join(token_dir, filename)
        
        # Write token to file
        with open(filepath, 'w') as f:
            json.dump(token_data, f, indent=2)
        
        print(f"Created token for {service}: {filename}")
        
        # Also create a second token for some services to test multiple tokens
        if service in ["authn-api", "site-capabilities-api"]:
            token_data2 = create_artificial_token(service, expires_in_hours=48)
            filename2 = f"{uuid.uuid4()}.token"
            filepath2 = os.path.join(token_dir, filename2)
            
            with open(filepath2, 'w') as f:
                json.dump(token_data2, f, indent=2)
            
            print(f"Created second token for {service}: {filename2}")
    
    print(f"\nTokens created in: {token_dir}")
    print("You can now test the UI with these artificial tokens!")
    print("\nTo simulate Auth and Permissions APIs being down:")
    print("1. Stop the backend server")
    print("2. Start the frontend only")
    print("3. The system status should show Auth and Permissions as offline")
    print("4. Token exchange should be disabled")
    print("5. But you should still see the artificial tokens in the UI")

if __name__ == "__main__":
    main() 