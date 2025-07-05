#!/usr/bin/env python3
import json
import base64
import sys

def decode_jwt_payload(token):
    """Decode JWT payload without signature verification"""
    try:
        # Split the token into parts
        parts = token.split('.')
        if len(parts) != 3:
            print(f"Invalid JWT format: {len(parts)} parts")
            return None
        
        # Decode the payload (second part)
        payload_b64 = parts[1]
        # Add padding if needed
        payload_b64 += '=' * (4 - len(payload_b64) % 4)
        
        payload_json = base64.urlsafe_b64decode(payload_b64)
        payload = json.loads(payload_json)
        return payload
    except Exception as e:
        print(f"Error decoding token: {e}")
        return None

# Read the token files
token_files = [
    "/tmp/srcnet/user/5e670c99-8239-49ec-918c-f84fcfcdc230.token",
    "/tmp/srcnet/user/6cab8734-734e-4eb1-8590-9069236ab6ff.token"
]

for i, token_file in enumerate(token_files, 1):
    print(f"\n=== Token {i} ===")
    try:
        with open(token_file, 'r') as f:
            token_data = json.load(f)
        
        access_token = token_data.get('access_token', '')
        payload = decode_jwt_payload(access_token)
        
        if payload:
            print(f"File: {token_file}")
            print(f"Audience (aud): {payload.get('aud', 'N/A')}")
            print(f"Expiration (exp): {payload.get('exp', 'N/A')}")
            print(f"Issued at (iat): {payload.get('iat', 'N/A')}")
            print(f"Token ID (jti): {payload.get('jti', 'N/A')}")
            
            # Convert timestamps to readable format
            import datetime
            if 'exp' in payload:
                exp_time = datetime.datetime.fromtimestamp(payload['exp'])
                print(f"Expires at: {exp_time}")
            if 'iat' in payload:
                iat_time = datetime.datetime.fromtimestamp(payload['iat'])
                print(f"Issued at: {iat_time}")
        
    except Exception as e:
        print(f"Error reading {token_file}: {e}") 