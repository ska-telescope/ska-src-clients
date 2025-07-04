#!/usr/bin/env python3
"""
Simple test script to verify the backend setup.
"""

import requests
import json
import sys

BASE_URL = "http://localhost:8000"
API_BASE = f"{BASE_URL}/api/v1"

def test_health_endpoints():
    """Test basic health endpoints."""
    print("Testing health endpoints...")
    
    # Test root endpoint
    try:
        response = requests.get(f"{BASE_URL}/")
        print(f"✓ Root endpoint: {response.status_code}")
        print(f"  Response: {response.json()}")
    except Exception as e:
        print(f"✗ Root endpoint failed: {e}")
        return False
    
    # Test health endpoint
    try:
        response = requests.get(f"{BASE_URL}/health")
        print(f"✓ Health endpoint: {response.status_code}")
        print(f"  Response: {response.json()}")
    except Exception as e:
        print(f"✗ Health endpoint failed: {e}")
        return False
    
    return True

def test_auth_endpoints():
    """Test authentication endpoints."""
    print("\nTesting authentication endpoints...")
    
    # Test auth health
    try:
        response = requests.get(f"{API_BASE}/auth/health")
        print(f"✓ Auth health: {response.status_code}")
        print(f"  Response: {response.json()}")
    except Exception as e:
        print(f"✗ Auth health failed: {e}")
        return False
    
    # Test device flow (this will fail without proper config, but should return 500, not crash)
    try:
        response = requests.post(f"{API_BASE}/auth/device-flow", json={})
        print(f"✓ Device flow endpoint: {response.status_code}")
        if response.status_code == 500:
            print("  Expected error (no config): OK")
    except Exception as e:
        print(f"✗ Device flow failed: {e}")
        return False
    
    return True

def test_data_endpoints():
    """Test data endpoints."""
    print("\nTesting data endpoints...")
    
    # Test data health
    try:
        response = requests.get(f"{API_BASE}/data/health")
        print(f"✓ Data health: {response.status_code}")
        print(f"  Response: {response.json()}")
    except Exception as e:
        print(f"✗ Data health failed: {e}")
        return False
    
    return True

def test_site_endpoints():
    """Test site endpoints."""
    print("\nTesting site endpoints...")
    
    # Test site health
    try:
        response = requests.get(f"{API_BASE}/site/health")
        print(f"✓ Site health: {response.status_code}")
        print(f"  Response: {response.json()}")
    except Exception as e:
        print(f"✗ Site health failed: {e}")
        return False
    
    return True

def main():
    """Run all tests."""
    print("SKA SRC Web UI Backend Test")
    print("=" * 40)
    
    # Check if server is running
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code != 200:
            print("✗ Backend server is not responding properly")
            return 1
    except requests.exceptions.ConnectionError:
        print("✗ Backend server is not running. Please start it with:")
        print("  cd backend && uvicorn main:app --reload")
        return 1
    
    # Run tests
    tests = [
        test_health_endpoints,
        test_auth_endpoints,
        test_data_endpoints,
        test_site_endpoints
    ]
    
    all_passed = True
    for test in tests:
        if not test():
            all_passed = False
    
    print("\n" + "=" * 40)
    if all_passed:
        print("✓ All tests passed!")
        print("\nNext steps:")
        print("1. Configure your SRCNet config file")
        print("2. Set the SRCNET_CONFIG_PATH environment variable")
        print("3. Test authentication flow")
        return 0
    else:
        print("✗ Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 