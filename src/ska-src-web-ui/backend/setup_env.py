#!/usr/bin/env python3
"""
Script to automatically set up the .env file for the SKA SRC Web UI backend.
"""

import os
import sys
from pathlib import Path


def setup_env_file():
    """Set up the .env file with the correct configuration path."""
    
    # Get the current directory (backend directory)
    backend_dir = Path(__file__).parent.absolute()
    
    # Calculate the path to the config file relative to the backend directory
    # From src/ska-src-web-ui/backend to etc/cfg/oper.yml
    config_path = backend_dir.parent.parent.parent / "etc" / "cfg" / "oper.yml"
    
    # Convert to absolute path
    config_path_absolute = config_path.absolute()
    
    # Check if config file exists
    if not config_path_absolute.exists():
        print(f"Error: Config file not found at {config_path_absolute}")
        print(f"Current backend directory: {backend_dir}")
        print(f"Looking for config at: {config_path}")
        sys.exit(1)
    
    # Create .env file content
    env_content = f"""# SKA SRC Web UI Backend Configuration

# API Configuration
API_V1_STR=/api/v1
PROJECT_NAME=SKA SRC Web UI
VERSION=1.0.0

# CORS Configuration
BACKEND_CORS_ORIGINS=["http://localhost:3000","http://localhost:4173","http://127.0.0.1:3000","http://127.0.0.1:4173"]

# Authentication
TOKEN_STORAGE_PATH=/tmp/srcnet/user

# SRCNet Configuration
SRCNET_CONFIG_PATH={config_path_absolute}
"""
    
    # Write .env file
    env_file_path = backend_dir / ".env"
    
    try:
        with open(env_file_path, 'w') as f:
            f.write(env_content)
        
        print(f"✅ .env file created successfully at {env_file_path}")
        print(f"📁 Config path set to: {config_path_absolute}")
        print(f"🔧 You can now start the backend without setting SRCNET_CONFIG_PATH manually")
        
    except Exception as e:
        print(f"Error creating .env file: {e}")
        sys.exit(1)


if __name__ == "__main__":
    setup_env_file() 