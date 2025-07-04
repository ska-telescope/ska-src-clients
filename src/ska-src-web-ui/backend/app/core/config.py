from pydantic import BaseSettings
from typing import Optional, List


class Settings(BaseSettings):
    """Application settings."""
    
    # API Configuration
    api_v1_str: str = "/api/v1"
    project_name: str = "SKA SRC Web UI"
    version: str = "1.0.0"
    
    # CORS Configuration
    backend_cors_origins: List[str] = [
        "http://localhost:3000",  # SvelteKit dev server
        "http://localhost:4173",  # SvelteKit preview
        "http://127.0.0.1:3000",
        "http://127.0.0.1:4173",
    ]
    
    # Authentication
    token_storage_path: str = "/tmp/srcnet/user"
    
    # SRCNet Configuration
    srcnet_config_path: Optional[str] = None

    class Config:
        env_prefix = ""
        env_file = ".env"
        case_sensitive = False
        fields = {
            "api_v1_str": {"env": "API_V1_STR"},
            "project_name": {"env": "PROJECT_NAME"},
            "version": {"env": "VERSION"},
            "backend_cors_origins": {"env": "BACKEND_CORS_ORIGINS"},
            "token_storage_path": {"env": "TOKEN_STORAGE_PATH"},
            "srcnet_config_path": {"env": "SRCNET_CONFIG_PATH"},
        }

settings = Settings() 