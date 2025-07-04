# SKA SRC Web UI

A lightweight browser-based UI for the SKA SRC CLI tools, providing an intuitive interface for data management, site operations, and authentication.

This project is part of the `ska-src-clients` repository and provides a web interface for the existing CLI tools.

## Project Structure

```
ska-src-clients/
├── setup.sh               # Project setup script (creates venv, installs dependencies)
├── src/
│   ├── ska_src_clients/    # Original CLI source code
│   └── ska-src-web-ui/     # Web UI project
│       ├── backend/        # FastAPI backend
│       │   ├── app/
│       │   │   ├── api/    # API routes
│       │   │   ├── core/   # Core configuration
│       │   │   ├── models/ # Pydantic models
│       │   │   └── services/ # Business logic
│       │   ├── requirements-backend.txt
│       │   └── main.py
│       ├── frontend/       # SvelteKit frontend (Phase 2)
│       │   ├── src/
│       │   │   ├── lib/
│       │   │   ├── routes/
│       │   │   └── components/
│       │   ├── package.json
│       │   └── svelte.config.js
│       └── docker-compose.yml # Development environment
├── bin/                    # CLI executables
└── ...                     # Other CLI project files
```

## Features

- **Authentication**: OIDC device flow integration
- **Data Management**: Upload, download, and manage data files
- **Site Operations**: Monitor and manage site resources
- **Token Management**: View and exchange authentication tokens
- **Real-time Updates**: Live status monitoring

## Development Setup

### Backend Setup
```bash
# From project root
./setup.sh  # This creates venv in project root and installs dependencies
source venv/bin/activate  # On Windows: venv\Scripts\activate
cd src/ska-src-web-ui/backend
python main.py
# or: uvicorn main:app --reload
```

### Frontend Setup
```bash
cd frontend
npm install
npm run dev
```

### Docker Setup
```bash
docker-compose up -d
```

## API Documentation

Once the backend is running, visit:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc 