# SKA SRC Web UI Backend

A FastAPI backend that provides a REST API for the SKA SRC CLI tools, enabling web-based access to data management, site operations, and authentication features.

## 🚀 Quick Start

### Option 1: Automated Setup (Recommended)

```bash
# Navigate to the backend directory
cd ska-src-web-ui/backend

# Run the automated setup script
./setup.sh
```

This script will:
- Create a virtual environment
- Install all dependencies with correct versions
- Build and install the ska-src-clients package
- Test the installation
- Create configuration files

## Starting the Backend

After setup, activate the environment and start the backend:

```bash
source venv/bin/activate
cd src/ska-src-web-ui/backend
uvicorn main:app --reload --port 8000
```

## 📚 API Documentation

Once running, visit:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **Health Check**: http://localhost:8000/health

## 🔧 Configuration

Edit the `.env` file to configure:

```bash
# Copy the example configuration
cp env.example .env

# Edit the configuration
nano .env
```

Key configuration options:
- `SRCNET_CONFIG_PATH`: Path to your SRCNet configuration file
- `TOKEN_STORAGE_PATH`: Directory for storing authentication tokens
- `BACKEND_CORS_ORIGINS`: Allowed CORS origins for frontend

## 🐳 Docker Deployment

### Development
```bash
docker-compose up -d backend
```

### Production
```bash
# Build the production image
docker build -f Dockerfile.prod -t ska-src-web-ui-backend .

# Run the container
docker run -p 8000:8000 ska-src-web-ui-backend
```

## 🧪 Testing

```bash
# Run tests
pytest

# Run with coverage
pytest --cov=app
```

## 📁 Project Structure

```
backend/
├── app/
│   ├── api/           # API route handlers
│   ├── core/          # Core configuration
│   ├── models/        # Pydantic models
│   └── services/      # Business logic
├── requirements-backend.txt  # Backend-specific dependencies
├── setup.sh           # Automated setup script
├── Dockerfile.prod    # Production Dockerfile
└── main.py           # FastAPI application entry point
```

## 🔍 Troubleshooting

### Common Issues

1. **Import Errors**: Run `./setup.sh` to ensure all dependencies are correctly installed
2. **Port Already in Use**: Use `--port 8001` or kill the existing process
3. **Configuration Errors**: Check your `.env` file and ensure `SRCNET_CONFIG_PATH` is correct

### Dependency Conflicts

If you encounter dependency conflicts:

```bash
# Remove existing environment
rm -rf venv

# Re-run setup
./setup.sh
```

## 🤝 Contributing

1. Follow the existing code style
2. Add tests for new features
3. Update documentation as needed
4. Ensure all tests pass before submitting

## 📄 License

This project is part of the SKA SRC Clients repository and follows the same license terms. 