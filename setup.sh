#!/bin/bash

# SKA SRC Web UI Backend Setup Script
# This script sets up the backend environment with all dependencies

set -e  # Exit on any error

echo "🚀 Setting up SKA SRC Web UI Backend..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    print_error "Python 3 is required but not installed."
    exit 1
fi

print_status "Python version: $(python3 --version)"

# Check if Node.js is available for frontend
if ! command -v node &> /dev/null; then
    print_warning "Node.js is required for the frontend but not installed."
    print_warning "Please install Node.js (version 14 or higher) to use the frontend."
    print_warning "You can still use the backend without the frontend."
else
    print_status "Node.js version: $(node --version)"
fi

# Check if npm is available
if ! command -v npm &> /dev/null; then
    print_warning "npm is required for the frontend but not installed."
    print_warning "Please install npm to use the frontend."
else
    print_status "npm version: $(npm --version)"
fi

# Check if we're in the right directory
if [ ! -f "pyproject.toml" ]; then
    print_error "Please run this script from the project root directory (ska-src-clients)"
    exit 1
fi

# Get the project root directory
PROJECT_ROOT="$(pwd)"
print_status "Project root: $PROJECT_ROOT"

# Remove existing virtual environment if it exists
if [ -d "$PROJECT_ROOT/venv" ]; then
    print_status "Removing existing virtual environment..."
    rm -rf "$PROJECT_ROOT/venv"
fi

# Create new virtual environment in project root
print_status "Creating virtual environment in project root..."
python3 -m venv venv

# Activate virtual environment
print_status "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
print_status "Upgrading pip..."
pip install --upgrade pip

# Install backend requirements first (to avoid conflicts)
print_status "Installing backend dependencies..."
pip install -r src/ska-src-web-ui/backend/requirements-backend.txt

# Build ska-src-clients package
print_status "Building ska-src-clients package..."

# Check if poetry is available
if ! command -v poetry &> /dev/null; then
    print_warning "Poetry not found. Installing poetry..."
    pip install poetry
fi

# Build the package
poetry build

# Install the built package with all dependencies
print_status "Installing ska-src-clients package with dependencies..."

# Find the wheel file dynamically - from project root, dist is at ./dist
WHEEL_FILE=$(find dist -name "ska_src_clients-*.whl" -type f 2>/dev/null | head -1)
if [ -z "$WHEEL_FILE" ]; then
    print_error "No wheel file found in ./dist/"
    print_error "Current directory: $(pwd)"
    print_error "Contents of ./dist: $(ls -la dist 2>/dev/null || echo 'Directory not accessible')"
    print_error "Looking for wheel file in: ./dist/"
    exit 1
fi

print_status "Installing wheel file: $WHEEL_FILE"

# Install with all required index URLs
pip install "$WHEEL_FILE" \
    --index-url https://pypi.org/simple \
    --extra-index-url https://gitlab.com/api/v4/projects/48376510/packages/pypi/simple \
    --extra-index-url https://gitlab.com/api/v4/projects/48060714/packages/pypi/simple \
    --extra-index-url https://artefact.skao.int/repository/pypi-internal/simple

# Test CLI installation
print_status "Testing CLI installation..."
python -c "from ska_src_clients.cli.oper import cli; print('✅ CLI import successful!')"

# Test backend installation
print_status "Testing backend installation..."
cd src/ska-src-web-ui/backend
python -c "from app.services.src_client import SRCClientService; print('✅ Backend import successful!')"
cd "$PROJECT_ROOT"

print_status "Setting up .env file with correct SRCNET_CONFIG_PATH..."
print_status "Current directory: $(pwd)"
print_status "Checking if setup_env.py exists: $(ls -la src/ska-src-web-ui/backend/setup_env.py 2>/dev/null || echo 'File not found')"
python src/ska-src-web-ui/backend/setup_env.py

# Setup frontend if Node.js and npm are available
if command -v node &> /dev/null && command -v npm &> /dev/null; then
    print_status "Setting up frontend..."
    cd src/ska-src-web-ui/frontend
    
    if [ -d "node_modules" ]; then
        print_status "Frontend dependencies already installed, skipping..."
    else
        print_status "Installing frontend dependencies..."
        npm install
    fi
    
    cd "$PROJECT_ROOT"
    print_status "✅ Frontend setup completed!"
else
    print_warning "Skipping frontend setup - Node.js or npm not available"
fi

print_status "✅ Setup completed successfully!"
echo ""
echo "To start the backend server:"
echo "  source venv/bin/activate"
echo "  cd src/ska-src-web-ui/backend"
echo "  python -m uvicorn main:app --reload --port 8000"
echo ""
echo "To start the frontend (in a new terminal):"
echo "  cd src/ska-src-web-ui/frontend"
echo "  npm start"
echo ""
echo "To use the CLI tools:"
echo "  source venv/bin/activate"
echo "  ska-src-clients --help"
echo ""
echo "Access points:"
echo "  - Frontend: http://localhost:3000"
echo "  - Backend API: http://localhost:8000"
echo "  - API Docs: http://localhost:8000/docs"
echo "  - Health check: http://localhost:8000/api/v1/auth/health" 