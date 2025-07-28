#!/bin/bash

# SKA SRC Clients Setup Script
# This script sets up the ska_src_clients package with CLI tools

set -e  # Exit on any error

echo "🚀 Setting up SKA SRC Clients..."

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

# Check if poetry is available
if ! command -v poetry &> /dev/null; then
    print_warning "Poetry not found. Installing poetry..."
    pip install poetry
fi

# Build the package
print_status "Building ska-src-clients package..."
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

# Test core library installation
print_status "Testing core library installation..."
python -c "from ska_src_clients.api.data import DataAPI; print('✅ Core library import successful!')"

print_status "✅ Setup completed successfully!"
echo ""
echo "To use the CLI tools:"
echo "  source venv/bin/activate"
echo "  srcnet-oper --help"
echo ""
echo "Available commands:"
echo "  srcnet-oper api --help"
echo "  srcnet-oper config --help"
echo "  srcnet-oper data --help"
echo "  srcnet-oper metadata --help"
echo "  srcnet-oper node --help"
echo "  srcnet-oper site --help"
echo "  srcnet-oper token --help"
echo "  srcnet-oper tui --help"
echo ""
echo "Configuration files are available in: etc/cfg/" 