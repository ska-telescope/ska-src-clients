#!/bin/bash

# SKA SRC Clients Docker CLI Helper Script
# This script provides easy access to CLI commands in the Docker container

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_help() {
    echo -e "${BLUE}SKA SRC Clients Docker CLI Helper${NC}"
    echo ""
    echo "Usage: $0 [COMMAND] [ARGS...]"
    echo ""
    echo "Commands:"
    echo "  build          Build the Docker image"
    echo "  run [ARGS]     Run CLI command (default: --help)"
    echo "  shell          Open shell in container"
    echo "  clean          Remove containers and images"
    echo "  help           Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 build"
    echo "  $0 run site list"
    echo "  $0 run token list"
    echo "  $0 shell"
    echo ""
}

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    print_error "Docker is required but not installed."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    print_error "Docker Compose is required but not installed."
    exit 1
fi

# Check if we're in the right directory
if [ ! -f "docker-compose.yml" ]; then
    print_error "Please run this script from the project root directory"
    exit 1
fi

# Create tokens directory if it doesn't exist
mkdir -p tokens

case "${1:-help}" in
    "build")
        print_status "Building Docker image..."
        docker-compose build
        print_status "✅ Build completed!"
        ;;
    "run")
        shift
        if [ $# -eq 0 ]; then
            print_status "Running default command (--help)..."
            docker-compose run --rm ska-src-clients srcnet-oper --help
        else
            print_status "Running command: srcnet-oper $*"
            docker-compose run --rm ska-src-clients srcnet-oper "$@"
        fi
        ;;
    "shell")
        print_status "Opening shell in container..."
        docker-compose run --rm ska-src-clients bash
        ;;
    "clean")
        print_warning "Removing containers and images..."
        docker-compose down --rmi all --volumes --remove-orphans
        print_status "✅ Cleanup completed!"
        ;;
    "help"|*)
        print_help
        ;;
esac 