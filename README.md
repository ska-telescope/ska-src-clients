# SKA SRC Clients

A Python client library and CLI tools for interacting with the SKA SRC (Source) APIs.

## Features

- **CLI Tools**: Command-line interface for SRC operations
- **Core Library**: Python library for programmatic access
- **Docker Support**: Containerized environment for easy deployment
- **Multiple APIs**: Support for Site Capabilities, Data Management, Authentication, and Permissions APIs

## Installation

### Option 1: Local Installation (Recommended for Development)

Use the provided setup script for a complete local installation:

```bash
# Make the script executable
chmod +x setup.sh

# Run the setup script
./setup.sh
```

The setup script will:
- Create a Python virtual environment
- Install all dependencies using Poetry
- Build and install the `ska_src_clients` package
- Test the installation

### Option 2: Docker Installation (Recommended for Production)

Use Docker for a containerized environment:

```bash
# Make the helper script executable
chmod +x docker-cli.sh

# Build the Docker image
./docker-cli.sh build

# Test the installation
./docker-cli.sh run --help
```

## Usage

### Local Installation

After running `./setup.sh`, activate the virtual environment and use the CLI:

```bash
# Activate virtual environment
source venv/bin/activate

# Get help
srcnet-oper --help

# List available commands
srcnet-oper --help

# Example commands
srcnet-oper site list
srcnet-oper token list
srcnet-oper data upload --help
```

### Docker Installation

Use the Docker helper script for all CLI operations:

```bash
# Get help
./docker-cli.sh run --help

# List sites
./docker-cli.sh run site list

# List tokens
./docker-cli.sh run token list

# Data operations
./docker-cli.sh run data upload --help

# Interactive shell
./docker-cli.sh shell

# Clean up containers
./docker-cli.sh clean
```

## Available Commands

The `srcnet-oper` CLI provides the following command groups:

- **`api`** - Generic operations against the SRCNet APIs
- **`config`** - Generic configuration operations
- **`data`** - Data operations (upload, download, etc.)
- **`metadata`** - Metadata operations for managing object metadata
- **`node`** - Operations related to nodes
- **`site`** - Operations related to sites
- **`token`** - Token operations (request, exchange, list, etc.)
- **`tui`** - Launch the Text User Interface

## Configuration

Configuration files are located in `etc/cfg/`:

- `oper.yml` - Production configuration
- `oper-dev.yml` - Development configuration
- `oper-dev-rem.yml` - Remote development configuration

The configuration includes:
- API endpoints for all SRC services
- Storage client configurations
- Core service URLs

## Docker Helper Script Commands

The `docker-cli.sh` script provides the following commands:

- **`build`** - Build the Docker image
- **`run [ARGS]`** - Run CLI command (defaults to `--help`)
- **`shell`** - Open interactive shell in container
- **`clean`** - Remove containers and images
- **`help`** - Show help

## Examples

### Basic Operations

```bash
# List all sites
./docker-cli.sh run site list

# Get information about a specific site
./docker-cli.sh run site get <site-id>

# List available tokens
./docker-cli.sh run token list

# Request a new token
./docker-cli.sh run token request

# List storage areas
./docker-cli.sh run site storage list
```

### Data Operations

```bash
# Upload a file
./docker-cli.sh run data upload <file-path> --namespace <namespace>

# Download a file
./docker-cli.sh run data download <namespace> <filename>

# List files in a namespace
./docker-cli.sh run data list <namespace>
```

### Interactive Development

```bash
# Open shell for interactive use
./docker-cli.sh shell

# Inside the shell, you can run any command
srcnet-oper --help
srcnet-oper site list
```

## Troubleshooting

### Common Issues

1. **Permission Denied**: Make sure the scripts are executable
   ```bash
   chmod +x setup.sh docker-cli.sh
   ```

2. **Docker Build Fails**: Check that Docker is running and you have sufficient disk space

3. **Import Errors**: Ensure the virtual environment is activated (for local installation)
   ```bash
   source venv/bin/activate
   ```

4. **Configuration Issues**: Verify your configuration files in `etc/cfg/`

### Getting Help

- Run `./docker-cli.sh run --help` for general CLI help
- Run `./docker-cli.sh run <command> --help` for specific command help
- Use `./docker-cli.sh shell` for interactive debugging

## Development

### Local Development

1. Clone the repository
2. Run `./setup.sh` for local installation
3. Activate the virtual environment: `source venv/bin/activate`
4. Make your changes
5. Test with `srcnet-oper --help`

### Docker Development

1. Clone the repository
2. Run `./docker-cli.sh build`
3. Use `./docker-cli.sh shell` for interactive development
4. Test changes with `./docker-cli.sh run <command>`

## License

BSD-3-Clause License
