FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the entire project
COPY . /app/
# Copy the config files
RUN mkdir -p /app/etc/cfg
COPY ./etc/cfg/oper.yml /app/etc/cfg/oper.yml
COPY ./etc/cfg/oper-dev.yml /app/etc/cfg/oper-dev.yml

# Install poetry
RUN pip install poetry

# Build the ska-src-clients package
RUN poetry build

# Install backend requirements first
RUN pip install --upgrade pip
RUN pip install -r src/ska-src-web-ui/backend/requirements-backend.txt

# Install the built package with all dependencies
RUN WHEEL_FILE=$(find dist -name "ska_src_clients-*.whl" -type f | head -1) && \
    pip install "$WHEEL_FILE" \
    --index-url https://pypi.org/simple \
    --extra-index-url https://gitlab.com/api/v4/projects/48376510/packages/pypi/simple \
    --extra-index-url https://gitlab.com/api/v4/projects/48060714/packages/pypi/simple \
    --extra-index-url https://artefact.skao.int/repository/pypi-internal/simple

# Set up .env file with correct SRCNET_CONFIG_PATH
RUN python src/ska-src-web-ui/backend/setup_env.py

# Create necessary directories
RUN mkdir -p /tmp/srcnet/user

# Set working directory to backend
WORKDIR /app/src/ska-src-web-ui/backend

# Expose port
EXPOSE 8000

# Default command (can be overridden in docker-compose)
CMD ["python", "main.py"] 