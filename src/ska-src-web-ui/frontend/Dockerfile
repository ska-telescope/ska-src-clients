FROM node:18-alpine

# Set working directory
WORKDIR /app

# Copy package files first for better caching
COPY src/ska-src-web-ui/frontend/package*.json ./

# Install dependencies
RUN npm install

# Copy the frontend code
COPY src/ska-src-web-ui/frontend/ .

# Expose port
EXPOSE 3000

# Default command (can be overridden in docker-compose)
CMD ["npm", "start"] 