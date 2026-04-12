FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies needed by asyncpg
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies first (layer-cached)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Make the startup script executable
RUN chmod +x scripts/start.sh

# Expose the application port
EXPOSE 8000

# Run migrations then start the ASGI server
CMD ["scripts/start.sh"]
