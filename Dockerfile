FROM python:3.12-slim

# Create a non-root user/group before any other work.
# Using explicit UID/GID 1001 makes the identity predictable across image rebuilds
# and avoids conflicts with common host UIDs (0=root, 1000=first user).
# --no-create-home: no home dir needed for a service account.
# --shell /sbin/nologin: prevents interactive login if the account is ever exposed.
RUN groupadd --gid 1001 appgroup \
 && useradd --uid 1001 --gid appgroup --no-create-home --shell /sbin/nologin appuser

# Set working directory (owned by root at this point — changed below)
WORKDIR /app

# Ensure the project root is always on sys.path so `from app.*` imports
# work regardless of how the container is invoked (Railway may not preserve CWD).
ENV PYTHONPATH=/app

# Install system dependencies needed to compile asyncpg (C extension).
# Removed in the same layer to keep the image lean.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies first (layer-cached when requirements.txt is unchanged)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code (.dockerignore excludes secrets, caches, and dev artefacts)
COPY . .

# Fix permissions and make the startup script executable in a single layer.
# chown transfers /app ownership to the non-root user; all subsequent writes
# by the process (e.g. SQLite file in dev) will succeed without root.
RUN chmod +x scripts/start.sh \
 && chown -R appuser:appgroup /app

# Drop root — all instructions and the running process from here on use appuser.
USER appuser

# Expose the application port
EXPOSE 8080

# Run migrations then start the ASGI server
CMD ["scripts/start.sh"]
