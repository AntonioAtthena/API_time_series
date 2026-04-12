"""
Alembic environment configuration.

Rationale — async engine via run_sync:
    Our application uses an async SQLAlchemy engine (asyncpg), but Alembic's
    default migration runner is synchronous.  The recommended pattern is to
    wrap the synchronous Alembic context in an async function and run it via
    asyncio.run(), using engine.sync_engine for the sync connection that
    Alembic expects internally.
"""

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import create_async_engine

# Import the Base and all models so Alembic can detect schema changes.
from app.database import Base
from app.models import Datapoint  # noqa: F401 — registers the model with Base
from app.config import settings

# Alembic Config object provides access to the .ini values.
config = context.config

# Set up Python logging from the alembic.ini [loggers] section.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# The metadata Alembic uses for autogenerate (--autogenerate).
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode (generates SQL without a live connection).

    Useful for generating migration scripts to review before applying.
    """
    context.configure(
        url=settings.database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Run migrations in 'online' mode against a live database connection."""
    connectable = create_async_engine(
        settings.database_url,
        poolclass=pool.NullPool,  # NullPool: no connection reuse; safe for migrations
    )

    async with connectable.connect() as connection:
        await connection.run_sync(_run_sync_migrations)

    await connectable.dispose()


def _run_sync_migrations(sync_connection):
    """Execute Alembic migrations using a synchronous connection wrapper.

    Args:
        sync_connection: The synchronous connection provided by
            AsyncConnection.run_sync().
    """
    context.configure(connection=sync_connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
