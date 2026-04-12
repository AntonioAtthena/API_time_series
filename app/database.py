"""
Async SQLAlchemy engine, session factory, and FastAPI dependency.

Uses SQLite + aiosqlite by default — no database server required.
The database file (financial.db) is created automatically on first run
in the directory where you launch uvicorn.
"""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from app.config import settings

# SQLite does not support pool_size / max_overflow — those are PostgreSQL-only.
# check_same_thread=False is required because aiosqlite runs SQLite in a
# thread pool; without it SQLite raises "objects created in a thread can only
# be used in that same thread".
_is_sqlite = settings.database_url.startswith("sqlite")

engine = create_async_engine(
    settings.database_url,
    echo=settings.env == "development",
    connect_args={"check_same_thread": False} if _is_sqlite else {},
    **({} if _is_sqlite else {"pool_size": 10, "max_overflow": 20, "pool_pre_ping": True}),
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    """Shared declarative base for all ORM models."""
    pass


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency that yields a database session per request."""
    async with AsyncSessionLocal() as session:
        yield session
