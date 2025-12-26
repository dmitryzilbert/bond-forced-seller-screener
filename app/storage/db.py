from __future__ import annotations

import asyncio
import logging
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.dialects import sqlite as sqlite_dialect
from sqlalchemy.orm import declarative_base

from ..settings import get_settings

logger = logging.getLogger(__name__)

Base = declarative_base()
_engine = None
_session_factory = None


def get_engine():
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_async_engine(settings.database_url, echo=False, future=True)
    return _engine


def async_session_factory() -> async_sessionmaker[AsyncSession]:
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(bind=get_engine(), expire_on_commit=False)
    return _session_factory


async def init_db():
    engine = get_engine()
    from .schema import InstrumentORM, OrderbookSnapshotORM, EventORM

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        if engine.dialect.name == "sqlite":
            await conn.run_sync(ensure_sqlite_schema)
    logger.info("DB initialized at %s", engine.url)


def ensure_sqlite_schema(conn) -> None:
    if conn.dialect.name != "sqlite":
        return

    from .schema import InstrumentORM

    added_columns = []
    table_name = InstrumentORM.__tablename__
    existing_columns = {
        row[1] for row in conn.exec_driver_sql(f"PRAGMA table_info({table_name})").fetchall()
    }

    custom_columns = {
        "instrument_uid": "TEXT",
        "ticker": "TEXT",
        "class_code": "TEXT",
        "floating_coupon_flag": "INTEGER NOT NULL DEFAULT 0",
    }

    for column in InstrumentORM.__table__.columns:
        if column.name in existing_columns or column.primary_key:
            continue

        if column.name in custom_columns:
            ddl = f"ALTER TABLE {table_name} ADD COLUMN {column.name} {custom_columns[column.name]}"
            conn.exec_driver_sql(ddl)
            added_columns.append(f"{table_name}.{column.name}")
            continue

        column_type = column.type.compile(dialect=sqlite_dialect.dialect())
        default_clause = ""
        if column.server_default is not None:
            default_clause = f" DEFAULT {column.server_default.arg}"
        elif not column.nullable:
            if column_type.upper().startswith("BOOL"):
                default_value = "0"
            elif column_type.upper().startswith(("INT", "FLOAT", "REAL", "NUMERIC")):
                default_value = "0"
            elif column_type.upper().startswith("DATETIME"):
                default_value = "'1970-01-01 00:00:00'"
            elif column_type.upper().startswith("DATE"):
                default_value = "'1970-01-01'"
            elif column_type.upper().startswith("JSON"):
                default_value = "'{}'"
            else:
                default_value = "''"
            default_clause = f" DEFAULT {default_value}"

        nullable_clause = "" if column.nullable else " NOT NULL"
        ddl = (
            f"ALTER TABLE {table_name} "
            f"ADD COLUMN {column.name} {column_type}{nullable_clause}{default_clause}"
        )
        conn.exec_driver_sql(ddl)
        added_columns.append(f"{table_name}.{column.name}")

    if added_columns:
        logger.info("SQLite schema migrated: added columns %s", ", ".join(added_columns))


async def close_db():
    global _engine
    global _session_factory
    if _engine is not None:
        await _engine.dispose()
        logger.info("DB engine disposed")
        _engine = None
        _session_factory = None
