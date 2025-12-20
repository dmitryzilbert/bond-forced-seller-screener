import asyncio
import sqlite3
from typing import Any, Iterable

__all__ = ["connect", "Connection", "Cursor"]

DatabaseError = sqlite3.DatabaseError
Error = sqlite3.Error
IntegrityError = sqlite3.IntegrityError
NotSupportedError = sqlite3.NotSupportedError
OperationalError = sqlite3.OperationalError
ProgrammingError = sqlite3.ProgrammingError
sqlite_version = sqlite3.sqlite_version
sqlite_version_info = sqlite3.sqlite_version_info


class Cursor:
    def __init__(self, cursor: sqlite3.Cursor):
        self._cursor = cursor

    async def execute(self, sql: str, parameters: Iterable[Any] | None = None):
        self._cursor.execute(sql, parameters or [])
        return self

    async def executemany(self, sql: str, seq_of_parameters):
        self._cursor.executemany(sql, seq_of_parameters)
        return self

    async def fetchone(self):
        return self._cursor.fetchone()

    async def fetchall(self):
        return self._cursor.fetchall()

    async def close(self):
        self._cursor.close()

    @property
    def description(self):
        return self._cursor.description

    @property
    def lastrowid(self):
        return self._cursor.lastrowid

    @property
    def rowcount(self):
        return self._cursor.rowcount

    def __aiter__(self):
        return self

    async def __anext__(self):
        row = self._cursor.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row


class Connection:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self.isolation_level = conn.isolation_level

    async def execute(self, sql: str, parameters: Iterable[Any] | None = None):
        cursor = self._conn.execute(sql, parameters or [])
        return Cursor(cursor)

    async def executemany(self, sql: str, seq_of_parameters):
        cursor = self._conn.executemany(sql, seq_of_parameters)
        return Cursor(cursor)

    async def cursor(self):
        return Cursor(self._conn.cursor())

    async def commit(self):
        self._conn.commit()

    async def rollback(self):
        self._conn.rollback()

    async def close(self):
        self._conn.close()

    async def create_function(self, *args, **kwargs):
        self._conn.create_function(*args, **kwargs)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc:
            await self.rollback()
        else:
            await self.commit()
        await self.close()


class _ConnectAwaitable:
    def __init__(self, database: str, kwargs: dict[str, Any]):
        self.database = database
        self.kwargs = kwargs
        self.daemon = False

    def __await__(self):
        async def _coro():
            loop = asyncio.get_running_loop()

            def _connect():
                return sqlite3.connect(
                    self.database,
                    timeout=self.kwargs.get("timeout", 5),
                    check_same_thread=False,
                    uri=self.kwargs.get("uri", False),
                )

            conn = await loop.run_in_executor(None, _connect)
            return Connection(conn)

        return _coro().__await__()


def connect(database: str, **kwargs) -> _ConnectAwaitable:
    return _ConnectAwaitable(database, kwargs)
