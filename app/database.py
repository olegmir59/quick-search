from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Iterable, Tuple


class Database:
    """Lightweight SQLite database wrapper."""

    def __init__(self, path: str) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._connection: sqlite3.Connection | None = None

    def connect(self) -> sqlite3.Connection:
        if self._connection is None:
            self._connection = sqlite3.connect(
                self._path.as_posix(),
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
                check_same_thread=False,
            )
            self._configure(self._connection)
        return self._connection

    @staticmethod
    def _configure(connection: sqlite3.Connection) -> None:
        cursor = connection.cursor()
        pragmas: Iterable[Tuple[str, str]] = [
            ("foreign_keys", "ON"),
            ("journal_mode", "WAL"),
            ("synchronous", "NORMAL"),
            ("temp_store", "MEMORY"),
            ("cache_size", "-65536"),  # 64 MB cache
        ]
        for pragma, value in pragmas:
            cursor.execute(f"PRAGMA {pragma}={value}")
        cursor.close()

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
        connection = self.connect()
        try:
            connection.execute("BEGIN")
            yield connection
        except Exception:
            connection.rollback()
            raise
        else:
            connection.commit()

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

