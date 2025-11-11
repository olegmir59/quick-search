from __future__ import annotations

from typing import Iterable, List

from .database import Database
from .models import Employee, chunked


class EmployeeRepository:
    def __init__(self, database: Database) -> None:
        self._db = database

    def create_schema(self) -> None:
        with self._db.transaction() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS employees (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    full_name TEXT NOT NULL,
                    birth_date TEXT NOT NULL,
                    gender TEXT NOT NULL CHECK(gender IN ('Male', 'Female'))
                )
                """
            )
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS ux_employees_fullname_birth
                ON employees (full_name, birth_date)
                """
            )

    def insert_employee(self, employee: Employee) -> bool:
        with self._db.transaction() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO employees (full_name, birth_date, gender)
                VALUES (?, ?, ?)
                """,
                employee.to_row(),
            )
            cursor = connection.execute("SELECT changes()")
            inserted = cursor.fetchone()[0]
            cursor.close()
        return inserted > 0

    def bulk_insert(self, employees: Iterable[Employee], batch_size: int = 10_000) -> int:
        inserted = 0
        with self._db.transaction() as connection:
            for batch in chunked(employees, batch_size):
                rows = [emp.to_row() for emp in batch]
                connection.executemany(
                    """
                    INSERT OR IGNORE INTO employees (full_name, birth_date, gender)
                    VALUES (?, ?, ?)
                    """,
                    rows,
                )
                cursor = connection.execute("SELECT changes()")
                inserted += cursor.fetchone()[0]
                cursor.close()
        return inserted

    def fetch_unique_sorted(self) -> List[Employee]:
        connection = self._db.connect()
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT e.full_name, e.birth_date, e.gender
            FROM employees e
            JOIN (
                SELECT MIN(id) AS min_id
                FROM employees
                GROUP BY full_name, birth_date
            ) uniq ON e.id = uniq.min_id
            ORDER BY e.full_name
            """
        )
        rows = cursor.fetchall()
        cursor.close()
        return [
            Employee.from_strings(full_name=row[0], birth_date=row[1], gender=row[2])
            for row in rows
        ]

    def fetch_male_surname_f(self) -> List[Employee]:
        connection = self._db.connect()
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT full_name, birth_date, gender
            FROM employees
            WHERE gender = 'Male'
              AND full_name LIKE 'F%'
            ORDER BY full_name
            """
        )
        rows = cursor.fetchall()
        cursor.close()
        return [
            Employee.from_strings(full_name=row[0], birth_date=row[1], gender=row[2])
            for row in rows
        ]

    def create_gender_surname_index(self) -> None:
        with self._db.transaction() as connection:
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_employees_gender_fullname
                ON employees (gender, full_name)
                """
            )

    def create_compressed_table(self) -> None:
        with self._db.transaction() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS compressed_employees (
                    full_name TEXT NOT NULL,
                    birth_date TEXT NOT NULL,
                    gender TEXT NOT NULL,
                    compressed_payload BLOB NOT NULL,
                    PRIMARY KEY (full_name, birth_date)
                ) WITHOUT ROWID
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_compressed_gender_fullname
                ON compressed_employees (gender, full_name)
                """
            )

    def replace_compressed_rows(
        self, rows: Iterable[tuple[str, str, str, bytes]]
    ) -> int:
        with self._db.transaction() as connection:
            connection.execute("DELETE FROM compressed_employees")
            connection.executemany(
                """
                INSERT INTO compressed_employees (full_name, birth_date, gender, compressed_payload)
                VALUES (?, ?, ?, ?)
                """,
                rows,
            )
            cursor = connection.execute("SELECT changes()")
            inserted = cursor.fetchone()[0]
            cursor.close()
        return inserted

    def fetch_compressed_subset(self) -> list[tuple[str, str, str, bytes]]:
        connection = self._db.connect()
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT full_name, birth_date, gender, compressed_payload
            FROM compressed_employees
            WHERE gender = 'Male'
              AND full_name LIKE 'F%'
            ORDER BY full_name
            """
        )
        rows = cursor.fetchall()
        cursor.close()
        return [(row[0], row[1], row[2], row[3]) for row in rows]

    def compressed_table_stats(self) -> tuple[int, int]:
        connection = self._db.connect()
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT COUNT(*), IFNULL(SUM(LENGTH(compressed_payload)), 0)
            FROM compressed_employees
            """
        )
        count, total_bytes = cursor.fetchone()
        cursor.close()
        return int(count), int(total_bytes)

