from __future__ import annotations

import argparse
import gzip
import json
import sys
import time
from pathlib import Path

from app.database import Database
from app.models import Employee
from app.repository import EmployeeRepository
from app.utils import EmployeeDataGenerator

DB_PATH = Path("employees.db")


def ensure_utf8() -> None:
    if sys.stdout.encoding and sys.stdout.encoding.lower() == "utf-8":
        return
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
        sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except AttributeError:
        pass


def mode_create_table(repository: EmployeeRepository) -> None:
    repository.create_schema()
    print("Employees table is ready.")


def mode_create_employee(repository: EmployeeRepository, params: list[str]) -> None:
    if len(params) < 3:
        raise ValueError("Mode 2 requires parameters: <Full Name> <YYYY-MM-DD> <Gender>")
    full_name, birth_date, gender = params[0], params[1], params[2]
    employee = Employee.from_strings(full_name, birth_date, gender)
    inserted = employee.persist(repository)
    if inserted:
        print(f"Employee '{employee.full_name}' saved successfully.")
    else:
        print(
            f"Employee '{employee.full_name}' with birth date {employee.birth_date.isoformat()} already exists."
        )


def mode_list_employees(repository: EmployeeRepository) -> None:
    employees = repository.fetch_unique_sorted()
    print("Full Name | Birth Date | Gender | Age")
    for employee in employees:
        age = employee.age()
        print(f"{employee.full_name} | {employee.birth_date.isoformat()} | {employee.gender} | {age}")
    print(f"Total rows: {len(employees)}")


def mode_bulk_fill(repository: EmployeeRepository, generator: EmployeeDataGenerator) -> None:
    total_primary = 1_000_000
    total_special = 100
    print("Generating and inserting 1,000,000 rows...")
    inserted_primary = repository.bulk_insert(generator.stream(total_primary))
    print(f"Inserted {inserted_primary} primary rows.")
    print("Adding 100 special rows (Male, surname starts with 'F')...")
    inserted_special = repository.bulk_insert(generator.male_surname_f(total_special))
    print(f"Inserted {inserted_special} special rows.")


def mode_filter(repository: EmployeeRepository) -> float:
    start = time.perf_counter()
    employees = repository.fetch_male_surname_f()
    duration = time.perf_counter() - start
    print("Filtered employees (Male, surname starts with 'F'):")
    for employee in employees[:10]:
        print(f"{employee.full_name} | {employee.birth_date.isoformat()} | {employee.gender}")
    if len(employees) > 10:
        print(f"... ({len(employees) - 10} more rows)")
    else:
        print("No additional rows.")
    print(f"Total rows: {len(employees)}")
    print(f"Execution time: {duration:.6f} seconds")
    return duration


def mode_optimize(repository: EmployeeRepository) -> None:
    print("Measuring query time before optimization...")
    before = mode_filter(repository)
    print("Creating index on (gender, full_name)...")
    repository.create_gender_surname_index()
    print("Measuring query time after optimization...")
    after = mode_filter(repository)
    improvement = before - after
    explanation = (
        "The composite index on (gender, full_name) allows SQLite to quickly narrow down rows by gender "
        "and use the prefix search on surnames without scanning the entire table."
    )
    print(f"Time before: {before:.6f} seconds")
    print(f"Time after: {after:.6f} seconds")
    print(f"Improvement: {improvement:.6f} seconds")
    print("Explanation:", explanation)


def mode_index_and_compress(repository: EmployeeRepository, params: list[str]) -> None:
    repository.create_gender_surname_index()
    employees = repository.fetch_male_surname_f()
    if not employees:
        print("No employees found for compression (Male, surname starts with 'F').")
        return
    output_path = Path(params[0]) if params else Path("exports/male_f.jsonl.gz")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    total_records = 0
    total_plain_bytes = 0
    with gzip.open(output_path, mode="wt", encoding="utf-8") as compressed_file:
        for employee in employees:
            record = {
                "full_name": employee.full_name,
                "birth_date": employee.birth_date.isoformat(),
                "gender": employee.gender,
                "age": employee.age(),
            }
            line = json.dumps(record, ensure_ascii=False)
            compressed_file.write(line + "\n")
            total_plain_bytes += len((line + "\n").encode("utf-8"))
            total_records += 1
    compressed_size = output_path.stat().st_size
    compression_ratio = (
        (1 - compressed_size / total_plain_bytes) * 100 if total_plain_bytes else 0
    )
    print(f"Compressed {total_records} records to {output_path.as_posix()}.")
    print(f"Uncompressed size estimate: {total_plain_bytes} bytes")
    print(f"Compressed size: {compressed_size} bytes")
    print(f"Compression saved approximately {compression_ratio:.2f}% space.")
    print("Index usage ensured via composite index on (gender, full_name).")


def mode_compressed_table(repository: EmployeeRepository) -> None:
    repository.create_gender_surname_index()
    employees = repository.fetch_male_surname_f()
    if not employees:
        print("No employees found matching criteria. Populate data (e.g., mode 4) first.")
        return

    repository.create_compressed_table()

    compressed_rows: list[tuple[str, str, str, bytes]] = []
    plain_bytes = 0
    for employee in employees:
        payload = {
            "full_name": employee.full_name,
            "birth_date": employee.birth_date.isoformat(),
            "gender": employee.gender,
            "age": employee.age(),
        }
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        compressed = gzip.compress(raw)
        compressed_rows.append(
            (employee.full_name, employee.birth_date.isoformat(), employee.gender, compressed)
        )
        plain_bytes += len(raw)

    inserted = repository.replace_compressed_rows(compressed_rows)
    print(f"Compressed table refreshed with {inserted} rows.")

    start_main = time.perf_counter()
    repository.fetch_male_surname_f()
    main_duration = time.perf_counter() - start_main

    start_compressed = time.perf_counter()
    compressed_subset = repository.fetch_compressed_subset()
    restored = [
        json.loads(gzip.decompress(payload).decode("utf-8"))
        for _, _, _, payload in compressed_subset
    ]
    compressed_duration = time.perf_counter() - start_compressed

    _, compressed_size = repository.compressed_table_stats()
    compression_ratio = (1 - compressed_size / plain_bytes) * 100 if plain_bytes else 0

    print(f"Main table query time: {main_duration:.6f} seconds")
    print(f"Compressed table query time: {compressed_duration:.6f} seconds")
    print(f"Compression saved approximately {compression_ratio:.2f}% space.")
    print(f"Compressed storage footprint: {compressed_size} bytes")
    print(f"Restored sample record: {restored[0] if restored else 'N/A'}")
    print(
        "Speed-up comes from scanning a smaller prefiltered table with indexed prefix lookups; "
        "compression reduces I/O footprint when reading the data."
    )


def parse_arguments(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Employee directory CLI application.")
    parser.add_argument("mode", type=int, help="Mode number (1-8).")
    parser.add_argument("params", nargs="*", help="Additional parameters for the selected mode.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    ensure_utf8()
    args = parse_arguments(argv or sys.argv[1:])
    database = Database(DB_PATH.as_posix())
    repository = EmployeeRepository(database)
    generator = EmployeeDataGenerator()

    mode_handlers = {
        1: lambda: mode_create_table(repository),
        2: lambda: mode_create_employee(repository, args.params),
        3: lambda: mode_list_employees(repository),
        4: lambda: mode_bulk_fill(repository, generator),
        5: lambda: mode_filter(repository),
        6: lambda: mode_optimize(repository),
        7: lambda: mode_index_and_compress(repository, args.params),
        8: lambda: mode_compressed_table(repository),
    }

    handler = mode_handlers.get(args.mode)
    if handler is None:
        raise ValueError("Mode must be an integer between 1 and 8.")
    try:
        handler()
    finally:
        database.close()


if __name__ == "__main__":
    main()

