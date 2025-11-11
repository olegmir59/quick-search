from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING, Iterable, Iterator

if TYPE_CHECKING:
    from .repository import EmployeeRepository


@dataclass(slots=True)
class Employee:
    full_name: str
    birth_date: date
    gender: str

    def __post_init__(self) -> None:
        self.full_name = self.full_name.strip()
        self.gender = self.gender.strip().capitalize()

    @classmethod
    def from_strings(cls, full_name: str, birth_date: str, gender: str) -> Employee:
        parsed = datetime.strptime(birth_date, "%Y-%m-%d").date()
        gender_norm = gender.strip().capitalize()
        if gender_norm not in {"Male", "Female"}:
            raise ValueError("Gender must be 'Male' or 'Female'")
        return cls(full_name=full_name, birth_date=parsed, gender=gender_norm)

    def to_row(self) -> tuple[str, str, str]:
        return (self.full_name, self.birth_date.isoformat(), self.gender)

    def persist(self, repository: EmployeeRepository) -> bool:
        return repository.insert_employee(self)

    def age(self, reference_date: date | None = None) -> int:
        today = reference_date or date.today()
        years = today.year - self.birth_date.year
        if (today.month, today.day) < (self.birth_date.month, self.birth_date.day):
            years -= 1
        return years


def chunked(iterable: Iterable[Employee], size: int) -> Iterator[list[Employee]]:
    batch: list[Employee] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch

