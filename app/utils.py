from __future__ import annotations

import random
import string
from datetime import date, timedelta
from typing import Iterable, Iterator, Optional

from .models import Employee

MALE_FIRST_NAMES = [
    "Alexander",
    "Benjamin",
    "Charles",
    "Daniel",
    "Edward",
    "Fig",
    "George",
    "Henry",
    "Isaac",
    "James",
    "Kevin",
    "Leonard",
    "Michael",
    "Nicholas",
    "Oliver",
    "Patrick",
    "Quentin",
    "Richard",
    "Samuel",
    "Thomas",
    "Ulysses",
    "Victor",
    "William",
    "Xavier",
    "Yannick",
    "Zachary",
]

FEMALE_FIRST_NAMES = [
    "Abigail",
    "Bianca",
    "Charlotte",
    "Diana",
    "Eleanor",
    "Fiona",
    "Gabriella",
    "Hannah",
    "Isabella",
    "Julia",
    "Katherine",
    "Lillian",
    "Madeline",
    "Natalie",
    "Olivia",
    "Penelope",
    "Queenie",
    "Rebecca",
    "Sophia",
    "Tabitha",
    "Uma",
    "Victoria",
    "Wendy",
    "Ximena",
    "Yvette",
    "Zoey",
]

MIDDLE_NAMES = [
    "Alexandrovich",
    "Borisovich",
    "Carlson",
    "Dmitrievich",
    "Evans",
    "Fedorovich",
    "Gregory",
    "Howard",
    "Ivanovich",
    "Jackson",
    "Konstantinovich",
    "Ludovic",
    "Mikhailovich",
    "Nathaniel",
    "Olivier",
    "Petrovich",
    "Quincy",
    "Robertson",
    "Sergeevich",
    "Timothy",
    "Ulyanov",
    "Vladimirovich",
    "Wilkinson",
    "Xavier",
    "Yakovlevich",
    "Zaharovich",
]

SURNAME_SUFFIXES = [
    "anderson",
    "bennett",
    "clarkson",
    "dawson",
    "ellington",
    "foster",
    "garrison",
    "hawkins",
    "iverson",
    "johnson",
    "kellington",
    "lancaster",
    "morrison",
    "norrington",
    "oakwood",
    "parkinson",
    "quinton",
    "robertson",
    "sanderson",
    "tremont",
    "upton",
    "vanderbilt",
    "wellington",
    "xander",
    "york",
    "zellington",
]


class EmployeeDataGenerator:
    """Generator that produces employee entities with controlled distribution."""

    def __init__(self, seed: Optional[int] = None) -> None:
        self._random = random.Random(seed)
        self._letters = list(string.ascii_uppercase)

    def _random_birth_date(self) -> date:
        start = date(1960, 1, 1)
        end = date(2010, 12, 31)
        delta_days = (end - start).days
        random_days = self._random.randint(0, delta_days)
        return start + timedelta(days=random_days)

    def _build_surname(self, initial: str, suffix_seed: int) -> str:
        suffix = SURNAME_SUFFIXES[suffix_seed % len(SURNAME_SUFFIXES)]
        return f"{initial.upper()}{suffix[1:]}"

    def _random_middle(self, gender: str) -> str:
        base = self._random.choice(MIDDLE_NAMES)
        return base if gender == "Male" else base + "a"

    def _first_name(self, gender: str) -> str:
        names = MALE_FIRST_NAMES if gender == "Male" else FEMALE_FIRST_NAMES
        return self._random.choice(names)

    def generate(self, gender: str, initial_letter: str, index: int) -> Employee:
        first = self._first_name(gender)
        surname = self._build_surname(initial_letter, index)
        middle = self._random_middle(gender)
        full_name = f"{surname} {first} {middle}"
        birth_date = self._random_birth_date()
        return Employee(full_name=full_name, birth_date=birth_date, gender=gender)

    def stream(self, total: int) -> Iterator[Employee]:
        genders = ["Male", "Female"]
        for idx in range(total):
            letter = self._letters[idx % len(self._letters)]
            gender = "Male" if letter == "F" else genders[idx % len(genders)]
            yield self.generate(gender, letter, idx)

    def male_surname_f(self, count: int) -> Iterable[Employee]:
        for idx in range(count):
            yield self.generate("Male", "F", idx + 1_000_000)

