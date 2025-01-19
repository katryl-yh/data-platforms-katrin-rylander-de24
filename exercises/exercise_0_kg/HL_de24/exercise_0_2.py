import random
from typing import Any

famous_european_orchards: list[str | None] = [
    "Brogdale Farm Collections",
    "Apfelgut Schloss Lindach",
    "Thatchers Orchard",
    "Domaine de Pomone",
    "Äppelriket Österlen",
    "De Fruithof",
    "Obstbau Knaller",
    "Apfelparadies Warnow",
    "Le Verger Conservatoire de Pézilla",
    "Pomoselekt Research Station",
]

# generate None values
for i in random.sample(range(len(famous_european_orchards)), random.randint(0, 3)):
    famous_european_orchards[i] = None

def qc(data: list[Any], default_type: type) -> None:
    if len(data) != 10:
            raise ValueError("List length must be exactly 10 items")
    if any(x is None for x in data):
        raise ValueError(f"List cannot contain None values - expected type: {default_type}")

try:
    qc(famous_european_orchards, str)
except Exception as e:
    print(e)
