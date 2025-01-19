from typing import Any

from pydantic import BaseModel, Field, ValidationError, field_validator

USERS = [
    {"id": 101, "name": "Erika", "is_active": True, "age": 45},
    {"id": 102, "name": "Marcus", "is_active": True, "age": 34},
    {"id": 103, "name": "David", "is_active": False, "age": 29},
    {"id": 104, "name": "Anna", "is_active": True, "age": 41.5},
    {"id": 106, "name": "Ingrid", "is_active": "NOPE", "age": 8},
]


class User(BaseModel):
    id: int = Field(ge=0)
    name: str = Field(min_length=1, max_length=50)
    is_active: bool
    age: int = Field(ge=0, le=150)

    @field_validator("is_active", mode="before")
    @classmethod
    def ensure_bool(cls, value: Any) -> Any:
        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            if value.lower() in ("true"):
                return True
            elif value.lower() in ("false"):
                return False

        raise ValueError(f"Invalid bool: {value}")


def validate_schema(users_data: list[dict]) -> dict:
    rows_invalid = {}

    for i, user in enumerate(users_data):
        try:
            User(**user)
        except ValidationError as exc:
            rows_invalid[i] = {
                exc.errors()[0].get("loc")[0]: exc.errors()[0].get("input")
            }

    if rows_invalid:
        return rows_invalid
    return {}
