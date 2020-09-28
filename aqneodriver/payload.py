import json
from typing import Dict
from typing import Sequence

from aqneodriver.exceptions import ValidationError
from aqneodriver.types import FormatData


class Payload(Sequence):
    """A graphdb transaction payload consisting of a query str and an optional
    data."""

    def __init__(self, query: str, data: Dict[str, FormatData]):
        self.query = query
        self.data = data
        self.validate()

    def validate(self):
        if not isinstance(self.query, str):
            raise ValidationError("Query must be a string")
        if not isinstance(self.data, dict):
            raise ValidationError("Data must be a dict")
        json.dumps(self.data)

    def __iter__(self):
        yield self.query
        yield self.data

    def __getitem__(self, x: int):
        if x == 0:
            return self.query
        elif x == 1:
            return self.data
        else:
            raise IndexError

    def __len__(self):
        return 2
