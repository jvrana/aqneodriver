from typing import Dict
from typing import NamedTuple

from aqneodriver.types import FormatData


class Payload(NamedTuple):
    """A graphdb transaction payload consisting of a query str and an optional
    data."""

    query: str
    data: Dict[str, FormatData]
