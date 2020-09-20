from typing import Any
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Tuple
from typing import Union

_X = Union[str, int, float]
FormatData = Union[_X, List[Union[_X, List[_X], Tuple[_X, ...]]]]
Args = Tuple[Any, ...]
ArgsList = Union[List[Args], Tuple[Args, ...]]


class Payload(NamedTuple):
    """A graphdb transaction payload consisting of a query str and an optional
    piece of data."""

    query: str
    data: Dict[str, FormatData]
