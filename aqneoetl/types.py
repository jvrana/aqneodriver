from typing import Any
from typing import List
from typing import Tuple
from typing import Union

_X = Union[str, int, float]
FormatData = Union[_X, List[Union[_X, List[_X], Tuple[_X, ...]]]]
Args = Tuple[Any, ...]
ArgsList = Union[List[Args], Tuple[Args, ...]]
