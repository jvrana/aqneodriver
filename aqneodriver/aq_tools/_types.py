from typing import Any
from typing import Callable
from typing import Dict
from typing import Generator
from typing import Hashable
from typing import List
from typing import Tuple

from pydent import Browser
from pydent import ModelBase


CacheFuncCallable = Callable[[Browser, List[ModelBase]], None]
GetModelsCallable = Callable[
    [Browser, ModelBase], Generator[Tuple[ModelBase, Dict[str, Any]], None, None]
]
KeyFuncCallable = Callable[[ModelBase], Tuple[Hashable, Dict[str, Any]]]
NewNodeCallback = Callable[[Hashable, Dict[str, Any]], None]
NewEdgeCallback = Callable[[Hashable, Hashable, Dict[str, Any]], None]
