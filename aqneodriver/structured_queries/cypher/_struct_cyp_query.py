import re
import textwrap
from copy import deepcopy
from dataclasses import dataclass
from dataclasses import fields
from typing import Dict
from typing import Set
from typing import Tuple
from typing import Type
from typing import TypeVar

from aqneodriver.exceptions import ClassDefinitionConflictError
from aqneodriver.exceptions import ImproperClassDefinitionError
from aqneodriver.exceptions import InitializationNotAllowedError
from aqneodriver.exceptions import ValidationError
from aqneodriver.payload import Payload
from aqneodriver.utils import format_cypher_query
from aqneodriver.utils import get_format_keys

_T = TypeVar("T")
_is_query_key = "__is_abstract_query__"
_variable_pattern = re.compile(r"\$(?P<variable>[a-zA-Z_]+)")


def abstract_query(c: _T) -> _T:
    setattr(c, _is_query_key, True)
    return c


def is_abstract_query(c: _T) -> bool:
    return getattr(c, _is_query_key, False)


class StructuredCypherQueryMeta(type):
    _query_key: str = "query"
    queries: Dict[str, _T] = {}

    def __new__(cls, clsname: str, superclasses: Tuple[Type, ...], attributedict) -> _T:
        newcls = super().__new__(cls, clsname, superclasses, attributedict)
        if superclasses and StructuredCypherQueryMeta not in superclasses:
            # then this might be a StructureQuery definition
            if clsname in cls.queries:
                raise ClassDefinitionConflictError(
                    "Query {name}' already defined.".format(name=clsname)
                )
            if not hasattr(newcls, cls._query_key):
                raise ValidationError("field {} must be defined".format(cls._query_key))
            newcls = dataclass(newcls)
            setattr(newcls, _is_query_key, False)
            newcls.validate()
            cls.queries[clsname] = newcls
        else:
            # then this might be a definition of a ABC or a user error
            if not is_abstract_query(newcls):
                msg = "{} is improperly defined.".format(clsname)
                msg += "\nNew abstract classes of metaclass '{}' must use the define {} key.".format(
                    cls.__name__, abstract_query.__name__
                )
                msg += "\nIf this is a new abstract query class, please set `{} = True`".format(
                    _is_query_key
                )
                msg += (
                    "\nElse if this is a query instance, inherit from "
                    "on of abstract query classes (such as StructuredQuery)"
                )
                raise ImproperClassDefinitionError(msg)
            else:

                def __init__(self, *args, **kwargs):
                    raise InitializationNotAllowedError(
                        "Cannot initialize {} class directly.".format(
                            self.__class__.__name__
                        )
                    )

                newcls.__init__ = __init__
        return newcls

    def validate(cls):
        field_list = [f.name for f in fields(cls)]
        expected_field_set = cls.expected_fields()
        missing_fields = expected_field_set.difference(set(field_list))
        errors = []
        if missing_fields:
            errors.append(
                "Query class is defintion for fields {}".format(
                    ",".join(["'{}'".format(f) for f in sorted(missing_fields)])
                )
            )
        if errors:
            msg = "\n".join(errors)
            msg += "\n"
            msg += str(cls)
            raise ValidationError(msg)

    def expected_fields(cls) -> Set[str]:
        field_set = set()
        for m in _variable_pattern.finditer(cls.query):
            field_set.add(m.groupdict()["variable"])
        return field_set

    def expected_format_keys(cls) -> Set[str]:
        keys = get_format_keys(cls.query)
        return keys

    def _comment(cls) -> str:
        return "// " + type.__str__(cls)

    def __str__(cls) -> str:
        if is_abstract_query(cls):
            s = repr(cls)
            return s
        else:
            s = cls._comment()
            s += textwrap.dedent(cls.query)
        return s


class StructuredCypherQuery(metaclass=StructuredCypherQueryMeta):

    __is_abstract_query__ = True

    def payload_data(self) -> Dict:
        data = {}
        for key in self.__class__.expected_fields():
            val = getattr(self, key)
            data[key] = val
        return deepcopy(data)

    def _kwarg_override(self, kwargs):
        field_names = [f.name for f in fields(self)]
        missing_keys = set()
        for k in self.__class__.expected_format_keys():
            if k in field_names:
                if k not in kwargs:
                    kwargs[k] = getattr(self, k)
            elif k not in kwargs:
                missing_keys.add(k)
        return kwargs, missing_keys

    # TODO: return as Payload???
    def payload(self, **kwargs) -> Payload:
        kwargs, missing_keys = self._kwarg_override(kwargs)
        if missing_keys:
            raise ValueError(
                "Could not create query payload for {}."
                "\nQuery is missing keys {} These must be supplied either in query "
                "\ninitialization or here as kwargs.".format(repr(self), missing_keys)
            )
        query_str = str(self)
        query_str = format_cypher_query(query_str, **kwargs)
        return Payload(query_str, self.payload_data())

    def __str__(self) -> str:
        return str(self.__class__)
