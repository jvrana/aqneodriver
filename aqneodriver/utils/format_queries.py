import re
from typing import Set

from aqneodriver.types import FormatData
import textwrap


def _resolve(_str, matches, repl):
    _str = str(_str)
    for match in matches:
        groupdict = match.groupdict()
        val = repl

        if groupdict["idx"] is not None:

            def resolver(x):
                return x[int(groupdict["idx"])]

        else:

            def resolver(x):
                return x

        _str = _str.replace(match.group(0), str(resolver(val)), 1)
    return _str


_format_key_pattern = re.compile(r"{\s*(?P<key>\w+)(\[\s*(?P<idx>\d+)\s*\])?\s*}")


def get_format_keys(line: str) -> Set[str]:
    keys = {}
    for match in _format_key_pattern.finditer(line):
        key = match.groupdict()["key"]
        keys.setdefault(key, [])
        keys[key].append(match)
    return keys


def format_cypher_query(query: str, **kwargs: FormatData) -> str:
    """Formats a Cypher query.

    Usage:

    .. code-block::

        query = '''
        MATCH (n:{label})
        SET n.{items[0]} = {items[1]}
        RETURN n
        '''

        format_cypher_query(query, label='Label', items=list(data.items()))
    """
    formatted_lines = list()
    for line in query.splitlines():
        keys = get_format_keys(line)
        keys1, keys2 = [], []
        for key in keys:
            val = kwargs[key]
            if isinstance(val, (list, tuple)):
                keys2.append(key)
            else:
                keys1.append(key)

        formatted_line = line
        for key in keys1:
            matches = keys[key]
            formatted_line = _resolve(formatted_line, matches, kwargs[key])

        if len(keys2) > 1:
            raise ValueError(
                "More than one iterable key detected. "
                "This is not yet supported. "
                "Please use single iterable of tuples as a "
                "key and inline reseolver if possible."
            )

        if keys2:
            for key in keys2:
                matches = keys[key]
                vals = kwargs[key]
                for val in vals:
                    formatted_lines.append(_resolve(formatted_line, matches, val))
        else:
            formatted_lines.append(formatted_line)
    qstr = "\n".join(formatted_lines)
    return textwrap.dedent(qstr)
