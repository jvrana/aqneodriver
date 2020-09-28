from os.path import abspath
from os.path import dirname
from os.path import join
from typing import List

from ._cypher_file_to_payloads import parse_cypher_file

here = dirname(abspath(__file__))


def get_relationships_queries() -> List[str]:
    return parse_cypher_file(join(here, "relationships.cypher"))
