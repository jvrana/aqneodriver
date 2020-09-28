from typing import List
from typing import TypeVar

from pydent import AqSession
from pydent import Browser
from pydent.models import Sample

from ._struct_aq_query import StructuredAquariumQuery

_T = TypeVar("T")
_GT = TypeVar("GT")


# TODO: set timeout
class StructuredInvQuery(StructuredAquariumQuery):
    def run(self, aq: AqSession, models: List[Sample], new_node_callback=None):
        with aq.with_cache(timeout=60) as sess:
            non_browser_sess = aq(using_cache=False)
            pages = non_browser_sess.Item.pagination(
                {"sample_id": [m.id for m in models]}, page_size=100
            )
            items = []
            for page in pages:
                new_node_callback(None, None)
                items += page

            browser: Browser = sess.browser
            browser.update_cache(items)
            browser.get(items, {"object_type"})

            return list(browser.models)


aq_inventory_to_cypher = StructuredInvQuery()
