from typing import List
from typing import TypeVar

from pydent import AqSession
from pydent import Browser
from pydent.models import Sample

from ._struct_aq_query import StructuredAquariumQuery
from aqneodriver.utils.progress import infinite_task_context
from rich.progress import Progress

_T = TypeVar("T")
_GT = TypeVar("GT")


# TODO: set timeout
class StructuredInvQuery(StructuredAquariumQuery):
    def run(self, aq: AqSession, models: List[Sample], new_node_callback=None):
        with aq.with_cache(timeout=60) as sess:
            page_size = 1000
            with Progress() as progress:
                task0 = progress.add_task("getting items...", total=page_size*2)
                with infinite_task_context(progress, task_id=task0) as callback:
                    items = sess.Item.where({"sample_id": [m.id for m in models]}, page_size=page_size, page_callback=callback)
                    sess.browser.get(items, {'object_type'})
                    return list(sess.browser.models)

aq_inventory_to_cypher = StructuredInvQuery()
