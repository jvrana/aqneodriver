from contextlib import contextmanager
from itertools import chain
from typing import List
from typing import Optional

from pydent import AqSession
from pydent import Browser
from pydent import ModelBase
from pydent.interfaces import QueryInterface
from pydent.models import Sample
from rich.progress import Progress

from ._struct_aq_query import StructuredAquariumQuery
from ._types import NewNodeCallback
from aqneodriver.utils.progress import infinite_task_context

def paginated_query(interface: QueryInterface, *queries, page_size=50, callback=None):
    pages_list = [interface.pagination(query, page_size=page_size) for query in queries]
    models = []
    for page in chain(*pages_list):
        if callback:
            callback(str(interface.model), len(page))
        models += page
    return models



class StructuredJobQuery(StructuredAquariumQuery):
    def run(
        self,
        aq: AqSession,
        models: List[Sample],
        *,
        new_node_callback: Optional[NewNodeCallback] = None
    ) -> List[ModelBase]:
        with aq.with_cache(timeout=60) as sess:
            sess: AqSession
            # non_browser_sess = aq(using_cache=False)
            page_size = 1000
            browser: Browser = sess.browser
            with Progress() as progress:
                task0 = progress.add_task("collecting samples", total=page_size*2)
                with infinite_task_context(progress, task0) as callback:
                    query = {
                        '__model__': 'FieldValue',
                        '__query__': {
                            'parent_class': 'Operation',
                            'child_sample_id': [m.id for m in models],
                            '__return__': {
                                'operation': {
                                    'plan_associations': {},
                                    'job_associations': {},
                                    'operation_type': 'field_types',
                                    'field_values': {
                                        'sample', 'item'
                                    }
                                },
                                'field_type': {}
                            }
                        }
                    }
                    results = sess.query(query, use_cache=True, page_size=page_size, page_callback=callback)
                    browser.update_cache(results)
            return list(browser.models)
