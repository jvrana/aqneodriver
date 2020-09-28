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
            non_browser_sess = aq(using_cache=False)
            page_size = 50

            with Progress() as progress:
                fv_task = progress.add_task("collecting field values...", total=100)
                op_task = progress.add_task("collecting operations...", total=100)
                wire_task = progress.add_task("collecting wires...", total=100)
                op_type_task = progress.add_task(
                    "collecting operation types...", total=100
                )
                ft_task = progress.add_task("collecting field types...", total=100)
                ja_task = progress.add_task("collecting job associations...", total=100)
                pa_task = progress.add_task(
                    "collecting plan associations...", total=100
                )

                def update_task(task_id, description, advance):
                    task = progress.tasks[task_id]
                    total = task.total
                    if task.percentage >= 100.0:
                        progress.update(
                            task_id=task_id,
                            completed=0,
                            description=description,
                            total=total * 2,
                        )
                    else:
                        progress.update(
                            task_id, advance=advance, description=description
                        )

                def complete_task(task_id):
                    progress.update(task_id, completed=progress.tasks[task_id].total)

                fvs = paginated_query(
                    non_browser_sess.FieldValue,
                    {
                        "parent_class": "Operation",
                        "child_sample_id": [m.id for m in models],
                    },
                    page_size=page_size,
                    callback=lambda x, n: update_task(
                        fv_task, description=x, advance=n
                    ),
                )
                complete_task(fv_task)

                ops = paginated_query(
                    non_browser_sess.Operation,
                    {"id": [m.parent_id for m in fvs]},
                    page_size=page_size,
                    callback=lambda x, n: update_task(
                        op_task, description=x, advance=n
                    ),
                )
                complete_task(op_task)

                wires = paginated_query(
                    non_browser_sess.Wire,
                    {"from_id": [m.id for m in fvs]},
                    {"id": [m.id for m in fvs]},
                    page_size=page_size,
                    callback=lambda x, n: update_task(
                        wire_task, description=x, advance=n
                    ),
                )
                complete_task(wire_task)

                op_types = paginated_query(
                    non_browser_sess.OperationType,
                    {"id": [m.operation_type_id for m in ops]},
                    page_size=page_size,
                    callback=lambda x, n: update_task(
                        op_type_task, description=x, advance=n
                    ),
                )
                complete_task(op_type_task)

                fts = paginated_query(
                    non_browser_sess.FieldType,
                    {
                        "parent_id": [m.id for m in op_types],
                        "parent_class": "OperationType",
                    },
                    page_size=page_size,
                    callback=lambda x, n: update_task(
                        ft_task, description=x, advance=n
                    ),
                )
                complete_task(ft_task)

                jas = paginated_query(
                    non_browser_sess.JobAssociation,
                    {"operation_id": [m.id for m in ops]},
                    page_size=page_size,
                    callback=lambda x, n: update_task(
                        ja_task, description=x, advance=n
                    ),
                )
                complete_task(ja_task)

                pas = paginated_query(
                    non_browser_sess.PlanAssociation,
                    {"operation_id": [m.id for m in ops]},
                    page_size=page_size,
                    callback=lambda x, n: update_task(
                        pa_task, description=x, advance=n
                    ),
                )
                complete_task(pa_task)

                browser: Browser = sess.browser
                browser.update_cache(fvs)
                browser.update_cache(ops)
                browser.update_cache(op_types)
                browser.update_cache(wires)
                browser.update_cache(pas)
                browser.update_cache(jas)
                browser.update_cache(fts)

            return list(browser.models)
