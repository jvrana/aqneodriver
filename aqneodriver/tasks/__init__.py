"""
Task
====

Basic tasks. To execute tasks outside of the cli (as in a jupyter notebook),
import the tasks, instantiate it, and then execute it on a config instance:

.. code-block::

    from aqneo.tasks import UpdateSampleDatabase

    task = UpdateSampleDatabase(n_jobs=12, strict=True)
    results = task(config)

To execute the same code in the command line:

.. code-block:: bash

    python app.py +task=update_samples task.n_jobs=12 task.strict=true

.. currentmodule: aqneodriver.tasks

.. autosummary::

    Task
    UpdateSampleDatabase
    ClearDatabase
"""
from ._clear_db import ClearDatabase
from ._task import Task
from ._update_inventory import UpdateInventory
from ._update_samples import UpdateSampleDatabase

__all__ = [ClearDatabase, Task, UpdateSampleDatabase]
