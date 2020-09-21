Getting Started
===============

Installation
------------

.. code-block:: bash

    pip install aqneodriver

Commandline usage
-----------------

Getting help
************

.. code-block::

    aqneodriver --help

::

    cli is powered by Hydra.

    == Configuration groups ==
    Compose your configuration from those groups (group=option)
    aquarium: default, local, production
    docker: docker-compose
    neo: default, local
    settings/help: default
    settings/out: default, disable
    task: clear_db, update_db

    == Config ==
    Override anything in the config (foo.bar=value)
    task: ???
    aquarium:
      host: http://0.0.0.0
      port: 80
      user: vrana
      password: Mountain5
      uri: ${neo.host}:${neo.port}
    neo:
      host: bolt://localhost
      port: 7687
      user: neo4j
      password: redpill
      uri: ${neo.host}:${neo.port}

    Powered by Hydra (https://hydra.cc)
    Use --hydra-help to view Hydra specific help



Running tasks
*************

.. code-block:: bash

    aqneodriver +task=update_db task.n_jobs=12 task.strict=false

