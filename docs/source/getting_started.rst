Installation
------------

.. code-block:: console

    pip install aqneodriver

Commandline usage
-----------------

Getting help
************

.. code-block:: console

    aqneodriver --help

.. code-block::

    cli is powered by Hydra.

    == Configuration groups ==
    Compose your configuration from those groups (group=option)
    aquarium: default, local, production
    docker: docker-compose
    neo: default, local
    settings/help: default
    settings/out: default, disable
    task: clear_db, update_samples

    == Config ==
    Override anything in the config (foo.bar=value)
    task: ???
    aquarium:
      host: http://0.0.0.0
      port: 80
      user: youruser
      password: yourpass
      uri: ${neo.host}:${neo.port}
    neo:
      host: bolt://localhost
      port: 7687
      user: neo4j
      password: neo4j
      uri: ${neo.host}:${neo.port}

    Powered by Hydra (https://hydra.cc)
    Use --hydra-help to view Hydra specific help

You can get **task specific** help by running --help with your task

.. code-block:: console

    aqneodriver +task=update_samples --help

Update db task
**************

:class:`aqneodriver.tasks._update_samples.UpdateSampleDatabase`

.. code-block:: console

    aqneodriver +task=update_samples task.n_jobs=12 task.strict=false
