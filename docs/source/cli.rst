Command-line interface
======================

AccessionFlow's command line interface consists of a number of sub-commands under the
`accessionflow` command. See :doc:`/how_to/pipeline` for how these fit together into a
pipeline.

The ``label``, ``store`` and ``check-store`` stages have a subcommand for each
data store, provided by the store's plugin, e.g. ``accessionflow label xnat`` or
``accessionflow label tree``.


.. click:: accessionflow.cli.collate_cli:collate_cmd
   :prog: accessionflow collate

.. click:: accessionflow.cli.collate_cli:collate_orthanc_cmd
   :prog: accessionflow collate-orthanc


.. click:: accessionflow.stores.xnat.cli.label_cli:label_cmd
   :prog: accessionflow label xnat

.. click:: accessionflow.stores.tree.cli.label_cli:label_cmd
   :prog: accessionflow label tree


.. click:: accessionflow.cli.deidentify_cli:deidentify_cmd
   :prog: accessionflow deidentify


.. click:: accessionflow.cli.package_cli:package_cmd
   :prog: accessionflow package


.. click:: accessionflow.stores.xnat.cli.store_cli:store_cmd
   :prog: accessionflow store xnat


.. click:: accessionflow.stores.xnat.cli.check_store_cli:check_store_cmd
   :prog: accessionflow check-store xnat


.. click:: accessionflow.stores.tree.cli.store_cli:store_cmd
   :prog: accessionflow store tree


.. click:: accessionflow.stores.tree.cli.check_store_cli:check_store_cmd
   :prog: accessionflow check-store tree


.. click:: accessionflow.cli.attach_cli:attach_cmd
   :prog: accessionflow attach
