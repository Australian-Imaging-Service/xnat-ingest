Command-line interface
======================

AccessionFlow's command line interface consists of a number of sub-commands under the
`accessionflow` command. See :doc:`/how_to/pipeline` for how these fit together into a
pipeline.


.. click:: accessionflow.cli.collate_cli:collate_cmd
   :prog: accessionflow collate

.. click:: accessionflow.cli.collate_cli:collate_orthanc_cmd
   :prog: accessionflow collate-orthanc


.. click:: accessionflow.cli.label_cli:label_cmd
   :prog: accessionflow label


.. click:: accessionflow.cli.deidentify_cli:deidentify_cmd
   :prog: accessionflow deidentify


.. click:: accessionflow.cli.package_cli:package_cmd
   :prog: accessionflow package


.. click:: accessionflow.cli.upload_cli:upload_cmd
   :prog: accessionflow upload


.. click:: accessionflow.cli.check_upload_cli:check_upload_cmd
   :prog: accessionflow check-upload


.. click:: accessionflow.cli.attach_cli:attach_cmd
   :prog: accessionflow attach
