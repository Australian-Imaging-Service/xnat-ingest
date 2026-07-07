Command-line interface
======================

XNAT Ingest's command line interface consists of a number of sub-commands under the
`xnat-ingest` command. See :doc:`/how_to/pipeline` for how these fit together into a
pipeline.


.. click:: xnat_ingest.cli.group:group_cli
   :prog: xnat-ingest group

.. click:: xnat_ingest.cli.group:group_orthanc_cli
   :prog: xnat-ingest group-orthanc


.. click:: xnat_ingest.cli.assign:assign_cli
   :prog: xnat-ingest assign


.. click:: xnat_ingest.cli.deidentify:deidentify_cli
   :prog: xnat-ingest deidentify


.. click:: xnat_ingest.cli.upload:upload_cli
   :prog: xnat-ingest upload


.. click:: xnat_ingest.cli.check_upload:check_upload_cli
   :prog: xnat-ingest check-upload


.. click:: xnat_ingest.cli.associate:associate_cli
   :prog: xnat-ingest associate
