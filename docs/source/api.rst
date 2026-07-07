API
===

Public
~~~~~~

The following functions mirror the command line tools, and can be used to
evoke the same modular behaviour via a Python workflow.


.. autofunction:: xnat_ingest.api.group

.. autofunction:: xnat_ingest.api.group_orthanc

.. autofunction:: xnat_ingest.api.assign

.. autofunction:: xnat_ingest.api.deidentify

.. autofunction:: xnat_ingest.api.upload

.. autofunction:: xnat_ingest.api.check_upload

.. autofunction:: xnat_ingest.api.associate



Model classes
~~~~~~~~~~~~~

These classes represent an imaging session as it moves through the pipeline stages
above: a session contains scans, each of which contains one or more resources (e.g.
the original DICOM files, a derived NIfTI conversion).

.. autoclass:: xnat_ingest.model.session.ImagingSession
    :members: uid, project_id, subject_id, session_id, scans, session_resources, run_uid,
        metadata, from_paths, assign, deidentify, associate_files, save, load

.. autoclass:: xnat_ingest.model.scan.ImagingScan
    :members: id, type, resources, associated, metadata, save, load

.. autoclass:: xnat_ingest.model.resource.ImagingResource
    :members: name, fileset, checksums, scan, metadata, save, load
