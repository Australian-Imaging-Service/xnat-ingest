API
===

Public
~~~~~~

The following functions mirror the command line tools, and can be used to
evoke the same modular behaviour via a Python workflow.


.. autofunction:: accessionflow.api.collate

.. autofunction:: accessionflow.api.collate_orthanc

.. autofunction:: accessionflow.api.label

.. autofunction:: accessionflow.api.deidentify

.. autofunction:: accessionflow.api.package

.. autofunction:: accessionflow.stores.xnat.api.store

.. autofunction:: accessionflow.stores.xnat.api.check_store

.. autofunction:: accessionflow.stores.tree.api.store

.. autofunction:: accessionflow.stores.tree.api.check_store

.. autofunction:: accessionflow.api.attach



Model classes
~~~~~~~~~~~~~

These classes represent an imaging session as it moves through the pipeline stages
above: a session contains scans, each of which contains one or more resources (e.g.
the original DICOM files, a derived NIfTI conversion).

.. autoclass:: accessionflow.model.accession.Accession
    :members: uid, project_id, subject_id, session_id, scans, session_resources, run_uid,
        metadata, from_paths, label, deidentify, attach_files, save, load

.. autoclass:: accessionflow.model.acquisition.Acquisition
    :members: id, type, resources, attached, metadata, save, load

.. autoclass:: accessionflow.model.resource.Resource
    :members: name, fileset, checksums, scan, metadata, save, load

.. autofunction:: accessionflow.model.accession.recipe_formats_for
