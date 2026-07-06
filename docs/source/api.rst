API
===

Public
~~~~~~

.. autofunction:: xnat_ingest.api.group



Model classes
~~~~~~~~~~~~~

Base classes form the foundation of the fileformats package and are not intended to be
instantiated directly, but rather subclassed to create new file formats. The methods
and properties of these classes are described here.

.. autoclass:: xnat_ingest.model.session.ImagingSession
    :members: uid, project_id, subject_id, session_id, scans, session_resources, run_uid, metadata
