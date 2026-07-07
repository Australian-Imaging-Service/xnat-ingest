Building tools/data model
=========================

The directory structure ``xnat_ingest`` reads and writes at every pipeline stage is
deliberately plain and human-readable, rather than a database or opaque intermediate
format:

.. code-block:: text

    <project>.<subject>.<session>/
        __METADATA__.json                  # session-level metadata
        <scan_id>.<scan_type>/
            __METADATA__.json              # scan-level metadata
            <resource_name>/
                __METADATA__.json          # resource-level metadata
                __MANIFEST__.json          # checksums + datatype
                <data files>

Nothing stops you from reading these JSON files directly, but the
:doc:`model classes </api>` give you a typed, correct way to work with the same
structure — handling checksums, deidentification, and staging consistently with the
rest of the pipeline:

* :class:`~xnat_ingest.model.session.ImagingSession` — a whole session: its
  project/subject/session IDs, scans, session-level resources, and metadata. Load an
  existing one from disk with :meth:`ImagingSession.load
  <xnat_ingest.model.session.ImagingSession.load>`, or build one from scratch with
  :meth:`ImagingSession.from_paths
  <xnat_ingest.model.session.ImagingSession.from_paths>`.
* :class:`~xnat_ingest.model.scan.ImagingScan` — one scan within a session, and the
  resources attached to it.
* :class:`~xnat_ingest.model.resource.ImagingResource` — one resource: a ``FileSet``,
  its checksums, and its metadata.

A minimal example of a custom script that loads a staged session and inspects it:

.. code-block:: python

    from xnat_ingest.model.session import ImagingSession

    session = ImagingSession.load("/data/staging/assigned/MYPROJECT.subject1.1")

    print(session.project_id, session.subject_id, session.session_id)
    for scan_id, scan in session.scans.items():
        print(scan_id, scan.type, list(scan.resources))
        print(scan.metadata.get("SeriesDescription"))

The :doc:`API functions </api>` that back each CLI sub-command (``group``, ``assign``,
``deidentify``, ``upload``, ``associate``, ...) are themselves just plain Python
functions built on these model classes, so they're usable directly from a script
without going through the CLI at all if that's a better fit for what you're building.
