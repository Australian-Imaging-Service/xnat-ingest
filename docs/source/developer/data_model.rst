Data model: building your own tools
===================================

The directory structure ``accessionflow`` reads and writes at every pipeline stage is
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

* :class:`~accessionflow.model.accession.Accession` — a whole session: its
  project/subject/session IDs, scans, session-level resources, and metadata. Load an
  existing one from disk with :meth:`Accession.load
  <accessionflow.model.accession.Accession.load>`, or build one from scratch with
  :meth:`Accession.from_paths
  <accessionflow.model.accession.Accession.from_paths>`.
* :class:`~accessionflow.model.acquisition.Acquisition` — one scan within a session, and the
  resources attached to it.
* :class:`~accessionflow.model.resource.Resource` — one resource: a ``FileSet``,
  its checksums, and its metadata.

Terms
-----

The model is named so that it isn't specific to imaging or to one store:

Accession
    Everything received from one subject on one occasion and handed from stage to
    stage, e.g. an imaging session or a sequencing run. Its ID labels (e.g. project,
    subject and session for XNAT) are set by the ``label`` stage. An accession is not
    a RIS accession number: DICOM's ``AccessionNumber`` is just one metadata field that
    can be used to label it.
Acquisition
    One distinct dataset within an accession, e.g. a scan or a sequencing library. For
    DICOM it is one series, not DICOM's narrower sense of an acquisition within a
    series.
Resource
    A set of files holding one form of an acquisition's data, e.g. its DICOM or NIfTI
    version, or of the accession itself for session-level resources.

The directory layout and the ``project``/``subject``/``session`` attributes still use
XNAT's terms. These classes were previously named ``ImagingSession``, ``ImagingScan``
and ``ImagingResource``, in the ``xnat_ingest.model.session``, ``.scan`` and
``.resource`` modules.

A minimal example of a custom script that loads a staged session and inspects it:

.. code-block:: python

    from accessionflow.model.accession import Accession

    session = Accession.load("/data/staging/labelled/MYPROJECT.subject1.1")

    print(session.project_id, session.subject_id, session.session_id)
    for scan_id, scan in session.scans.items():
        print(scan_id, scan.type, list(scan.resources))
        print(scan.metadata.get("SeriesDescription"))

The :doc:`API functions </api>` that back each CLI sub-command (``collate``, ``label``,
``deidentify``, ``upload``, ``attach``, ...) are themselves just plain Python
functions built on these model classes, so they're usable directly from a script
without going through the CLI at all if that's a better fit for what you're building.
