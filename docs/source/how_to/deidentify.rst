Deidentification
================

For sites where the ``assign``-ed sessions still contain identifiable data (e.g. a
clinical scanner staging DICOMs before upload), insert a ``deidentify`` step between
``assign`` and ``upload``. It strips patient-identifying fields from each session and
writes both the deidentified copy and a re-identification mapping (so the process can
be reversed later if needed, e.g. to look up a scan for a clinical follow-up).

.. code-block:: console

    $ xnat-ingest deidentify /data/staging/assigned /data/staging/deidentified \
        /etc/xnat-ingest/deid-specs /data/staging/reid

* ``INPUT_DIR`` — the ``assign`` output directory (``XINGEST_INPUT_DIR``)
* ``OUTPUT_DIR`` — where deidentified sessions are written; point ``upload`` at this
  directory instead of the ``assign`` output (``XINGEST_OUTPUT_DIR``)
* ``SPEC_DIR`` — the deidentification specs, one directory per project (see below)
  (``XINGEST_SPEC_DIR``)
* ``REID_DIR`` — where the re-identification mappings are written, one JSON file per
  session (``XINGEST_REID_DIR``)

As with ``assign``, ``--unlink-source all``/``--unlink-source keep-metadata`` clean up
the ``assign`` output once a session's been deidentified — see
:ref:`2. Assign project/subject/session IDs` for what the two modes do.

Only formats known to carry patient information are touched — DICOM is treated this
way by default, while derived formats like NIfTI are assumed already
de-identifiable-in-place and are just copied through unchanged. If a session contains
a PHI-bearing format with no applicable spec (see below), that session is skipped and
logged as an error rather than uploaded with PHI still attached.

Laying out the deidentification specs
------------------------------------------

``SPEC_DIR`` holds one subdirectory per project, named to match the project ID that
``assign`` gave the session, plus an optional ``__default__`` subdirectory used as a
fallback for any project without its own:

.. code-block:: text

    deid-specs/
        __default__/
            medimage@dicom-series.json
        MYPROJECT/
            medimage@dicom-series.json

Within each project directory, there's one JSON spec file per format, named after the
format's MIME-like identifier with ``/`` replaced by ``@`` (e.g.
``medimage/dicom-series`` becomes ``medimage@dicom-series.json``). A spec also covers
more specific sub-formats — e.g. a spec for the broader ``medimage/dicom-collection``
format applies to ``medimage/dicom-series`` sessions too if there's no more specific
match.

.. note::

    The intent is for each spec file's contents to configure *how* that format is
    deidentified for that project — e.g. which additional DICOM tags to blank or
    remap beyond the built-in set, or project-specific replacement values. That
    per-project customisation isn't wired up yet: today, the mere *presence* of a
    spec file is what matters (it tells ``deidentify`` a format is handled for that
    project), while the actual fields removed from DICOM are currently a fixed,
    built-in list regardless of what the spec file contains. Until the content is
    read, an empty JSON object (``{}``) is a reasonable placeholder for each spec
    file.


Re-identifying data later
------------------------------

Each session's original identifying values (before they were stripped) are written to
``REID_DIR`` as ``<session_id>.json``. If ``--reid-encrypt-key`` is set to a
URL-safe base64-encoded 32-byte key (e.g. from ``Fernet.generate_key()`` in the
``cryptography`` package), the file is written encrypted instead, as
``<session_id>.json.enc``, and can only be read back with that same key — keep it
somewhere separate from ``REID_DIR`` itself.
