Deidentification
================

For sites where the ``assign``-ed sessions still contain identifiable data (e.g. a
clinical scanner staging DICOMs before upload), insert a ``deidentify`` step between
``assign`` and ``upload``. It strips patient-identifying fields from each session and
writes the deidentified copy, and — only if ``--reid-dir`` is given — a
re-identification mapping so the process can be reversed later if needed (e.g. to look
up a scan for a clinical follow-up).

.. code-block:: console

    $ xnat-ingest deidentify /data/staging/assigned /data/staging/deidentified \
        /etc/xnat-ingest/deid-specs --reid-dir /data/staging/reid

* ``INPUT_DIR`` — the ``assign`` output directory (``XINGEST_INPUT_DIR``)
* ``OUTPUT_DIR`` — where deidentified sessions are written; point ``upload`` at this
  directory instead of the ``assign`` output (``XINGEST_OUTPUT_DIR``)
* ``SPEC_DIR`` — the deidentification specs, one directory per project (see below)
  (``XINGEST_SPEC_DIR``)
* ``--reid-dir`` — optional; where the re-identification mappings are written, one
  JSON file per session (``XINGEST_REID_DIR``). Omit it to discard the mapping rather
  than persist it, which is the safer choice when holding an
  original\ ↔\ deidentified lookup on disk is itself a concern.

As with ``assign``, ``--unlink-source all``/``--unlink-source keep-metadata`` clean up
the ``assign`` output once a session's been deidentified — see
:ref:`2. Assign project/subject/session IDs` for what the two modes do.

A session that is deidentified more than once (a series that arrived late is staged
as a session of its own, under the same name) is saved into the deidentified session
directory that already exists. The stage judges each run by the files of the input
in front of it: before a run, an output counts as current only if it holds a file
for every data file of the input, at the same path inside the session, and verifies
against its manifests; after a run, only the files that run produced are counted
against the input. Older files in the output never stand in for missing ones. The
input is removed only when it still holds exactly the files the run read; a file
that arrives while the session is being deidentified is kept, with the rest of the
input, for the next cycle. The check and the removal are two steps: a file that
lands between them is removed with the input. Closing that window needs the
writer (``assign``) and this stage to agree on who owns the session, which no
stage enforces today. A DICOM file is matched by name: deidentification changes
its bytes but keeps the SOP Instance UID the name derives from, which DICOM defines as
the identity of an instance, so a DICOM file sent again under the same name with
other content is not told apart from the one already deidentified. Every other
format is copied through unchanged and is matched by content as well. A change of
recipe does not refresh an output that already exists: remove that output, or stage
the input again, to deidentify it with the new recipe.

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
    read, the path to an empty JSON object is a reasonable placeholder for each spec
    file.


Re-identifying data later
------------------------------

If ``--reid-dir`` is given, each session's original identifying values (before they
were stripped) are written there as ``<session_id>.json``. If ``--reid-encrypt-key``
is also set to a URL-safe base64-encoded 32-byte key (e.g. from
``Fernet.generate_key()`` in the ``cryptography`` package), the file is written
encrypted instead, as ``<session_id>.json.enc``, and can only be read back with that
same key — keep it somewhere separate from the ``--reid-dir`` directory itself.
Without ``--reid-dir`` the mapping is never written and the deidentification cannot be
reversed (``--reid-encrypt-key`` then has no effect and is warned about).
