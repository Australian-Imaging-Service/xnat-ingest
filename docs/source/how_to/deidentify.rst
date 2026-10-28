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

Every input session with resources is processed, even when its output already
exists. A late series is saved into the existing output session. Earlier scans
remain there. A format's deidentification method can change bytes, remove files,
or remove resources. The stage does not compare input and output file counts or
use a format list to decide whether a run is complete. The method must raise an
exception if it cannot complete its work.

``ImagingSession.load_for_processing`` records the input file paths before loading
the session. After processing, saving the output and writing any requested
re-identification mapping succeed, ``--unlink-source all`` removes only those
recorded files, then empty directories. ``--unlink-source keep-metadata`` removes
only recorded files inside the loaded resource directories. It keeps session and
scan metadata. A new path that arrives during the run stays for the next cycle,
including a file that arrives during cleanup. A processing or save failure keeps
the input. Without ``--unlink-source``, the input stays and is processed on each
cycle. Metadata-only skeletons from ``keep-metadata`` are skipped.

New sessions are built outside the visible session directory and published by
rename. When ``ImagingSession.save`` adds or replaces a resource in an existing
session, it also builds the resource, manifest and metadata under ``__build__``
on the destination filesystem before publishing the complete resource by rename.
This prevents a reader from seeing a partly copied file. A delay alone would not
protect against a writer that pauses mid-copy. An unchanged resource is retained
without changing its file times.

This protection is per resource. It is not a transaction for the whole session.
Replacing an existing resource has a brief interval when its directory is absent;
a reader can fail and retry or see only the other resources. A reader can also see
a mix of complete resources from different saves. The input path snapshot is not
a writer lock: writers must not replace a recorded path during processing. Files
written by external programs must also be published only after they are complete.
Do not run two deidentify workers on the same input directory.

The file format's ``contains_phi`` flag selects deidentification. DICOM uses this
path by default. Other formats can provide their own deidentification method;
formats without that flag are copied. If the method requires a recipe and there
is no applicable spec (see below), the session fails and its input stays.

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
