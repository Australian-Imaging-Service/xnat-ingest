Deidentification
================

For sites where the ``label``-ed sessions still contain identifiable data (e.g. a
clinical scanner staging DICOMs before upload), insert a ``deidentify`` step between
``label`` and ``store``. It strips patient-identifying fields from each session and
writes the deidentified copy, and — only if ``--reid-dir`` is given — a
re-identification mapping so the process can be reversed later if needed (e.g. to look
up a scan for a clinical follow-up).

.. code-block:: console

    $ accessionflow deidentify /data/staging/labelled /data/staging/deidentified \
        /etc/accessionflow/deid-recipes --reid-dir /data/staging/reid

* ``INPUT_DIR`` — the ``label`` output directory (``ACFLOW_INPUT_DIR``)
* ``OUTPUT_DIR`` — where deidentified sessions are written; point ``store`` at this
  directory instead of the ``label`` output (``ACFLOW_OUTPUT_DIR``)
* ``RECIPE_DIR`` — the deidentification recipes, one directory per project (see below)
  (``ACFLOW_RECIPE_DIR``, or ``ACFLOW_SPEC_DIR`` as it was previously named)
* ``--reid-dir`` — optional; where the re-identification mappings are written, one
  JSON file per session (``ACFLOW_REID_DIR``). Omit it to discard the mapping rather
  than persist it, which is the safer choice when holding an
  original\ ↔\ deidentified lookup on disk is itself a concern.

As with ``label``, ``--unlink-source all``/``--unlink-source keep-metadata`` clean up
the ``label`` output once a session's been deidentified — see
:ref:`2. Label project/subject/session IDs` for what the two modes do.

Every input session with resources is processed, even when its output already
exists. A late series is saved into the existing output session. Earlier scans
remain there. A format's deidentification method can change bytes, remove files,
or remove resources. The stage does not compare input and output file counts or
use a format list to decide whether a run is complete. The method must raise an
exception if it cannot complete its work.

``Accession.load_for_processing`` records the input file paths before loading
the session. After processing, saving the output and writing any requested
re-identification mapping succeed, ``--unlink-source all`` removes only those
recorded files, then empty directories. ``--unlink-source keep-metadata`` removes
only recorded files inside the loaded resource directories. It keeps session and
scan metadata. A new path that arrives during the run stays for the next cycle,
including a file that arrives during cleanup. A processing or save failure keeps
the input. Without ``--unlink-source``, the input stays and is processed on each
cycle. Metadata-only skeletons from ``keep-metadata`` are skipped.

New sessions are built outside the visible session directory and published by
rename. When ``Accession.save`` adds or replaces a resource in an existing
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
is no applicable recipe (see below), the session fails and its input stays.

Laying out the deidentification recipes
------------------------------------------

``RECIPE_DIR`` holds one subdirectory per project, named to match the project ID that
``label`` gave the session, plus an optional ``__default__`` subdirectory used as a
fallback for any project without its own:

.. code-block:: text

    deid-recipes/
        __default__/
            medimage/
                dicom-series.json
                dicom-series.transforms.yaml
        MYPROJECT/
            medimage/
                dicom-series.json

Within each project directory, there's one recipe file per format, at the path given by
the format's MIME-like identifier (e.g. ``medimage/dicom-series`` is
``medimage/dicom-series.json``), alongside any side-car files the recipe format uses
(e.g. ``dicom-series.transforms.yaml``). A recipe also covers
more specific sub-formats — e.g. a recipe for the broader ``medimage/dicom-collection``
format applies to ``medimage/dicom-series`` sessions too if there's no more specific
match.

.. note::

    A recipe's contents decide how the format is deidentified for that project: for
    DICOM, a `deid <https://pydicom.github.io/deid/>`_ recipe saying which tags to
    remove, blank or replace, with the values of its ``var:`` references declared in
    the ``<format>.transforms.yaml`` side-car. accessionflow ships an example in
    ``accessionflow/assets/deid_recipes/__default__``, which can be copied as a
    starting point. Check any recipe with your data's governance requirements before
    relying on it.


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
