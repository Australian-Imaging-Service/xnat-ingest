Basic ingest workflow
=====================

This walks through the minimum needed to get files from a directory on disk into an
XNAT project: **group** the raw files into scans/resources, **assign** each session a
project/subject/session ID, then **upload** to XNAT. Each stage is a separate CLI
sub-command and writes its output to a directory that the next stage reads from, so
they can be run as a chain, on separate schedules, or repeatedly with ``--loop``.

(:doc:`De-identification <deidentify>`, :doc:`linking in files that don't carry their
own sorting metadata <associate>`, and site-specific steps like linking sessions to an
external records database, are separate concerns layered on top of this — covered in
their own guides.)

1. Group files into sessions/scans/resources
-----------------------------------------------

``group`` scans one or more input paths, reads each file's metadata (DICOM tags by
default), and collates files into a directory structure of
``<session>/<scan>/<resource>`` under ``OUTPUT_DIR``, ready for the next stage.

.. code-block:: console

    $ xnat-ingest group /data/incoming/*.dcm /data/staging/grouped

* ``INPUT_PATHS`` — one or more directories or glob patterns pointing at the raw files
  (``XINGEST_INPUT_PATHS``)
* ``OUTPUT_DIR`` — where grouped sessions are written (``XINGEST_OUTPUT_DIR``)

By default, sessions are grouped by DICOM ``StudyInstanceUID``, scans by
``SeriesNumber`` and resources by ``ImageType``. These are configurable via
``--session``/``--scan``/``--resource`` if your data needs different fields (see
:doc:`/cli`).

Add ``--unlink-source all`` to remove each source file once it's been staged
(``all``/``keep-metadata`` behave the same here — see below).


2. Assign project/subject/session IDs
-----------------------------------------

``assign`` reads the grouped sessions and works out which XNAT project/subject/session
each one belongs to, from metadata fields, writing the result under
``<project>.<subject>.<session>`` directories in ``OUTPUT_DIR``.

.. code-block:: console

    $ xnat-ingest assign /data/staging/grouped /data/staging/assigned

* ``INPUT_DIR`` — the ``group`` output directory (``XINGEST_INPUT_DIR``)
* ``OUTPUT_DIR`` — where assigned sessions are written (``XINGEST_OUTPUT_DIR``)

The metadata fields used default to ``StudyComments`` (project), ``PatientID``
(subject) and ``AccessionNumber`` (session). Override with ``--project``/
``--subject``/``--session`` if your scanner populates different fields — this is the
bit most likely to need tweaking per node/site.

Add ``--unlink-source`` to clean up the ``group`` output once a session's been
assigned. Unlike ``group``, this directory is one that xnat-ingest itself created, so
there are two meaningfully different modes:

* ``--unlink-source all`` — removes the whole grouped session directory
* ``--unlink-source keep-metadata`` — removes just the resource data (the actual
  image files), but leaves the session's and each scan's own metadata file
  (``__METADATA__.json``) behind, so a lightweight, data-free skeleton of the session
  survives on disk. See :doc:`associate` for why you'd want that.

Leave it unset (the default) to keep the grouped directory untouched.


3. Upload to XNAT
--------------------

``upload`` takes the assigned sessions and pushes them to an XNAT instance, creating
projects/subjects/sessions/scans/resources as needed.

.. code-block:: console

    $ xnat-ingest upload /data/staging/assigned xnat.example.org \
        --user my-upload-user --password my-upload-password

* ``STAGED`` — the ``assign`` output directory, or an S3 bucket URI
  (``XINGEST_STAGED``)
* ``SERVER`` — the XNAT server address (``XINGEST_HOST``)
* ``--user``/``--password`` — XNAT credentials (``XINGEST_USER``/``XINGEST_PASS``)

Credentials are read from the environment if the flags are omitted, so on a shared
node you'd typically set ``XINGEST_HOST``, ``XINGEST_USER`` and ``XINGEST_PASS`` once
in the environment rather than passing them on every invocation.


4. Check what made it up
----------------------------

``check-upload`` compares a staging directory against what's actually present on the
XNAT server and logs anything that's missing — useful to run after ``upload`` to
confirm nothing was dropped.

.. code-block:: console

    $ xnat-ingest check-upload /data/staging/assigned xnat.example.org

It takes the same ``STAGED``/``SERVER``/credential options as ``upload``.
