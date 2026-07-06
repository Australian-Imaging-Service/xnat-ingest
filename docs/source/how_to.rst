How-to guides
=============

Short, task-focused recipes for specific things you might want to do with *XNAT Ingest*.
For full option listings see :doc:`cli`.


Run the basic ingest pipeline
------------------------------

This walks through the minimum needed to get files from a directory on disk into an
XNAT project: **group** the raw files into scans/resources, **assign** each session a
project/subject/session ID, then **upload** to XNAT. Each stage is a separate CLI
sub-command and writes its output to a directory that the next stage reads from, so
they can be run as a chain, on separate schedules, or repeatedly with ``--loop``.

(De-identification, linking in files that don't carry their own sorting metadata, and
site-specific steps like linking sessions to an external records database, are
separate concerns layered on top of this — covered in their own guides.)

1. Group files into sessions/scans/resources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
:doc:`cli`).


2. Assign project/subject/session IDs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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


3. Upload to XNAT
~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~

``check-upload`` compares a staging directory against what's actually present on the
XNAT server and logs anything that's missing — useful to run after ``upload`` to
confirm nothing was dropped.

.. code-block:: console

    $ xnat-ingest check-upload /data/staging/assigned xnat.example.org

It takes the same ``STAGED``/``SERVER``/credential options as ``upload``.


Associate files that don't carry their own sorting metadata
----------------------------------------------------------------

``group``/``assign`` sort files using metadata read out of the files themselves (DICOM
tags, by default). Some data that belongs in a session doesn't work that way — e.g.
raw list-mode or count-rate files from a Siemens PET scanner, which have no readable
patient/session metadata of their own, or live in a completely separate export
location to the DICOMs. If those files can still be found some other way (a shared
naming convention with the primary DICOMs, in the same folder or elsewhere) and you
know which scan/resource they belong to from their path, ``associate`` can link them
into an already-assigned session.

It has to run on ``assign``'s output (or later), since it needs a session's own
metadata already resolved to build the search pattern:

.. code-block:: console

    $ xnat-ingest associate /data/staging/assigned /data/staging/associated \
        "medimage/vnd.siemens.syngo-mi.vr20b.count-rate|medimage/vnd.siemens.syngo-mi.vr20b.list-mode" \
        "/data/raw-exports/{PatientName.family_name}_{PatientName.given_name}*.ptd" \
        ".*/[^.]+\.[^.]+\.[^.]+\.(?P<id>\d+)\.[A-Z]+_(?P<resource>[^.]+).*"

* ``INPUT_DIR`` — the ``assign`` output directory (``XINGEST_INPUT_DIR``)
* ``OUTPUT_DIR`` — where sessions are written with the associated files attached
  (``XINGEST_OUTPUT_DIR``)
* ``DATATYPE`` — the format of the files being linked in, as one or more
  ``|``-separated MIME-like identifiers (``XINGEST_DATATYPE``) — multiple formats can
  be matched by a single call, as in the example above
* ``GLOB`` — a glob pattern used to find the files, with ``{field}`` placeholders
  filled in from the session's own metadata (e.g. ``{PatientName.family_name}``)
  (``XINGEST_GLOB``). A relative pattern is resolved against the directory the primary
  DICOMs came from; an absolute one (as above) searches a separate location entirely,
  ignoring where the DICOMs live
* ``ID_PATTERN`` — a regular expression matched against each found file's path
  (``XINGEST_ID_PATTERN``), with named groups ``id`` (the scan ID to attach it to) and
  ``resource`` (the resource name to file it under), and optionally ``type`` (the scan
  type/description, which otherwise defaults to the scan ID)


Deidentify images before upload
----------------------------------

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

Only formats known to carry patient information are touched — DICOM is treated this
way by default, while derived formats like NIfTI are assumed already
de-identifiable-in-place and are just copied through unchanged. If a session contains
a PHI-bearing format with no applicable spec (see below), that session is skipped and
logged as an error rather than uploaded with PHI still attached.

Laying out the deidentification specs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each session's original identifying values (before they were stripped) are written to
``REID_DIR`` as ``<session_id>.json``. If ``--reid-encrypt-key`` is set to a
URL-safe base64-encoded 32-byte key (e.g. from ``Fernet.generate_key()`` in the
``cryptography`` package), the file is written encrypted instead, as
``<session_id>.json.enc``, and can only be read back with that same key — keep it
somewhere separate from ``REID_DIR`` itself.


Deploying as a long-running service
--------------------------------------

Rather than a one-off run, most nodes want ``group``/``assign``/``deidentify``/
``upload`` running continuously, each watching its input directory and picking up new
files as they arrive. Every stage except ``check-upload`` supports this directly via
``--loop <seconds>``: instead of running once and exiting, the command re-runs itself
every ``<seconds>`` seconds, forever, in a single process.

Errors encountered while processing an individual session (a bad file, a missing
spec, a dropped connection) are logged and skipped rather than crashing the process —
in fact ``--raise-errors`` and ``--loop`` can't be combined, since raising would
defeat the point of looping forever. This means each stage is designed to be started
once and left running, rather than needing a process supervisor to restart it after
every transient failure (though a restart policy is still good practice as a
backstop — see below).

A published Docker image (``ghcr.io/australian-imaging-service/xnat-ingest``) wraps
the CLI as its entrypoint, so each stage becomes a single long-running container:

.. code-block:: console

    $ docker run -d \
        -v /data/incoming:/input \
        -v /data/staging:/staging \
        ghcr.io/australian-imaging-service/xnat-ingest \
        group /input /staging/grouped --loop 300

Repeat with ``assign``, optionally ``deidentify``, and ``upload`` as separate
containers, each mounting the previous stage's output as its input, chained through a
shared staging area on disk (or an S3 bucket, which ``upload``/``check-upload`` can
read from directly).

Docker Compose
~~~~~~~~~~~~~~~~

A ``docker-compose.yml`` can express the whole chain as one stack, with each stage as
a service sharing a named volume for the staging directories:

.. code-block:: yaml

    services:
      group:
        image: ghcr.io/australian-imaging-service/xnat-ingest
        command: group /input /staging/grouped --loop 300
        volumes:
          - /data/incoming:/input
          - staging:/staging
        restart: unless-stopped

      assign:
        image: ghcr.io/australian-imaging-service/xnat-ingest
        command: assign /staging/grouped /staging/assigned --loop 300
        volumes:
          - staging:/staging
        restart: unless-stopped

      upload:
        image: ghcr.io/australian-imaging-service/xnat-ingest
        command: upload /staging/assigned xnat.example.org --always-include all --loop 300
        environment:
          XINGEST_USER: my-upload-user
          XINGEST_PASS: my-upload-password
        volumes:
          - staging:/staging
        restart: unless-stopped

    volumes:
      staging:

Kubernetes
~~~~~~~~~~~~

The same shape maps onto a Kubernetes ``Deployment`` per stage (one replica each,
``restartPolicy: Always``), with the staging directories on a shared
``PersistentVolumeClaim`` mounted into each Pod, and connection details (XNAT host,
user, password) injected via a ``ConfigMap``/``Secret`` as ``XINGEST_*`` environment
variables rather than passed as command-line flags. ``check-upload`` doesn't support
``--loop``, so it's a natural fit for a ``CronJob`` instead, run periodically to audit
what has and hasn't made it to XNAT rather than as an always-on service.
