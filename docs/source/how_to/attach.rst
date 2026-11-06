Attach files without relevant metadata
=========================================

``collate``/``label`` sort files using metadata read out of the files themselves (DICOM
tags, by default). Some data that belongs in a session doesn't work that way — e.g.
raw list-mode or count-rate files from a Siemens PET scanner, which have no readable
patient/session metadata of their own, or live in a completely separate export
location to the DICOMs. If those files can still be found some other way (a shared
naming convention with the primary DICOMs, in the same folder or elsewhere) and you
know which scan/resource they belong to from their path, ``attach`` can link them
into a session that's already been through ``collate``.

It can run at any point after :ref:`1. Collate files into sessions/scans/resources`
— on ``collate``'s own output, on ``label``'s, or later — since it only needs the
session's own metadata (read from the primary files' own headers), not resolved
project/subject/session IDs. The one real ordering constraint is ``deidentify``: run
``attach`` *before* it if ``GLOB`` relies on identifying metadata, as in the
example below (``{PatientName.family_name}``) — ``deidentify`` replaces
``PatientName`` with a fixed placeholder rather than removing it, so the glob would
still evaluate afterward, just silently against the wrong (anonymised) name, and
never find the real files:

.. code-block:: console

    $ accessionflow attach /data/staging/labelled /data/staging/attached \
        "medimage/vnd.siemens.syngo-mi.vr20b.count-rate|medimage/vnd.siemens.syngo-mi.vr20b.list-mode" \
        "/data/raw-exports/{PatientName.family_name}_{PatientName.given_name}*.ptd" \
        ".*/[^.]+\.[^.]+\.[^.]+\.(?P<id>\d+)\.[A-Z]+_(?P<resource>[^.]+).*"

* ``INPUT_DIR`` — the directory of sessions to attach files into, from whichever
  stage you're running this after (``ACFLOW_INPUT_DIR``)
* ``OUTPUT_DIR`` — where sessions are written with the attached files attached
  (``ACFLOW_OUTPUT_DIR``)
* ``DATATYPE`` — the format of the files being linked in, as one or more
  ``|``-separated MIME-like identifiers (``ACFLOW_DATATYPE``) — multiple formats can
  be matched by a single call, as in the example above
* ``GLOB`` — a glob pattern used to find the files, with ``{field}`` placeholders
  filled in from the session's own metadata (e.g. ``{PatientName.family_name}``)
  (``ACFLOW_GLOB``). A relative pattern is resolved against the directory the primary
  DICOMs came from; an absolute one (as above) searches a separate location entirely,
  ignoring where the DICOMs live
* ``ID_PATTERN`` — a regular expression matched against each found file's path
  (``ACFLOW_ID_PATTERN``), with named groups ``id`` (the scan ID to attach it to) and
  ``resource`` (the resource name to file it under), and optionally ``type`` (the scan
  type/description, which otherwise defaults to the scan ID)

Attaching files after cleanup (metadata skeletons)
--------------------------------------------------

Attached files sometimes turn up late — after a session's primary DICOMs have
already been through ``label``/``deidentify``/``upload`` and had their local copies
cleaned up. ``attach`` doesn't need to know or care whether that's happened: if
``label`` (and/or ``deidentify``) was run with ``--unlink-source keep-metadata``
rather than ``all``, the session and scan directories are still there — just without
the (now redundant) image data — since ``--unlink-source keep-metadata`` only removes
each resource's files, never the session's or scan's own ``__METADATA__.json``.

``attach`` loads a session the same way either way. A scan with no resources at all
is an ordinary, valid state (it's what a freshly-collated scan looks like before
anything's been added to it), so a metadata-only skeleton reloads and matches files
against it exactly as it would for a session whose data is still fully present. The
``GLOB`` template is filled in from the session's own metadata (preserved regardless),
and ``ID_PATTERN`` only ever looks at the *new* file's path — neither depends on the
primary resource data actually being there.

In short: run ``label``/``deidentify`` with ``--unlink-source keep-metadata`` instead
of ``all`` if you expect ``attach`` to need to attach files sometime after the bulk
of a session's data has already moved on, and you don't want to keep the full image
data around indefinitely just in case.
