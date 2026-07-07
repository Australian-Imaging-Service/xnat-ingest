Associate files without relevant metadata
=========================================

``group``/``assign`` sort files using metadata read out of the files themselves (DICOM
tags, by default). Some data that belongs in a session doesn't work that way — e.g.
raw list-mode or count-rate files from a Siemens PET scanner, which have no readable
patient/session metadata of their own, or live in a completely separate export
location to the DICOMs. If those files can still be found some other way (a shared
naming convention with the primary DICOMs, in the same folder or elsewhere) and you
know which scan/resource they belong to from their path, ``associate`` can link them
into an already-assigned session.

It has to run on :ref:`assign <2. Assign project/subject/session IDs>`'s output (or
later), since it needs a session's own metadata already resolved to build the search
pattern:

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

Attaching files after cleanup (metadata skeletons)
--------------------------------------------------

Associated files sometimes turn up late — after a session's primary DICOMs have
already been through ``assign``/``deidentify``/``upload`` and had their local copies
cleaned up. ``associate`` doesn't need to know or care whether that's happened: if
``assign`` (and/or ``deidentify``) was run with ``--unlink-source keep-metadata``
rather than ``all``, the session and scan directories are still there — just without
the (now redundant) image data — since ``--unlink-source keep-metadata`` only removes
each resource's files, never the session's or scan's own ``__METADATA__.json``.

``associate`` loads a session the same way either way. A scan with no resources at all
is an ordinary, valid state (it's what a freshly-grouped scan looks like before
anything's been added to it), so a metadata-only skeleton reloads and matches files
against it exactly as it would for a session whose data is still fully present. The
``GLOB`` template is filled in from the session's own metadata (preserved regardless),
and ``ID_PATTERN`` only ever looks at the *new* file's path — neither depends on the
primary resource data actually being there.

In short: run ``assign``/``deidentify`` with ``--unlink-source keep-metadata`` instead
of ``all`` if you expect ``associate`` to need to attach files sometime after the bulk
of a session's data has already moved on, and you don't want to keep the full image
data around indefinitely just in case.
