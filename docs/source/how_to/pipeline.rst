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

Pulling extra metadata from file/directory paths
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Not everything you need is necessarily readable from a file's own headers — a raw
format might carry no metadata at all, or your incoming data might already be
organised into directories that encode something useful (a cohort label, a scanner
ID, ...) that never made it into the DICOM tags. ``--path-metadata-regex`` pulls extra
metadata fields out of a resource's own file/directory path instead, via named groups
in a Python regular expression — likely a common one to reach for, since it's often
the easiest way to get a field XNAT needs (e.g. a project ID) when the scanner itself
doesn't write it anywhere in the file:

.. code-block:: console

    $ xnat-ingest group /data/incoming/cohort-A /data/staging/grouped \
        --path-metadata-regex '.*/(?P<cohort>[^/]+)$' medimage/dicom-series

Here, anything found under a ``.../cohort-A/...`` directory gets a ``cohort``
metadata field set to ``cohort-A``, alongside whatever's read from its DICOM headers —
usable anywhere a metadata field is, e.g. as part of a ``--project``/``--session``
specifier in ``assign``, or in the compound ``{...}`` format strings below.

* the regex is matched (via ``re.match``, so from the start of the string, but not
  necessarily the whole thing unless you anchor it with ``$`` as above) against the
  resource's own directory — or file path, for single-file formats — not the whole
  ``INPUT_PATHS`` argument
* the second argument restricts which datatype the pattern applies to (matched via
  ``isinstance``, so a broader datatype like ``medimage/dicom-collection`` also
  matches ``medimage/dicom-series``)
* can be repeated for different datatypes/patterns
* if a resource matches the datatype but its path doesn't actually match the regex,
  that's a hard error — it aborts the whole ``group`` run rather than just skipping
  that one session, so keep the pattern narrow enough that it only applies to data
  you know follows the convention

Composing IDs from more than one field
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A field specifier (``--session``/``--scan``/``--resource`` here; ``--project``/
``--subject``/``--session``/``--scan`` in ``assign``, below) doesn't have to name a
single metadata field. It can instead be a Python format string over several fields,
to compose an ID from more than one and/or apply formatting:

.. code-block:: console

    $ xnat-ingest group ... --session '{PatientID}_{StudyDate:%Y%m%d}' all

This is detected automatically (a specifier is treated as a format string if it
contains a ``{``, and as a plain field name otherwise, so existing specifiers like
``SeriesNumber`` or ``ImageType[2:]`` keep working unchanged). A ``%``-style format
spec on a field (``{StudyDate:%Y%m%d}``) works whether that field is still a live date
value or has become a plain string (e.g. after being reloaded from a
``__METADATA__.json`` file written by an earlier stage) — a plain string is parsed as
a date first if the format spec looks like it wants one. If a field referenced in the
specifier can't be resolved at all, that part of the ID falls back to the same
placeholder mechanism described below for ``assign``.


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
bit most likely to need tweaking per node/site. Any of them can also be a format
string composing more than one field, e.g. ``--project '{PatientID}_{StudyDate:%Y}'``
— see :ref:`Composing IDs from more than one field` above.

What happens when an ID can't be resolved
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If a project/subject/session field isn't present in a session's metadata at all, that
session isn't dropped — it's assigned a unique placeholder ID instead
(``INVALID_NOTFOUND_<FIELD>_<random>``), and the whole session is saved under an
``__invalid__`` subdirectory of ``OUTPUT_DIR`` (e.g.
``assigned/__invalid__/INVALID_NOTFOUND_PROJECT_ab12cd34.Session_Label.123/``) instead
of alongside normally-assigned sessions. This is so a misconfigured field (or a
session that genuinely doesn't have the data it needs) can still be found and manually
reviewed or reprocessed, rather than only showing up as a line in the logs with
nothing to act on.

Cleaning up previous stages
~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

At the upload stage you are able to filter which resources get uploaded based on their
file types. This can be specified by the "always-include" flag, e.g. ``--always-include medimage/dicom-series``
(defaults to all file types ``--always-include all``) uploads every file found in the session
that matches that type. The alternative is defining an expected column layout for the
project with the `frametree <https://arcanaframework.github.io/frametree/>`_ tool, and
only data with a matching column in the dataset definition is uploaded.


4. Check what made it up
----------------------------

``check-upload`` compares a staging directory against what's actually present on the
XNAT server and logs anything that's missing — useful to run after ``upload`` to
confirm nothing was dropped.

.. code-block:: console

    $ xnat-ingest check-upload /data/staging/assigned xnat.example.org

It takes the same ``STAGED``/``SERVER``/credential options as ``upload``.
