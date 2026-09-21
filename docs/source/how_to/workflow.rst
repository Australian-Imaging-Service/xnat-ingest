Running a whole pipeline from one spec
=======================================

:ref:`Deployment tips` chains ``group``/``assign``/``deidentify``/``upload`` as
separate long-running processes (containers, Compose services, ...), each wired to
the next by mounting one stage's output directory as the next stage's input. That
works well, but every composite option (``--session``, ``--scan``,
``--on-resource-clash``, ``--path-metadata-regex``, ...) has to be either repeated as
CLI tokens or packed into a ``;``-separated env var, and the chain itself has to be
maintained by hand across however many container definitions.

``xnat-ingest workflow`` runs the whole chain from a single YAML spec instead, via
`Prefect <https://www.prefect.io/>`_: one process, one file, and every composite
option expressed as real YAML structure rather than a packed string. It is
deliberately **not** a general orchestration language — no expressions, no shell
steps, no templating beyond ``${ENV_VAR}`` interpolation for secrets. A spec is a flat
list of xnat-ingest stages chained by data dependencies; everything else under a
stage's ``args:`` is exactly the keyword arguments of the matching
:doc:`../api` function, just spelled in YAML.

Install the extra this needs — it's optional, so plain CLI usage never requires
Prefect:

.. code-block:: console

    $ python3 -m pip install xnat-ingest[workflow]

A minimal spec — see ``example-specs/`` in the repository for more, including the
full non-DICOM case with every composite argument:

.. code-block:: yaml

    name: minimal-dicom
    work_dir: /var/lib/xnat-ingest/work/minimal-dicom

    xnat:
      server: https://xnat.example.org
      user: ${XNAT_USER}
      password: ${XNAT_PASSWORD}

    stages:
      - name: group
        command: group
        args:
          input_paths: [/data/scanner-export]

      - name: assign
        command: assign
        input: group
        args:
          project: StudyComments
          subject: PatientID
          session: AccessionNumber

      - name: upload
        command: upload
        input: assign

.. code-block:: console

    $ xnat-ingest workflow check minimal-dicom.yaml   # validate, no Prefect needed
    $ xnat-ingest workflow run   minimal-dicom.yaml    # run once, synchronously
    $ xnat-ingest workflow serve minimal-dicom.yaml    # long-running deployment

Spec format
-----------

``name``
    Used as the Prefect flow/deployment name. Defaults to the spec file's stem.

``work_dir``
    Where each stage's output lands, one subdirectory per stage
    (``<work_dir>/<stage-name>``), unless a stage sets its own ``args.output_dir``.
    Only the first stage's real input and the final ``upload`` target need spelling
    out anywhere in the spec. Defaults to ``.xnat-ingest-<name>`` next to the spec
    file.

``xnat``
    ``server``/``user``/``password``/``verify_ssl``, required by any ``upload``
    stage. Interpolate secrets rather than inlining them — see below.

``schedule``
    A cron expression, used by ``workflow serve`` (ignored by ``run``/``check``).
    Schedule the whole workflow this way rather than looping individual stages —
    ``--loop`` (see :ref:`Deployment tips`) is a plain-CLI mechanism and isn't used
    here.

``stages``
    A list of:

    ``name``
        Unique within the spec; referenced by other stages' ``input``/``after``.

    ``command``
        One of ``group``, ``assign``, ``deidentify``, ``associate``, ``upload``.

    ``input``
        The name of another stage whose *output directory* becomes this stage's
        input — the data-dependency edge; implies ordering too. Omit it only on a
        stage whose ``args`` supply its input directly (typically the first
        ``group`` in the pipeline).

    ``after``
        Pure ordering with no data flow — a list of stage names this one must
        follow without consuming their output.

    ``args``
        The command's keyword arguments as YAML — see below.

    ``enabled``
        Defaults ``true``. A ``false`` stage is skipped entirely and forwards its
        own input straight through unchanged, so a dependent's ``input:`` still
        resolves — e.g. disabling ``deidentify`` between ``assign`` and ``upload``
        means ``upload`` reads ``assign``'s output directly, with no edit needed at
        either neighbour.

    ``retries`` / ``retry_delay_seconds``
        Per-stage Prefect task retry settings (default ``0`` / ``10``) — for a
        flaky XNAT connection or a transiently locked export, without failing the
        whole run.

``extends``
    Path (relative to this file) to another spec to deep-merge underneath this
    one's own top-level keys, so several workflows can share one ``xnat:`` block
    (or ``work_dir``, etc.) without duplicating it while staying independently
    deployable. See ``example-specs/common.yaml``.

Expressing composite arguments
-------------------------------

Anything that was a repeatable ``<field> <datatype>``-style CLI flag becomes a list
of small mappings; a single-field flag can stay a bare string or a list:

.. code-block:: yaml

    args:
      session:
        - specifier: "{subject_uid}_{CaptureDate:%Y%m%d}"
      scan:
        - specifier: "dermoscopy-{LesionID}"
          datatype: "image/png|image/jpeg"
      on_resource_clash:
        - policy: merge
          scope: "image/png|image/jpeg"
      path_metadata_regex:
        - regex: '.*/(?P<subject_uid>[\w-]+)/(?P<filename>[\w-]+\.(?:png|jpe?g))'
          datatype: "image/png|image/jpeg"
      metadata_tables:
        - path: /data/lesion-table.csv
          row_frequency: "fileset[image/png|image/jpeg]"
          joins:
            ImagePath: '=HYPERLINK("{subject_uid}/{filename}")'

Each entry is built the same way the CLI's own composite flags are (a mapping as
keywords, a list positionally, a bare scalar for a single-field type) — see
``xnat_ingest.workflow.coerce`` for the full mapping from YAML shape to argument
type, one function per composite kind. ``xnat-ingest workflow check`` dry-runs this
conversion (and validates every ``args:`` key against the target function's actual
parameters) without touching the filesystem or network, so a typo'd datatype or an
unknown argument is caught before anything runs.

Secrets
-------

``${ENV_VAR}`` is the only templating a spec supports, resolved once at load time
after any ``extends`` merge — so a shared ``xnat:`` block in ``common.yaml`` can
reference ``${XNAT_PASSWORD}`` once and every workflow that extends it just needs
that variable set in its own environment. Never inline a password — an undefined
``${VAR}`` reference fails validation immediately rather than being passed through
literally.
