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
steps, no templating beyond ``${NAME}`` placeholders resolved against declared
parameters. A spec is a flat list of xnat-ingest stages chained by data dependencies;
everything else under a stage's ``args:`` is exactly the keyword arguments of the
matching :doc:`../api` function, just spelled in YAML.

Install the extra this needs — it's optional, so plain CLI usage never requires
Prefect:

.. code-block:: console

    $ python3 -m pip install xnat-ingest[workflow]

A minimal spec — see ``example-specs/`` in the repository for more, including the
full non-DICOM case with every composite argument:

.. code-block:: yaml

    name: minimal-dicom

    params:
      input_dir:
        description: "Root directory of the scanner export to ingest"
      xnat_server:
        description: "URL of the XNAT server to upload to"
      xnat_user:
        description: "XNAT username"
      xnat_password:
        description: "XNAT password"
        secret: true

    stages:
      - name: group
        command: group
        args:
          input_paths: ["${input_dir}"]

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
        args:
          server: "${xnat_server}"
          user: "${xnat_user}"
          password: "${xnat_password}"

.. code-block:: console

    $ xnat-ingest workflow check minimal-dicom.yaml \
        -p input_dir=/data/scanner-export \
        -p xnat_server=https://xnat.example.org \
        -p xnat_user=scanner-uploader \
        -p xnat_password="$XNAT_PASSWORD"
    $ xnat-ingest workflow run   minimal-dicom.yaml   # run once, synchronously
    $ xnat-ingest workflow serve minimal-dicom.yaml   # long-running local deployment

``check`` never requires Prefect (or the parameters to resolve to anything real —
just to resolve at all); ``run``/``serve``/``deploy`` do.

Declaring what a workflow needs: ``params:``
---------------------------------------------

The top-level ``params:`` block names every ``${NAME}`` placeholder a spec expects
supplied per-deployment — the same idea as a GitHub Actions ``workflow_dispatch``
``inputs:`` block or an Argo ``parameters:`` list, kept deliberately simple: every
param is a plain string. Each entry takes:

``description``
    Shown by ``xnat-ingest workflow check``.

``default``
    If given, the param is optional and falls back to this when nothing else
    supplies it. Omit it to make the param **required**.

``secret``
    Marks the param as one that must never be handed to Prefect's own parameter
    store (see "Two kinds of parameter" below) — it also masks the value (and a
    literal ``default:``, if given) in ``check`` output. Use an environment
    variable, not a literal ``default:``, for anything actually sensitive.

A ``${NAME}`` reference resolves in this order:

1. ``--param``/``-p name=value`` on the ``check``/``run``/``serve``/``deploy``
   command line — always wins, useful for local testing or a k8s ``args:`` entry
   that reads a Secret via ``$(SECRET_ENV_VAR)`` substitution.
2. A same-named **environment variable** — so a k8s Secret mounted the ordinary way
   (``env:``/``envFrom:``) just works, no CLI flag needed.
3. The param's declared ``default:``.
4. Otherwise: for a plain, non-secret param this isn't an error yet (see below) —
   for anything else, ``check``/``run``/``serve``/``deploy`` fail immediately with
   an error naming the missing parameter and its description.

A ``${NAME}`` used somewhere in the spec but never declared under ``params:`` still
resolves via ``--param``/the environment (step 1/2 above) — declaring it just adds a
default and a clearer error if it's missing.

This is also how a spec stays transportable between sites: ``input_dir`` (and
anything else site-specific — a metadata table CSV path, say) is a param like any
other, referenced as ``"${input_dir}"`` wherever a literal path would otherwise
appear, so the same spec file runs unmodified at every site with different
``--param``/environment values.

Two kinds of parameter: baked-in vs. a real Prefect parameter
-----------------------------------------------------------------

Not every ``${NAME}`` resolves the same way. Used as the *whole value* of a plain
argument (one that isn't parsed into a structured type — see the composite-argument
section below) for a **non-secret** param, it isn't resolved when the spec loads at
all — it becomes a genuine Prefect flow parameter instead, with the resolved value
above (or the declared default) as its default:

.. code-block:: console

    $ xnat-ingest workflow check minimal-dicom.yaml -p ... | grep prefect-param
        - input_dir (required) [prefect-param] - Root directory of the scanner export to ingest
        - xnat_server (required) [prefect-param] - URL of the XNAT server to upload to

That means it shows up as a normal, named/typed parameter in Prefect's own UI/API/
orchestration database — visible in run history, and editable on the deployment
without redeploying, the same as any other Prefect flow's parameters. Everything
else — a ``secret: true`` param (regardless of where it's used), a placeholder
inside a structured/coerced argument (a datatype, an ``IDSpec``, ...), one embedded
in a larger string rather than filling the whole value, or an undeclared name — is
resolved once, here, from ``--param``/the environment/the declared default, and
baked into the flow's closure. Most importantly, this is what keeps a secret out of
Prefect's parameter store entirely: it is never data Prefect sees, stores, or
displays.

This is also why a required non-secret param (``xnat_server (required)`` above) is
fine to leave unresolved through ``check`` and ``deploy`` — Prefect enforces
"required" itself, when the deployment is actually triggered, the same way it would
for any of its own flow parameters. ``run`` is the exception: since a one-shot run
executes immediately with no later trigger to ask for anything, it still requires a
concrete value for every parameter up front.

No implicit backend config
---------------------------

There is no top-level ``xnat:`` block that gets silently wired into every ``upload``
stage. Credentials are plain ``args:`` on the ``upload`` stage(s) that need them,
resolved through the same ``params:`` mechanism as everything else:

.. code-block:: yaml

    - name: upload
      command: upload
      input: deidentify
      args:
        server: "${xnat_server}"
        user: "${xnat_user}"
        password: "${xnat_password}"

This keeps the spec format backend-agnostic — a future non-XNAT upload stage just
declares its own ``args:`` shape, with no change to the spec schema — and means a
workflow with two ``upload`` stages targeting different servers just gives each its
own ``server``/``user``/``password``, explicitly, rather than assuming "the one
configured backend".

Sharing config between specs, where it's actually worth it, is ``extends:`` (see
below) — but reach for it deliberately: the shipped ``example-specs/`` deliberately
keep the (small) duplication of their ``xnat_server``/``xnat_user``/
``xnat_password`` declarations rather than factoring out a shared file, so each spec
stays fully self-contained and readable on its own.

Where output gets staged: ``--work-dir``
-------------------------------------------

There is likewise no ``work_dir:``/``${work_dir}`` in the spec's own YAML — where
each stage's output lands on *this* host is a deployment/runtime concern, not part
of what the pipeline does, so the same spec runs on a different host with a
different ``--work-dir`` completely unmodified. ``run``/``serve``/``deploy`` all
accept it:

.. code-block:: console

    $ xnat-ingest workflow run minimal-dicom.yaml --work-dir /var/lib/xnat-ingest/work

Every stage without its own ``args.output_dir`` gets a subdirectory under
``<work-dir>/<workflow-name>/<stage-name>`` — namespaced by workflow name so several
specs (e.g. everything ``deploy``ed from one directory) can share one ``--work-dir``
root without their outputs colliding. Left unset, it defaults to
``.xnat-ingest-<name>`` next to the spec file.

Spec format
-----------

``name``
    Used as the Prefect flow/deployment name. Defaults to the spec file's stem.

``params``
    See above.

``schedule``
    A cron expression, used by ``workflow serve``/``workflow deploy`` (ignored by
    ``run``/``check``). Schedule the whole workflow this way rather than looping
    individual stages — ``--loop`` (see :ref:`Deployment tips`) is a plain-CLI
    mechanism and isn't used here.

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
    one's own top-level keys. Use sparingly (see above) — genuinely shared
    ``params:`` declarations are the main legitimate case.

Expressing composite arguments
-------------------------------

Anything that was a repeatable ``<field> <datatype>``-style CLI flag becomes a list
of small mappings; a single-field flag can stay a bare string or a list:

.. code-block:: yaml

    args:
      session:
        - expr: "{subject_uid}_{CaptureDate:%Y%m%d}"
      scan:
        - expr: "dermoscopy-{LesionID}"
          datatype: "image/png|image/jpeg"
      on_resource_clash:
        - policy: merge
          datatype: "image/png|image/jpeg"
      path_metadata_regex:
        - pattern: '.*/(?P<subject_uid>[\w-]+)/(?P<filename>[\w-]+\.(?:png|jpe?g))'
          datatype: "image/png|image/jpeg"
      metadata_tables:
        - path: /data/lesion-table.csv
          rows: "fileset[image/png|image/jpeg]"
          joins:
            ImagePath: '=HYPERLINK("{subject_uid}/{filename}")'

Each entry is built the same way the CLI's own composite flags are (a mapping as
keywords, a list positionally, a bare scalar for a single-field type) — see
``xnat_ingest.workflow.coerce`` for the full mapping from YAML shape to argument
type, one function per composite kind. ``xnat-ingest workflow check`` dry-runs this
conversion (and validates every ``args:`` key against the target function's actual
parameters) without touching the filesystem or network, so a typo'd datatype or an
unknown argument is caught before anything runs.

Deploying to a Prefect server
------------------------------

``workflow run``/``workflow serve`` execute locally, in the calling process.
``workflow deploy`` instead registers a spec (or every spec in a directory) as a
proper Prefect deployment against a remote server:

.. code-block:: console

    $ xnat-ingest workflow deploy example-specs/ \
        --prefect-api-url https://prefect.example.org/api \
        --work-pool xnat-ingest \
        --work-dir /var/lib/xnat-ingest/work \
        -p xnat_server=https://xnat.example.org \
        -p xnat_user=scanner-uploader \
        -p xnat_password="$XNAT_PASSWORD"

This is registration only — it does **not** start anything. A Prefect worker
(``prefect worker start --pool xnat-ingest``), running somewhere with network
access to the relevant input/work/XNAT paths, must be polling that same work pool
for a deployment to ever actually execute. Create the work pool ahead of time if it
doesn't already exist (e.g. ``prefect work-pool create xnat-ingest --type process``
for a worker that runs flows as local subprocesses).

``--prefect-api-url``/``--prefect-api-key`` (also settable via the standard
``PREFECT_API_URL``/``PREFECT_API_KEY`` environment variables) point ``deploy`` at
the target server explicitly, rather than relying on whatever Prefect profile
happens to be active. They're accepted by both the CLI and the
``xnat_ingest.api.workflow_api.deploy`` function directly.

Secrets (``xnat_password`` above) are resolved once, at deploy time, from
``--param``/the environment, and baked into that deployment's flow closure — never
becoming a Prefect flow parameter, so a secret never reaches Prefect's own
orchestration database/UI at all (see "Two kinds of parameter" above). The
consequence is that rotating a secret means redeploying with a new
``--param``/environment value, rather than updating a value the server holds.

Every other (non-secret, plain-argument) param — ``xnat_server``/``xnat_user``
above — becomes a real Prefect deployment parameter instead: the value resolved at
deploy time (or the declared default) becomes its default, editable/re-triggerable
later from Prefect's own UI/API with no redeploy needed, same as any other Prefect
flow's parameters. One left with no resolvable value at deploy time is simply a
required parameter with no default — Prefect asks for it when the deployment is
actually triggered.

``deploy`` applies the same ``param_overrides``/``--work-dir``/``--work-pool`` to
every spec matched by ``--pattern`` (default ``*.yaml``) under ``specs_dir``. A
batch that genuinely needs different values per spec (different sites'
credentials, say) should be deployed with separate ``deploy`` calls rather than one
covering the whole directory. A spec that fails to load or deploy is logged and
skipped by default so the rest of the batch still goes through; pass
``--raise-errors`` to stop at the first failure instead.
