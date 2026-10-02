# `xnat-ingest workflow` example specs

Run any of these with:

```console
$ xnat-ingest workflow check <spec>.yaml            # validate, print declared params + resolved stage order
$ xnat-ingest workflow run   <spec>.yaml             # run once (needs `pip install xnat-ingest[workflow]`)
$ xnat-ingest workflow serve <spec>.yaml             # long-running local Prefect deployment, honours 'schedule:'
$ xnat-ingest workflow deploy example-specs/         # register every spec in a directory as a Prefect deployment
```

Every spec here declares a top-level `params:` block naming what it needs
per-deployment (`input_dir`, XNAT credentials, ...). A param with no `default:` is
required - supply it with a same-named environment variable (how a k8s Secret
mounted via `env:`/`envFrom:` reaches it) or `-p name=value` on the CLI, which
always wins if both are given:

```console
$ xnat-ingest workflow check minimal-dicom.yaml \
    -p input_dir=/data/scanner-export \
    -p xnat_server=https://xnat.example.org \
    -p xnat_user=scanner-uploader \
    -p xnat_password="$XNAT_PASSWORD"
```

Where each stage's output actually gets staged is a separate concern from any of
that - see `--work-dir` below.

None of these will run for real as-is beyond that - the datatype-specific bits in
`acemid-lesion.yaml`/`acemid-wholebody-internal.yaml` are Canfield/ACEMID-specific.
Copy the closest one and adjust.

| Spec | Shows |
| --- | --- |
| [`minimal-dicom.yaml`](minimal-dicom.yaml) | The smallest useful pipeline: `group` → `assign` → `upload`, every default left in place (plain DICOM). Start here. |
| [`dicom-deidentify.yaml`](dicom-deidentify.yaml) | Adds a `deidentify` stage between `assign` and `upload`, with `--reid-dir` deliberately omitted (mapping discarded, not written to disk). |
| [`acemid-lesion.yaml`](acemid-lesion.yaml) | The full non-DICOM case: composite `session`/`scan`/`resource`/`on_resource_clash`/`path_metadata_regex`/`metadata_tables` args as real YAML structure, and a disabled `deidentify` stage forwarding straight through to `upload`. |
| [`acemid-wholebody-internal.yaml`](acemid-wholebody-internal.yaml) | A second, independent workflow, deployed alongside `acemid-lesion.yaml` from the same directory - the "several workflows on one host" case. |
| [`associate-sidecars.yaml`](associate-sidecars.yaml) | `group` → `associate` → `assign` → `upload`, for files that arrive without their own sorting metadata (e.g. a loose sidecar dropped next to a session after the fact). |
| [`scheduled-scanner-drop.yaml`](scheduled-scanner-drop.yaml) | `schedule:` for a continuously-running clinical-scanner drop folder, with per-stage `retries:` - runnable via `serve` (local) or `deploy` (registered on a Prefect server, needs a worker polling its work pool). |

Each spec is fully self-contained (no `common.yaml`/`extends:` between them) - the
small duplication of the `xnat_server`/`xnat_user`/`xnat_password` param
declarations across `acemid-lesion.yaml`/`acemid-wholebody-internal.yaml` is
deliberate, so either file can be read, understood and redeployed entirely on its
own. (`extends:` is still supported by the spec format for cases where a genuinely
shared block earns the indirection - see `workflow.spec` - it's just not used to
tie these examples together.)

Note there's no top-level `xnat:` block anywhere either - `upload`'s `server`/
`user`/`password` are plain `args:` on the `upload` stage itself, like any other
argument. That keeps the spec format backend-agnostic (a future non-XNAT upload
stage just declares its own `args:` shape) and means credentials are only ever
referenced explicitly, at the stage that actually uses them.

## `--work-dir`

Where each stage's output is staged is a runtime concern, not part of the spec's
own YAML - there's no `work_dir:`/`${work_dir}` anywhere above. `--work-dir` on
`run`/`serve`/`deploy` sets the scratch root, namespaced per workflow
(`<work-dir>/<spec-name>/<stage-name>`) so several specs can share one root - e.g.
everything deployed from this directory - without their outputs colliding. Left
unset, it defaults to `.xnat-ingest-<name>` next to the spec file.

## Deploying to a Prefect server

```console
$ xnat-ingest workflow deploy example-specs/ \
    --prefect-api-url https://prefect.example.org/api \
    --work-pool xnat-ingest \
    --work-dir /var/lib/xnat-ingest/work \
    -p xnat_server=https://xnat.example.org \
    -p xnat_user=scanner-uploader \
    -p xnat_password="$XNAT_PASSWORD"
```

This registers every `*.yaml` spec in the directory as a Prefect deployment
against the `xnat-ingest` work pool - it does **not** start a worker. A Prefect
worker (`prefect worker start --pool xnat-ingest`) must be running against that
same work pool, somewhere with network access to the input/work/XNAT paths, for a
deployment to actually execute anything.

Every `secret: true` param (`xnat_password`, `orthanc_password`) is resolved once
at deploy time and baked into that deployment's flow closure - never passed
through Prefect's own parameter/orchestration layer, so it never ends up stored
in, or visible via, the Prefect API/UI. Rotating one means redeploying with a new
`-p`/environment value.

Every other (non-secret, plain-argument) param - `input_dir`, `xnat_server`,
`orthanc_url`, ... - becomes a genuine Prefect deployment parameter instead: the
value resolved at deploy time becomes its default, but it's then a normal,
named/typed Prefect parameter - editable and re-triggerable later from Prefect's
own UI/API with no redeploy needed, and visible in run history like any other
flow's parameters. Run `check` with `-p` values supplied to see which params ended
up which way - each is marked `[secret]` or `[prefect-param]`.

The same `-p`/`--param` overrides apply to every spec matched in one `deploy`
call, so a batch that genuinely needs different values per spec (different sites'
credentials, say) should be deployed with separate `deploy` invocations.
