# `xnat-ingest workflow` example specs

Run any of these with:

```console
$ xnat-ingest workflow check <spec>.yaml   # validate, print the resolved stage order
$ xnat-ingest workflow run   <spec>.yaml   # run once (needs `pip install xnat-ingest[workflow]`)
$ xnat-ingest workflow serve <spec>.yaml   # long-running Prefect deployment, honours 'schedule:'
```

None of these will run as-is - the paths, server URL and datatype-specific bits are
placeholders. Copy the closest one and adjust.

| Spec | Shows |
| --- | --- |
| [`minimal-dicom.yaml`](minimal-dicom.yaml) | The smallest useful pipeline: `group` → `assign` → `upload`, every default left in place (plain DICOM). Start here. |
| [`dicom-deidentify.yaml`](dicom-deidentify.yaml) | Adds a `deidentify` stage between `assign` and `upload`, with `--reid-dir` deliberately omitted (mapping discarded, not written to disk). |
| [`acemid-lesion.yaml`](acemid-lesion.yaml) | The full non-DICOM case: composite `session`/`scan`/`resource`/`on_resource_clash`/`path_metadata_regex`/`metadata_tables` args as real YAML structure, a disabled `deidentify` stage forwarding straight through to `upload`, and `extends:` pulling shared XNAT credentials from `common.yaml`. |
| [`acemid-wholebody-internal.yaml`](acemid-wholebody-internal.yaml) | A second, independent workflow sharing `common.yaml`'s `xnat:` block via `extends:` - the "several workflows on one host" case. |
| [`associate-sidecars.yaml`](associate-sidecars.yaml) | `group` → `associate` → `assign` → `upload`, for files that arrive without their own sorting metadata (e.g. a loose sidecar dropped next to a session after the fact). |
| [`scheduled-scanner-drop.yaml`](scheduled-scanner-drop.yaml) | `schedule:` + `workflow serve` for a continuously-running clinical-scanner drop folder, with per-stage `retries:`. |

`common.yaml` isn't a runnable spec itself (it has no `stages:`) - it's `extends:`-ed
by `acemid-lesion.yaml` and `acemid-wholebody-internal.yaml` so both workflows share
one `xnat:` block without duplicating it, while staying two independent specs (two
Prefect deployments, each redeployable on its own).
