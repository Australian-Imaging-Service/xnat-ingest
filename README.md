
# XNAT Ingest

[![CI/CD](https://github.com/Australian-Imaging-Service/xnat-ingest/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/Australian-Imaging-Service/xnat-ingest/actions/workflows/ci-cd.yml)
[![codecov](https://codecov.io/gh/Australian-Imaging-Service/xnat-ingest/graph/badge.svg?token=V860ZYIKQ3)](https://codecov.io/gh/Australian-Imaging-Service/xnat-ingest)
[![PyPI](https://img.shields.io/pypi/v/xnat-ingest.svg)](https://pypi.python.org/pypi/xnat-ingest/)
[![Documentation Status](https://img.shields.io/badge/docs-latest-brightgreen.svg?style=flat)](https://australian-imaging-service.github.io/xnat-ingest/)

XNAT-Ingest is a toolkit used for sorting data into project/subject/sessions, de-identifying images before
uploading them to an XNAT instance. Support for various file formats is provided through
the [FileFormats](https://arcanaframework.github.io/fileformats/) package and its extensions
(e.g. [FileFormats MedImage](https://arcanaframework.github.io/fileformats-medimage/), [FileFormats Siemens](https://arcanaframework.github.io/fileformats-vendor-siemens/),...).

```mermaid
flowchart LR
    Scanner(["Scanner / instrument"]) --> Group["group"]
    Group --> Assign["assign"]
    Assign --> Deidentify["deidentify (optional)"]
    Deidentify --> Upload["upload"]
    Upload --> XNAT[("XNAT")]
    Upload -.-> CheckUpload["check-upload"]

    Extra(["files without\nsorting metadata"]) -.-> Associate["associate (optional)"]
    Assign -.-> Associate
    Associate -.-> Deidentify
```


## Installation

XNAT ingest can be installed as a Python package from PyPI with `pip`:

```
$ python3 -m pip install xnat-ingest
```

Alternatively, a Docker image containing the toolkit can be pulled from `docker pull ghcr.io/australian-imaging-service/xnat-ingest:latest`

## Running

XNAT Ingest has a public API and a command-line interface (CLI), with sub-commands to group, assign,
associate, de-identify, and upload imaging sessions to XNAT — either as a one-off run or as a
continuously-running service (e.g. via Docker Compose or Kubernetes).

See the [full documentation](https://australian-imaging-service.github.io/xnat-ingest/) for a hands-on
quick start, how-to guides for each part of the pipeline, and the complete CLI/API reference.
