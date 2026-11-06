
<img src="./docs/source/_static/images/logo-text.png" alt="Logo Small" style="float: right; width: 100mm">

[![CI/CD](https://github.com/Australian-Imaging-Service/accessionflow/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/Australian-Imaging-Service/accessionflow/actions/workflows/ci-cd.yml)
[![codecov](https://codecov.io/gh/Australian-Imaging-Service/accessionflow/graph/badge.svg?token=V860ZYIKQ3)](https://codecov.io/gh/Australian-Imaging-Service/accessionflow)
[![PyPI](https://img.shields.io/pypi/v/accessionflow.svg)](https://pypi.python.org/pypi/accessionflow/)
[![Documentation Status](https://img.shields.io/badge/docs-latest-brightgreen.svg?style=flat)](https://australian-imaging-service.github.io/accessionflow/)


Accessioning is the process of formally receiving material from a source and registering
it in a managed collection. AccessionFlow is a
collection of tools to accession data: collate it, label it, de-identify it and upload it
to an XNAT repository. Support for various file types is provided through
the [FileFormats](https://arcanaframework.github.io/fileformats/) package and its extensions
(e.g. [FileFormats MedImage](https://arcanaframework.github.io/fileformats-medimage/), [FileFormats Siemens](https://arcanaframework.github.io/fileformats-vendor-siemens/),...).

```mermaid
flowchart LR
    Scanner(["Scanner / instrument"]) --> Collate["collate"]
    Collate --> Label["label"]
    Label --> Deidentify["deidentify (optional)"]
    Deidentify --> Upload["upload"]
    Upload --> XNAT[("XNAT")]
    Upload -.-> CheckUpload["check-upload"]

    Extra(["files without\nsorting metadata"]) -.-> Attach["attach (optional)"]
    Collate -.-> Attach
    Attach -.-> Label
```


## Installation

A Docker image containing the toolkit can be pulled from the GitHub Container Registry
and entrypoint of the image is the accessionflow CLI so commands can be run by the form

```
docker run ghcr.io/australian-imaging-service/accessionflow:latest <command-name> <arg1> <arg2> ... --opt1 <opt1> ...
```

Alternatively, AccessionFlow can be installed as a Python package from PyPI with `pip`:

```
$ python3 -m pip install accessionflow
```


## Running

AccessionFlow has a public API and a command-line interface (CLI), with sub-commands to collate, label,
attach, de-identify, and upload imaging sessions to XNAT — either as a one-off run or as a
continuously-running service (e.g. via Docker Compose or Kubernetes).

See the [full documentation](https://australian-imaging-service.github.io/accessionflow/) for a hands-on
quick start, how-to guides for each part of the pipeline, and the complete CLI/API reference.
