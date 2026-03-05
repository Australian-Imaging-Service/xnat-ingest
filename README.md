
# Xnat-ingest

[![CI/CD](https://github.com/Australian-Imaging-Service/xnat-ingest/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/Australian-Imaging-Service/xnat-ingest/actions/workflows/ci-cd.yml)
[![codecov](https://codecov.io/gh/Australian-Imaging-Service/xnat-ingest/graph/badge.svg?token=V860ZYIKQ3)](https://codecov.io/gh/Australian-Imaging-Service/xnat-ingest)

XNAT-Ingest is a toolkit used for sorting data into project/subject/sessions, de-identifying images before
uploading them to an XNAT instance. Support for various file formats is provided through
the [FileFormats](https://arcanaframework.github.io/fileformats/) package and its extensions
(e.g. [FileFormats MedImage](https://arcanaframework.github.io/fileformats-medimage/)).


## Installation

XNAT ingest can be installed as a Python package from PyPI with `pip`:

```
$ python3 -m pip install xnat-ingest
```

Alternatively, a Docker image containing the toolkit can be pulled from `docker pull ghcr.io/australian-imaging-service/xnat-ingest:latest`

## Running

XNAT Ingest has a public API and a command-line interface (CLI). The CLI can be explored by its in-built
help menu, e.g.

```
$ xnat-ingest --help
```

When using docker, the root CLI command is set to be the entrypoint of the Docker image so it can be run
by

```
docker run xnat-ingest --help
```
