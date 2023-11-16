
# Xnat-ingest

[![tests](https://github.com/Australian-Imaging-Service/xnat-ingest/actions/workflows/tests.yml/badge.svg)](https://github.com/Australian-Imaging-Service/xnat-ingest/actions/workflows/tests.yml)
[![coverage](https://codecov.io/gh/arcanaframework/xnat-ingest/branch/main/graph/badge.svg?token=UIS0OGPST7)](https://codecov.io/gh/Australian-Imaging-Service/xnat-ingest)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/xnat-ingest.svg)](https://pypi.python.org/pypi/xnat-ingest/)
[![PyPI](https://img.shields.io/pypi/v/xnat-ingest.svg)](https://pypi.python.org/pypi/xnat-ingest/)

Upload exported DICOM and list-mode data from Siemens Quadra "Total Body" PET scanner to
XNAT


## Installation

Build the docker image from the root directory of a clone of this code repository

```
docker build -t xnat-ingest .
```


## Running

The root CLI command is set to be the entrypoint of the Docker image so it can be run
by

```
docker run xnat-ingest --help
```
```
