
# Xnat-ingest

[![CI/CD](https://github.com/Australian-Imaging-Service/xnat-ingest/actions/workflows/tests.yml/ci-cd.svg)](https://github.com/Australian-Imaging-Service/xnat-ingest/actions/workflows/ci-cd.yml)
[![codecov](https://codecov.io/gh/Australian-Imaging-Service/xnat-ingest/graph/badge.svg?token=V860ZYIKQ3)](https://codecov.io/gh/Australian-Imaging-Service/xnat-ingest)

De-identify and upload exported DICOM and associated data files to XNAT based on ID values
stored within the DICOM headers.


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
