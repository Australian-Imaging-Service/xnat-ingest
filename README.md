
# xnat-siemens-export-upload

[![tests](https://github.com/Australian-Imaging-Service/xnat-siemens-export-upload/actions/workflows/tests.yml/badge.svg)](https://github.com/Australian-Imaging-Service/xnat-siemens-export-upload/actions/workflows/tests.yml)

Upload exported DICOM and list-mode data from Siemens Quadra "Total Body" PET scanner to
XNAT


## Installation

Build the docker image from the root directory of a clone of this code repository

```
docker build -t xnat-siemens-export-upload .
```


## Running

The root CLI command is set to be the entrypoint of the Docker image so it can be run
by

```
docker run xnat-siemens-export-upload --help
```
```
