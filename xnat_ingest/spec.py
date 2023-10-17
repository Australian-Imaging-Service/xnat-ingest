import typing as ty
import yaml
import logging
import attrs


logger = logging.getLogger("xnat-ingest")


@attrs.define
class DicomSpec:

    type: str


@attrs.define
class NonDicomSpec:

    name: str


@attrs.define
class UploadSpec:

    dicoms: ty.List[DicomSpec]
    non_dicoms: ty.List[NonDicomSpec]
