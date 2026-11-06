"""Command-line argument types specific to XNAT"""

import typing as ty

import attrs
from fileformats.core import FileSet

from accessionflow.helpers.arg_types import CliTyped, MultiCliTyped, datatype_converter


@attrs.define
class UploadMethod(MultiCliTyped):
    method: str = attrs.field(
        validator=attrs.validators.in_(
            {"per_file", "tar_memory", "tgz_memory", "tar_file", "tgz_file"}
        )
    )
    datatype: ty.Type[FileSet] = attrs.field(converter=datatype_converter)

    @classmethod
    def select_method(
        cls, methods: list["UploadMethod"], datatype: ty.Type[FileSet]
    ) -> str:
        """Get the upload method for the given datatype"""
        for method in methods:
            if issubclass(datatype, method.datatype):
                return method.method
        return "tgz_file"


@attrs.define
class XnatLogin(CliTyped):
    host: str
    user: str
    password: str
