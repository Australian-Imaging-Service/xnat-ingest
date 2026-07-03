"""Helper functions and classes for defining custom Click parameter types for use in the CLI."""

from __future__ import annotations

import logging
import random
import re
import string
import typing as ty
from collections import Counter
from pathlib import Path
from datetime import datetime

import attrs
import click.types
from fileformats.core import DataType, FileSet, from_mime

from ..exceptions import ImagingSessionParseError

if ty.TYPE_CHECKING:
    from ..model.resource import ImagingResource
    from ..model.scan import ImagingScan
    from ..model.session import ImagingSession
    from .metadata import Metadata

    MetadataLike: ty.TypeAlias = ty.Union[
        "ImagingSession",
        "ImagingScan",
        "ImagingResource",
        "FileSet",
        Metadata,
        ty.Mapping[str, ty.Any],
    ]

logger = logging.getLogger("xnat-ingest")


def datatype_converter(
    datatype_str: ty.Union[str, ty.Type[DataType]],
) -> ty.Type[DataType]:
    if datatype_str == "all":
        return FileSet
    if isinstance(datatype_str, str):
        return from_mime(datatype_str)
    return datatype_str


class classproperty(object):
    def __init__(self, f: ty.Callable[..., ty.Any]) -> None:
        self.f = f

    def __get__(self, obj: object, owner: ty.Any) -> ty.Any:
        return self.f(owner)


class CliType(click.types.ParamType):

    is_composite = True

    def __init__(
        self,
        type_: ty.Union[ty.Type["CliTyped"], ty.Type["MultiCliTyped"]],
        multiple: bool = False,
    ):
        self.type = type_
        self.multiple = multiple

    def convert(
        self, value: ty.Any, param: click.Parameter | None, ctx: click.Context | None
    ) -> ty.Any:
        if isinstance(value, self.type):
            return value
        if len(attrs.fields(self.type)) == 1:
            return self.type(value)  # type: ignore[call-arg]
        return self.type(*value)

    @property
    def arity(self) -> int:  # type: ignore[override]
        return len(attrs.fields(self.type))

    @property
    def name(self) -> str:  # type: ignore[override]
        return type(self).__name__.lower()

    def split_envvar_value(self, envvar: str) -> ty.Any:
        if self.multiple:
            tokens = []
            for entry in envvar.split(";"):
                if not entry.strip():
                    continue
                args = entry.split(maxsplit=self.arity - 1)
                # Allow for default values supplied by the attrs type class
                tokens.extend(self._add_defaults_for_missing_args(args, self.type))
            return tokens
        else:
            args = envvar.split(maxsplit=self.arity - 1)
            return self._add_defaults_for_missing_args(args, self.type)

    def _add_defaults_for_missing_args(self, args: list[str], type_: type) -> list[str]:
        fields = attrs.fields(type_)
        if len(args) < len(fields):
            for field in fields[len(args) :]:
                if field.default is not attrs.NOTHING:
                    args.append(
                        field.default()
                        if isinstance(field.default, attrs.Factory)  # type: ignore[arg-type]
                        else field.default
                    )
                else:
                    raise click.BadParameter(
                        f"Not enough arguments provided for {type_.__name__}, "
                        f"missing value for '{field.name}' ({args})"
                    )
        return args


@attrs.define
class CliTyped:
    @classproperty
    def cli_type(cls) -> CliType:
        return CliType(cls)  # type: ignore[arg-type]


@attrs.define
class MultiCliTyped:
    @classproperty
    def cli_type(cls) -> CliType:
        return CliType(cls, multiple=True)  # type: ignore[arg-type]


def to_upper(value: str) -> str:
    return value.upper()


def to_lower(value: str) -> str:
    return value.lower()


@attrs.define
class LoggerConfig(MultiCliTyped):

    type: str
    loglevel: str
    location: str

    @property
    def loglevel_int(self) -> int:
        return getattr(logging, self.loglevel.upper())  # type: ignore[no-any-return]


@attrs.define
class PathMetadata(MultiCliTyped):

    regex: str
    datatype: ty.Type[FileSet] = attrs.field(converter=datatype_converter)


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
class AssociatedFiles(CliTyped):

    datatype: ty.Type[FileSet] = attrs.field(converter=datatype_converter)
    glob: str = attrs.field()
    identity_pattern: str = attrs.field()


@attrs.define
class XnatLogin(CliTyped):

    host: str
    user: str
    password: str


@attrs.define
class OrthancLogin(CliTyped):

    url: str
    user: str
    password: str
    storage_dir: Path = attrs.field(converter=Path)


@attrs.define
class StoreCredentials(CliTyped):

    access_key: str
    access_secret: str


@attrs.define
class IDSpec(MultiCliTyped):
    """Extract an ID to sort the data with (e.g. project, subject, session, scan,...) from
    either a metadata field or the path that the resource is saved with. If the 'type' is
    'field' then the specifier is the name of the field, if it is 'path', the specifier
    is a Python-style regular expression with a single matching group to select a portion
    of the path. 'datatype' restricts the specification to resources of that type (default
    is FileSet, i.e. any type), and 'formatter' is an optional format string (or strftime
    format for datetime values) applied to the extracted value.
    """

    specifier: str = attrs.field()
    datatype: ty.Type[FileSet] = attrs.field(
        converter=datatype_converter, default=FileSet
    )

    @property
    def specifier_name(self) -> str:
        """The plain metadata field name, with any '[index]' suffix stripped off"""
        match = re.match(r"(\w+)\[[\-\d:]+\]$", self.specifier)
        return match.group(1) if match else self.specifier

    def get_value(
        self,
        metadata: MetadataLike,
        escape: bool = True,
        missing_ids: dict[str, str] | None = None,
    ) -> str:
        """Get the value of the ID from the resource's metadata, applying any indexing and
        formatting specified in the IDSpec. If the metadata field is not found, a unique
        placeholder value will be generated and stored in the missing_ids dict if provided,
        otherwise an exception will be raised.

        Parameters
        ----------
        metadata: MetadataLike
            The metadata to extract the ID from
        escape: bool
            If True, the extracted value will be escaped to be a valid XNAT ID (alphanumeric and underscores only)
        missing_ids: dict[str, str] | None
            If provided, a dict to store any generated placeholder values for missing metadata fields, keyed by the field name

        Returns
        -------
        str
            The extracted ID value from the resource's metadata, formatted according to the IDSpec

        Raises
        ------
        ImagingSessionParseError
            If the metadata field is not found and missing_ids is not provided
        """
        if not isinstance(metadata, ty.Mapping):
            metadata = metadata.metadata
        if match := re.match(r"(\w+)\[([\-\d:]+)\]", self.specifier):
            _, index = match.groups()
            if ":" in index:
                index = slice(*(int(d) if d else None for d in index.split(":")))
            else:
                index = int(index)
        else:
            index = None
        try:
            value = metadata[self.specifier_name]
        except KeyError:
            value = ""
        if not value:
            if missing_ids is not None:
                try:
                    value = missing_ids[self.specifier_name]
                except KeyError:
                    value = missing_ids[self.specifier_name] = (
                        "INVALID_NOTFOUND_"
                        + re.sub(r"[^A-Z0-9_]", "_", self.specifier_name.upper())
                        + "_"
                        + "".join(
                            random.choices(string.ascii_letters + string.digits, k=8)
                        )
                    )
            else:
                raise ImagingSessionParseError(
                    f"Did not find '{self.specifier_name}' field in {metadata!r}, "
                    "cannot uniquely identify the resource, found:\n"
                    + "\n".join(metadata)
                )
        if index is not None:
            value = value[index]
            if isinstance(value, list):
                value = "_".join(value)
        elif isinstance(value, list):
            frequency = Counter(value)
            value = frequency.most_common(1)[0][0]
        value = str(value)
        if escape:
            value = self.xnat_id_escape_re.sub("_", value)
        return value

    def format_value(self, value: ty.Any):
        if self.formatter is None:
            formatted = value
        elif isinstance(value, ty.Mapping):
            formatted = self.formatter.format(**value)
        elif isinstance(value, ty.Iterable):
            formatted = self.formatter.format(*value)
        elif isinstance(value, datetime):
            formatted = value.strftime(self.formatter)
        else:
            raise TypeError(
                f"Unsupported type for value to format, {value}, for "
                f"formatter '{self.formatter}"
            )
        return invalid_path_chars_re.sub("_", str(formatted))

    xnat_id_escape_re = re.compile(r"[^a-zA-Z0-9_]+")

    @classmethod
    def get_value_from_matching_spec(
        cls,
        metadata: MetadataLike,
        id_fields: list["IDSpec"],
        missing_ids: dict[str, str] | None = None,
        escape: bool = True,
    ) -> str:
        """
        Given a list of IDSpec objects, find the first one that matches the type of the
        resource and use it to extract the ID value from the resource's metadata. If no
        matching IDSpec is found, raise a TypeError.

        Parameters
        ----------
        metadata: MetadataLike
            The metadata mapping, or object with 'metadata' attribute, to extract the ID from
        id_fields: list[IDSpec]
            A list of IDSpec objects to try to match against the resource's type
        missing_ids: dict[str, str] | None
            If provided, a dict to store any generated placeholder values for missing metadata fields, keyed by
            the field name
        escape: bool
            If True, the extracted value will be escaped to be a valid XNAT ID (alphanumeric and underscores only)

        Returns
        -------
        str
            The extracted ID value from the resource's metadata, formatted according to the matching IDSpec

        Raises
        ------
        TypeError
            If no matching IDSpec is found for the resource's type
        ImagingSessionParseError
            If the metadata field is not found and missing_ids is not provided
        """
        for id_field in id_fields:
            if isinstance(metadata, id_field.datatype):
                value = id_field.get_value(
                    metadata, escape=escape, missing_ids=missing_ids
                )
                logger.debug("Using %s to extract ID from %s", id_field, metadata)
                return value
        raise TypeError(
            f"No resource label field specification matches type of {metadata}, "
            f"provided {id_fields}"
        )


@attrs.define
class MimeType(str, MultiCliTyped):

    mime: str

    @property
    def datatype(self) -> ty.Type[DataType]:
        return from_mime(self.mime)


@attrs.define
class CollationSpec(MultiCliTyped):

    mime: str
    collation: str = attrs.field(default="siblings")

    @property
    def datatype(self) -> ty.Type[DataType]:
        return from_mime(self.mime)

    @property
    def collation_level(self) -> FileSet.CopyCollation:
        return FileSet.CopyCollation[self.collation.lower()]


class CopyModeParamType(click.ParamType):
    name = "copy_mode"

    def convert(
        self,
        value: str,
        param: ty.Optional[click.Parameter],
        ctx: ty.Optional[click.Context],
    ) -> FileSet.CopyMode:
        if isinstance(value, FileSet.CopyMode):
            return value
        try:
            # Allow case-insensitive matching on enum member names.
            return FileSet.CopyMode[value.lower()]
        except KeyError:
            self.fail(f"{value!r} is not a valid copy mode", param, ctx)


invalid_path_chars_re = re.compile(r'[\-<>:"/\\|?*\x00-\x1F]')
