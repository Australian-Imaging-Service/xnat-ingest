"""Helper functions and classes for defining custom Click parameter types for use in the CLI."""

import logging
import random
import re
import string
import typing as ty
from collections import Counter

import attrs
import click.types
from fileformats.core import DataType, FileSet, from_mime

from ..exceptions import ImagingSessionParseError
from ..model.resource import ImagingResource

logger = logging.getLogger("xnat-ingest")


def datatype_converter(
    datatype_str: ty.Union[str, ty.Type[DataType]],
) -> ty.Type[DataType]:
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
                args = entry.split(",", maxsplit=self.arity - 1)
                # Allow for default values supplied by the attrs type class
                tokens.extend(self._add_defaults_for_missing_args(args, self.type))
            return tokens
        else:
            args = envvar.split(",", maxsplit=self.arity - 1)
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


@attrs.define
class LoggerConfig(MultiCliTyped):

    type: str
    loglevel: str
    location: str

    @property
    def loglevel_int(self) -> int:
        return getattr(logging, self.loglevel.upper())  # type: ignore[no-any-return]


@attrs.define
class UploadMethod(MultiCliTyped):

    method: str = attrs.field(
        validator=attrs.validators.in_(
            {"per_file", "tar_memory", "tgz_memory", "tar_file", "tgz_file"}
        )
    )
    datatype: ty.Type[FileSet] = attrs.field(converter=datatype_converter)


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
class StoreCredentials(CliTyped):

    access_key: str
    access_secret: str


@attrs.define
class FieldSpec(MultiCliTyped):

    field: str = attrs.field()
    datatype: ty.Type[FileSet] = attrs.field(
        converter=datatype_converter, default=FileSet
    )

    @property
    def field_name(self) -> str:
        return self.field.split("[")[0]

    def get_value(
        self, resource: ImagingResource, missing_ids: dict[str, str] | None = None
    ) -> str:
        if match := re.match(r"(\w+)\[([\-\d:]+)\]", self.field):
            field_name, index = match.groups()
            if ":" in index:
                index = slice(*(int(d) if d else None for d in index.split(":")))
            else:
                index = int(index)
        else:
            index = None
        try:
            value = resource.metadata[self.field_name]
        except KeyError:
            value = ""
        if not value:
            if missing_ids is not None:
                try:
                    value = missing_ids[self.field_name]
                except KeyError:
                    value = missing_ids[self.field_name] = (
                        "INVALID_MISSING_"
                        + re.sub(r"[^A-Z0-9_]", "_", self.field_name.upper())
                        + "_"
                        + "".join(
                            random.choices(string.ascii_letters + string.digits, k=8)
                        )
                    )
            else:
                raise ImagingSessionParseError(
                    f"Did not find '{self.field_name}' field in {resource!r}, "
                    "cannot uniquely identify the resource, found:\n"
                    + "\n".join(resource.metadata)
                )
        if index is not None:
            value = value[index]
            if isinstance(value, list):
                value = "_".join(value)
        elif isinstance(value, list):
            frequency = Counter(value)
            value = frequency.most_common(1)[0][0]
        value_str = str(value)
        value_str = invalid_path_chars_re.sub("_", value_str)
        return value_str

    @classmethod
    def get_value_from_fields(
        cls,
        resource: ImagingResource,
        id_fields: list["FieldSpec"],
        missing_ids: dict[str, str] | None = None,
    ) -> ty.List["FieldSpec"]:
        for id_field in id_fields:
            if isinstance(resource, id_field.datatype):
                return id_field.get_value(resource, missing_ids=missing_ids)
        raise ValueError(
            f"No resource label field specification matches type of {resource}, "
            f"provided {id_fields}"
        )


@attrs.define
class MimeType(str, MultiCliTyped):

    mime: str

    @property
    def datatype(self) -> ty.Type[DataType]:
        return from_mime(self.mime)


invalid_path_chars_re = re.compile(r'[\-<>:"/\\|?*\x00-\x1F]')
