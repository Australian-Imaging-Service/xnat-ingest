"""Helper functions and classes for defining custom Click parameter types for use in the CLI."""

from __future__ import annotations

import csv
import functools
import logging
import operator
import random
import re
import string
import types
import typing as ty
from collections import Counter
from pathlib import Path

import attrs
import click.types
from dateutil import parser as dateutil_parser
from fileformats.core import DataType, FileSet, from_mime, from_paths
from fileformats.core.exceptions import FormatRecognitionError
from fileformats.text import Csv, Tsv
from fileformats.vendor.openxmlformats_officedocument.application import (
    Spreadsheetml_Sheet,
)

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

# Define a type for the --on-resource-clash option, which can be one of "avoid", "merge", or "error".
# The tuple is the single source of truth: it doubles as the click.Choice() options, while the
# Literal (derived from it) is used for type-checking the parameter/argument annotations.
ON_RESOURCE_CLASH = ("error", "avoid", "merge", "overwrite")
OnResourceClash = ty.Literal["error", "avoid", "merge", "overwrite"]


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
        if len(self._init_fields(self.type)) == 1:
            return self.type(value)  # type: ignore[call-arg]
        return self.type(*value)

    @staticmethod
    def _init_fields(type_: type) -> list[ty.Any]:
        """The attrs fields that are actually settable via ``__init__`` - i.e. the ones
        the CLI needs to supply values for. Fields declared ``init=False`` (e.g. a lazily
        populated cache) still show up in ``attrs.fields()`` but must be excluded from the
        parameter arity and default-filling, otherwise the type is constructed with too
        many positional args."""
        return [f for f in attrs.fields(type_) if f.init]

    @property
    def arity(self) -> int:  # type: ignore[override]
        return len(self._init_fields(self.type))

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
        fields = self._init_fields(type_)
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
class Convert(MultiCliTyped):

    source: ty.Type[FileSet] = attrs.field(converter=datatype_converter)
    target: ty.Type[FileSet] = attrs.field(converter=datatype_converter)


@attrs.define
class PathMetadataRegex(MultiCliTyped):

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


class _PlaceholderStr(str):
    """A plain placeholder string that tolerates being substituted into a
    strftime-style ('%...') format spec (e.g. '{MissingDateField:%Y%m%d}') by just
    rendering itself as-is, rather than raising - a normal ``str`` doesn't understand
    '%' format codes and would otherwise turn a missing-field placeholder into a
    ``ValueError`` instead of the placeholder text it's meant to be.
    """

    def __format__(self, format_spec: str) -> str:
        if format_spec and "%" in format_spec:
            return str(self)
        return super().__format__(format_spec)


@attrs.define
class IDSpec(MultiCliTyped):
    """Extract an ID to sort the data with (e.g. project, subject, session, scan,...)
    from the resource's metadata. 'specifier' is either:

    - the name of a metadata field, optionally with a '[index]' or '[start:end]' slice
      suffix to select part of a list/string value, e.g. 'SeriesNumber' or
      'ImageType[2:]'
    - a Python format string over the metadata fields, to compose an ID from more
      than one field and/or apply formatting, e.g.
      '{PatientID}_{AcquisitionDate:%Y%m%d}' (detected by the presence of '{' in the
      specifier). Fields with a strftime-style ('%...') format spec are parsed from
      plain strings into dates first if needed (via `dateutil`), since metadata that
      has round-tripped through JSON (e.g. reloaded in a later pipeline stage) loses
      its original date/datetime typing. Only named fields can be referenced this way
      - an all-digit field name (as DICOM falls back to for private/unnamed tags)
      can't be, since Python's format-string syntax always treats an all-digit name as
      a positional index rather than a keyword lookup.

    'datatype' restricts the specification to resources of that type (default is
    FileSet, i.e. any type).
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
        if "{" in self.specifier:
            value = self._get_formatted_value(metadata, missing_ids=missing_ids)
        else:
            value = self._get_field_value(metadata, missing_ids=missing_ids)
        if escape:
            value = self.xnat_id_escape_re.sub("_", value)
        return value

    def _missing_field_placeholder(
        self,
        field_name: str,
        metadata: ty.Mapping[str, ty.Any],
        missing_ids: dict[str, str] | None,
    ) -> str:
        """Generate (or reuse) a unique placeholder for a metadata field that wasn't
        found, or raise if no missing_ids dict was provided to hold it"""
        if missing_ids is not None:
            try:
                return missing_ids[field_name]
            except KeyError:
                placeholder = missing_ids[field_name] = _PlaceholderStr(
                    "INVALID_MISSING_"
                    + re.sub(r"[^A-Z0-9_]", "_", field_name.upper())
                    + "_"
                    + "".join(random.choices(string.ascii_letters + string.digits, k=8))
                )
                return placeholder
        raise ImagingSessionParseError(
            f"Did not find '{field_name}' field in {metadata!r}, "
            "cannot uniquely identify the resource, found:\n" + "\n".join(metadata)
        )

    def _get_field_value(
        self,
        metadata: ty.Mapping[str, ty.Any],
        missing_ids: dict[str, str] | None,
    ) -> str:
        """Handles today's plain 'FieldName' / 'FieldName[index]' specifier syntax"""
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
            value = self._missing_field_placeholder(
                self.specifier_name, metadata, missing_ids
            )
        if index is not None:
            value = value[index]
            if isinstance(value, list):
                value = "_".join(value)
        elif isinstance(value, list):
            frequency = Counter(value)
            value = frequency.most_common(1)[0][0]
        return str(value)

    def _get_formatted_value(
        self,
        metadata: ty.Mapping[str, ty.Any],
        missing_ids: dict[str, str] | None,
    ) -> str:
        """Handles the '{Field}_{OtherField:spec}'-style format-string specifier
        syntax, composing an ID from one or more metadata fields"""
        values: dict[str, ty.Any] = {}
        for _, field_name, format_spec, _ in string.Formatter().parse(self.specifier):
            if not field_name or field_name.isdigit():
                # Skip literal text segments and positional ('{}'/'{0}') fields,
                # which aren't meaningful for metadata-field lookups
                continue
            base_name = re.split(r"[.\[]", field_name, maxsplit=1)[0]
            if base_name in values:
                continue
            value = metadata.get(base_name, "")
            if not value:
                value = self._missing_field_placeholder(
                    base_name, metadata, missing_ids
                )
            elif isinstance(value, str) and format_spec and "%" in format_spec:
                try:
                    value = dateutil_parser.parse(value)
                except (dateutil_parser.ParserError, ValueError, OverflowError):
                    pass
            values[base_name] = value
        try:
            return str(self.specifier.format(**values))
        except IndexError:
            # An all-digit field name (e.g. '{00100010}', as DICOM falls back to for
            # private/unnamed tags) is always parsed by str.format as a *positional*
            # index rather than a keyword lookup, regardless of what's in `values` -
            # so it can't be supported directly. Fail clearly rather than let a raw,
            # confusing IndexError propagate.
            raise ImagingSessionParseError(
                f"Specifier '{self.specifier}' references an all-digit field name, "
                "which can't be resolved from metadata directly (only named fields "
                "are supported in format-string specifiers) - use "
                "'--path-metadata-regex' to give it a proper name first if needed"
            ) from None

    # '-' is permitted in XNAT IDs/labels (and BIDS-style 'sub-01' labels rely on
    # it); everything else outside [A-Za-z0-9_-] is collapsed to '_'
    xnat_id_escape_re = re.compile(r"[^a-zA-Z0-9_-]+")

    @classmethod
    def get_value_from_matching_spec(
        cls,
        metadata: MetadataLike,
        id_fields: ty.Sequence["IDSpec"],
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


def parse_join_exprs(exprs: str | list[str] | list[JoinExpr]) -> list[JoinExpr]:
    if isinstance(exprs, str):
        exprs = [e.replace(r"\,", ",") for e in re.split(r"(?<!\\),", exprs)]
    parsed = []
    for expr in exprs:
        if isinstance(expr, JoinExpr):
            parsed.append(expr)
        else:
            try:
                column_name, value_expr = expr.split("=", 1)
                parsed.append(
                    JoinExpr(
                        column_name=column_name.strip(), value_expr=value_expr.strip()
                    )
                )
            except ValueError:
                raise ValueError(
                    f"Invalid join expression format: '{expr}'. Expected 'column_name=value_expr'."
                )
    return parsed


def table_file_converter(value: str | Path | FileSet) -> FileSet:
    if isinstance(value, FileSet):
        return value
    if isinstance(value, (str, Path)):
        if match := re.match(r"([^\[]+)\[(.*)\]", str(value).strip()):
            return from_mime(match.group(2))(match.group(1))
        filesets = from_paths([value], *MetadataTable.DEFAULT_FILE_TYPES)
        if filesets:
            return filesets[0]
        raise ValueError(
            f"No valid filesets found for value: {value} (from candidate types {MetadataTable.DEFAULT_FILE_TYPES})"
        )
    raise TypeError(
        f"Invalid type for table_file: {type(value).__name__}. Expected str, Path, or FileSet."
    )


def row_frequency_converter(
    value: str | type[FileSet] | types.UnionType | ty.Iterable[type[FileSet] | str],
) -> str | type[FileSet] | types.UnionType:
    """Normalise the ``--metadata-table`` row-frequency arg. The hierarchy levels
    'session', 'scan' and 'resource' are returned as-is; 'fileset' and a
    'fileset[<mime-like>]' spec (or an iterable of ``FileSet`` types / mime-like
    strings) are resolved to the ``FileSet`` type (or ``|``-union of types) they name -
    'fileset' itself becomes the base ``FileSet`` class - ready to be used directly
    with ``isinstance()``."""
    if isinstance(value, str):
        value = value.lower()
        if value in {"session", "scan", "resource"}:
            return value
        if value == "fileset":
            return FileSet
        if not (value.startswith("fileset[") and value.endswith("]")):
            raise ValueError(
                f"Invalid frequency '{value}'. Must be one of 'session', 'scan', "
                "'resource', 'fileset', 'fileset[<mime-type>]' (multiple mime-types "
                "can be '|'-separated, e.g. 'fileset[image/png|image/jpeg]')."
            )
        mime_like = value[len("fileset[") : -1]
        try:
            return from_mime(mime_like)  # type: ignore[return-value]
        except FormatRecognitionError as e:
            raise ValueError(
                f"Invalid row_frequency '{value}'. Could not recognise mime type "
                f"'{mime_like}'"
            ) from e
    if isinstance(value, types.UnionType) or (
        isinstance(value, type) and issubclass(value, FileSet)
    ):
        return value  # already resolved (e.g. attrs re-running the converter)
    if isinstance(value, ty.Iterable):
        resolved: list[type[FileSet]] = []
        for v in value:
            if isinstance(v, type) and issubclass(v, FileSet):
                resolved.append(v)
            elif isinstance(v, str):
                resolved.append(from_mime(v.lower()))  # type: ignore[arg-type]
            else:
                raise TypeError(
                    f"Invalid entry in row_frequency list: {v!r}. "
                    "Expected a mime-like str or a FileSet subclass."
                )
        if not resolved:
            raise ValueError("row_frequency iterable must not be empty")
        return functools.reduce(operator.or_, resolved)  # type: ignore[return-value]
    raise TypeError(
        f"Invalid type for row_frequency: {type(value).__name__}. "
        "Expected a str or an iterable of str/FileSet subclasses."
    )


@attrs.define
class JoinExpr(MultiCliTyped):

    column_name: str
    value_expr: str

    def value(self, context: dict[str, ty.Any]) -> str:
        if "{" in self.value_expr:
            value = self.value_expr.format(**context)
        else:
            value = str(context[self.value_expr])
        return value


@attrs.define
class MetadataTable(MultiCliTyped):
    """A tabular metadata source (CSV/TSV) whose rows are joined onto the input data and
    merged into the corresponding objects' metadata.

    Parameters
    ----------
    table_file : FileSet | str | Path
        The metadata table file. A str/Path is auto-detected as CSV or TSV from its
        extension; append '[<mime-type>]' to force a format, e.g. 'table.dat[text/csv]'.
    row_frequency : str | type[FileSet] | types.UnionType
        What each row corresponds to: one of 'session', 'scan', 'resource', 'fileset'
        or 'fileset[<mime-like>]'. 'fileset' resolves to the base ``FileSet`` class and
        'fileset[<mime-like>]' to the named ``FileSet`` type (or '|'-union of types), so
        the value is usable directly with ``isinstance()``.
    join_exprs : list[JoinExpr] | str
        One or more '<column-name>=<cell-value>' expressions (comma-separated in string
        form). A row matches a target when every expression holds; '<cell-value>' is a
        metadata field name or a Python format string over metadata fields
        (e.g. '{PatientID}_{SessionID}'). On a match, all columns of that row are merged
        into the target's metadata.
    """

    table_file: FileSet = attrs.field(converter=table_file_converter)
    row_frequency: str | type[FileSet] | types.UnionType = attrs.field(
        converter=row_frequency_converter
    )
    join_exprs: list[JoinExpr] = attrs.field(converter=parse_join_exprs)
    _table: ty.Any = attrs.field(default=None, init=False, eq=False, repr=False)

    DEFAULT_FILE_TYPES = (Csv, Tsv, Spreadsheetml_Sheet)

    @join_exprs.validator
    def _validate_join_exprs(
        self, attribute: attrs.Attribute, value: list[JoinExpr]
    ) -> None:
        if not value:
            raise ValueError("At least one join expression must be provided.")

    def inject(
        self, target: ImagingSession | ImagingScan | ImagingResource | FileSet
    ) -> None:
        """Merge the row of this table that matches ``target`` into its metadata.

        Does nothing if ``target``'s type doesn't match ``row_frequency``, if no row
        matches the join expressions, or if a join field is absent from ``target``'s
        metadata. Raises ``ValueError`` if more than one row matches.

        Parameters
        ----------
        target : ImagingSession | ImagingScan | ImagingResource | FileSet
            The object whose metadata to inject into.
        """
        from ..model.resource import ImagingResource
        from ..model.scan import ImagingScan
        from ..model.session import ImagingSession

        rf = self.row_frequency
        if (
            (rf == "session" and isinstance(target, ImagingSession))
            or (rf == "scan" and isinstance(target, ImagingScan))
            or (rf == "resource" and isinstance(target, ImagingResource))
            # rf is a FileSet type (bare 'FileSet', a subclass, or a '|'-union of them)
            # -> match raw filesets of it
            or (not isinstance(rf, str) and isinstance(target, rf))
        ):
            row_ids: set[int] | None = None
            for expr in self.join_exprs:
                try:
                    column = self.table[expr.column_name]
                except KeyError:
                    raise ValueError(
                        f"Join column '{expr.column_name}' not found in metadata table "
                        f"{self.table_file} (available columns: {list(self.table)})"
                    ) from None
                try:
                    value = expr.value(context=target.metadata)
                except KeyError:
                    # A field referenced by the join expression isn't present in the
                    # target's metadata, so this table simply can't match this target
                    return
                matching = {i for i, v in enumerate(column) if str(v) == str(value)}
                row_ids = matching if row_ids is None else (row_ids & matching)
                if not row_ids:
                    return
            if not row_ids:
                return
            if len(row_ids) > 1:
                raise ValueError(
                    f"Multiple rows ({sorted(i + 1 for i in row_ids)}) in metadata "
                    f"table {self.table_file} match the join expressions for {target}"
                )
            row_id = next(iter(row_ids))
            # Inject every column of the matched row, not just the join columns
            target.metadata.update(
                {column: values[row_id] for column, values in self.table.items()}
            )
        # Pass and don't inject metadata if the row frequency doesn't match the target type

    @property
    def table(self) -> ty.Mapping[str, list[ty.Any]]:
        """The metadata table as a column-oriented mapping of column name to the list of
        that column's cell values (one entry per row)."""
        if self._table is None:
            self._table = self._load_table()
        return self._table

    def _load_table(self) -> dict[str, list[ty.Any]]:
        if isinstance(self.table_file, (Csv, Tsv)):
            delimiter = "\t" if isinstance(self.table_file, Tsv) else ","
            with open(self.table_file.fspath, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=delimiter)
                fieldnames = list(reader.fieldnames or [])
                columns: dict[str, list[ty.Any]] = {n: [] for n in fieldnames}
                for row in reader:
                    for name in fieldnames:
                        columns[name].append(row.get(name))
            return columns
        # Fall back to the format's own loader (e.g. spreadsheets) and coerce whatever
        # it returns into the same column-oriented shape
        loaded = self.table_file.load()
        if hasattr(loaded, "to_dict"):  # e.g. a pandas DataFrame
            loaded = loaded.to_dict("list")
        if isinstance(loaded, ty.Mapping):
            return {str(k): list(v) for k, v in loaded.items()}
        raise TypeError(
            f"Could not interpret the loaded contents of {self.table_file} "
            f"(type {type(loaded).__name__}) as a metadata table"
        )

    @classmethod
    def inject_list(
        cls,
        tables: list[MetadataTable] | None,
        targets: list[ImagingSession | ImagingScan | ImagingResource | FileSet],
    ) -> None:
        """
        Inject metadata from a list of metadata tables into the target objects.

        Parameters
        ----------
        tables : list[MetadataTable]
            The list of metadata tables to inject metadata from.
        targets : list[ImagingSession | ImagingScan | ImagingResource | FileSet]
            The list of target objects to inject metadata into.
        """
        if tables is None:
            return
        for table in tables:
            for target in targets:
                table.inject(target)
