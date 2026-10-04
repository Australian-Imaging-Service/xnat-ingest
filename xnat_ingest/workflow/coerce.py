"""Shared YAML -> xnat-ingest-object coercion for the ``workflow`` command.

Every composite CLI type in ``helpers.arg_types`` (``IDSpec``, ``ClashSpec``,
``PathMetadataRegex``, ``MetadataTable``, ...) is an ``attrs`` class whose fields
already have converters that accept plain strings (``datatype_converter``,
``table_file_converter``, ...). :func:`construct` builds one of these from a YAML
value using the same three shapes ``CliType.convert`` accepts from the CLI - a
mapping as keywords, a list/tuple positionally, or a bare scalar for a single-field
type - so a workflow YAML entry and its CLI-flag equivalent always parse to the same
object with no duplicated parsing logic.
"""

from __future__ import annotations

import typing as ty

from fileformats.core import FileSet

from ..helpers.arg_types import (
    ClashSpec,
    CollationSpec,
    Convert,
    IDSpec,
    MetadataTable,
    OnResourceClash,
    PathMetadataRegex,
    StoreCredentials,
    UploadMethod,
    datatype_converter,
)


def construct(cls: type, value: ty.Any) -> ty.Any:
    """Build an attrs composite type from a YAML value: a mapping is passed as
    keywords, a list/tuple positionally, and a bare scalar as the sole positional
    argument (only valid for a single-field type)."""
    if isinstance(value, cls):
        return value
    if isinstance(value, ty.Mapping):
        return cls(**value)
    if isinstance(value, (list, tuple)):
        return cls(*value)
    return cls(value)


def datatype(value: ty.Any) -> ty.Type[FileSet]:
    return datatype_converter(value)  # type: ignore[return-value]


def datatypes(value: ty.Iterable[ty.Any] | None) -> list[ty.Type[FileSet]]:
    return [datatype_converter(v) for v in (value or [])]  # type: ignore[misc]


def id_specs(value: ty.Any) -> list[IDSpec]:
    """A bare string/mapping is treated as a single spec; otherwise a list of
    strings/mappings/2-item lists, one per ``IDSpec``."""
    if not value:
        return []
    if isinstance(value, (str, ty.Mapping)):
        value = [value]
    return [construct(IDSpec, v) for v in value]


def path_metadata_regexes(value: ty.Any) -> list[PathMetadataRegex]:
    return [construct(PathMetadataRegex, v) for v in (value or [])]


def on_resource_clash(value: ty.Any) -> "OnResourceClash | list[ClashSpec]":
    """A bare policy string ('error'/'avoid'/'merge'/'overwrite') applies to any
    clash; a list of {policy, scope} entries is datatype-scoped (``group`` only -
    ``deidentify``/``associate`` only accept the bare-string form)."""
    if value is None:
        return "error"
    if isinstance(value, str):
        return value  # type: ignore[return-value]
    return [construct(ClashSpec, v) for v in value]


def metadata_tables(value: ty.Any) -> list[MetadataTable]:
    tables = []
    for v in value or []:
        if isinstance(v, ty.Mapping):
            v = dict(v)
            # 'path' is a friendlier YAML alias for MetadataTable's 'table_file'.
            if "path" in v and "table_file" not in v:
                v["table_file"] = v.pop("path")
            # 'joins' is a friendlier YAML alias for MetadataTable's 'join_exprs':
            # a mapping of column -> value-expr, rather than a list of
            # 'column=value-expr' strings.
            joins = v.pop("joins", None)
            if joins is not None and "join_exprs" not in v:
                v["join_exprs"] = (
                    [f"{k}={val}" for k, val in joins.items()]
                    if isinstance(joins, ty.Mapping)
                    else joins
                )
        tables.append(construct(MetadataTable, v))
    return tables


def collation_map(
    value: ty.Any,
) -> dict[ty.Type[FileSet], "FileSet.CopyCollation"] | None:
    if not value:
        return None
    result = {}
    for v in value:
        cs = construct(CollationSpec, v)
        result[cs.datatype] = cs.collation_level
    return result


def conversion_map(value: ty.Any) -> dict[ty.Type[FileSet], ty.Type[FileSet]] | None:
    if not value:
        return None
    result = {}
    for v in value:
        c = construct(Convert, v)
        result[c.source] = c.target
    return result


def upload_methods(value: ty.Any) -> list[UploadMethod]:
    return [construct(UploadMethod, v) for v in (value or [])]


def store_credentials(value: ty.Any) -> StoreCredentials | None:
    if not value:
        return None
    return construct(StoreCredentials, value)


def copy_mode(value: ty.Any) -> "FileSet.CopyMode":
    if isinstance(value, str):
        return FileSet.CopyMode[value.lower()]
    return value  # type: ignore[return-value]
