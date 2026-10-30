import typing as ty

from ..exceptions import ImagingSessionParseError
from .arg_types import IDSpec
from .logging import logger
from .metadata import Metadata


class Fields(Metadata):
    """XNAT custom variables for a session, persisted as ``__FIELDS__.json``.

    Reuses ``Metadata``'s atomic, write-only-if-changed ``save`` and its ``load``, but
    has no lazy source: values come only from the file or from :meth:`extract`, never
    from the owner's children (``Metadata._ensure_read`` would otherwise collate the
    session's scan metadata into this file).
    """

    FNAME = "__FIELDS__.json"

    def _ensure_read(self) -> None:
        self._read = True

    def extract(
        self,
        metadata: ty.Mapping[str, ty.Any],
        specs: ty.Mapping[str, IDSpec],
    ) -> dict[str, str]:
        """Resolve each XNAT field's ``IDSpec`` against ``metadata`` and store the
        results in these fields.

        Values are not escaped (``escape=False``): they are field values, not XNAT
        IDs. A field that can't be resolved (missing or empty key, bad index or
        format spec) is skipped with a warning rather than failing the caller.

        Returns
        -------
        dict[str, str]
            the fields that were resolved and stored
        """
        extracted: dict[str, str] = {}
        for field_name, spec in specs.items():
            try:
                extracted[field_name] = spec.get_value(metadata, escape=False)
            except (ImagingSessionParseError, IndexError, ValueError, TypeError) as e:
                logger.warning(
                    "Could not resolve XNAT field '%s' from '%s', skipping: %s",
                    field_name,
                    spec.specifier,
                    e,
                )
        self.update(extracted)
        return extracted
