import json
import os
import typing as ty
from itertools import chain
from pathlib import Path

import attrs


@attrs.define
class Metadata(ty.Mapping[str, ty.Any]):
    """A dictionary-like object to provide access to an object's metadata, lazily-reading
    from the object if it is not present in the dictionary that has been loaded from a
    JSON within the object's data dir"""

    _dct: dict[str, ty.Any]
    _obj: ty.Any
    _read: bool = False

    def __getitem__(self, key: str) -> ty.Any:
        """Get item from the metadata object, lazily reading from the underlying
        object if the key isn't found been loaded"""
        try:
            return self._dct[key]
        except KeyError:
            self._ensure_read()
            try:
                return self._dct[key]
            except KeyError:
                raise KeyError(f"{self._obj} doesn't have metadata for key '{key}'")

    def __setitem__(self, key: str, value: ty.Any) -> None:
        self._dct[key] = value

    def __iter__(self) -> ty.Iterator[str]:
        return iter(self.keys())

    def __len__(self) -> int:
        self._ensure_read()
        return len(self._dct)

    def __bool__(self) -> bool:
        return len(self) > 0

    def keys(self) -> ty.KeysView[str]:
        self._ensure_read()
        return self._dct.keys()

    def __contains__(self, key: str) -> bool:
        return key in self.keys()

    def get(self, key: str, default: ty.Any = None) -> ty.Any:
        try:
            return self[key]
        except KeyError:
            return default

    def update(self, other: ty.Mapping[str, ty.Any]) -> None:
        self._dct.update(other)

    def _ensure_read(self) -> None:
        if not self._read:
            self._dct.update(self._obj.load_metadata())
            self._read = True

    def save(self, data_dir: Path) -> None:
        """Write the metadata file, but ONLY if its content would change.

        WHY THE COMPARISON. This is called once per scan directory and once per
        session directory on every pass. Under `--loop` a stage reprocesses the same
        sessions repeatedly, and an unconditional write moved every mtime each cycle
        even when nothing had changed. Both upload paths refuse to touch a session
        that was modified recently (`upload --wait-period`, and the s3 uploader's
        settle window), so the session never settled and was never uploaded. The
        DICOMs were never the problem: ImagingResource.save already returns early when
        checksums match. It was these three writes.

        WHY TEMP-AND-RENAME. os.replace is atomic within a filesystem, so a reader
        never sees a half-written file. The previous form truncated in place, which
        made a truncated __METADATA__.json a reachable artefact if the process died
        mid-write.
        """
        # Pull in any not-yet-loaded metadata from the underlying object (e.g. file
        # headers, path-regex fields set on the fileset) so the persisted JSON is the
        # full picture, not just whatever happens to have been accessed so far.
        #
        # BEFORE the comparison, not after: comparing a partially-populated _dct
        # against a fully-populated file on disk would report a difference every pass
        # and defeat the short-circuit, and writing it would replace a complete file
        # with a partial one.
        self._ensure_read()
        # 'default=str' handles values that aren't natively JSON-serialisable but
        # have a sensible string representation, e.g. pydicom's PersonName
        serialised = json.dumps(self._dct, default=str, indent=4)
        fspath = data_dir / self.FNAME
        try:
            if fspath.read_text() == serialised:
                return
        except (OSError, UnicodeDecodeError):
            # Absent, unreadable or not text: fall through and write it.
            pass
        tmp = fspath.with_name(fspath.name + ".tmp")
        tmp.write_text(serialised)
        os.replace(tmp, fspath)

    @classmethod
    def load(cls, data_dir: Path, obj: ty.Any) -> ty.Self:
        with open(data_dir / cls.FNAME) as f:
            dct = json.load(f)
        return cls(dct, obj)

    FNAME = "__METADATA__.json"

    @classmethod
    def collate(cls, metadata_list: ty.Iterable[ty.Self]) -> dict[str, ty.Any]:
        """Collates series metadata dictionaries into a single dictionary spanning the
        union of all keys present across the entries. If a key resolves to a single
        distinct value across the entries that define it, that value is stored as a
        singleton. If it holds more than one distinct value, the per-entry values are
        stored as a list, aligned with metadata_dicts, with None standing in for entries
        where the key isn't present"""
        metadata_list = list(metadata_list)
        all_keys = set(chain(*(m.keys() for m in metadata_list)))
        collated: dict[str, ty.Any] = {}
        for key in all_keys:
            values = [m[key] if key in m else None for m in metadata_list]
            distinct: list[ty.Any] = []
            for val in values:
                if val is not None and val not in distinct:
                    distinct.append(val)
            if len(distinct) == 0:
                collated[key] = None
            elif len(distinct) == 1:
                collated[key] = distinct[0]
            else:
                collated[key] = values
        return collated
