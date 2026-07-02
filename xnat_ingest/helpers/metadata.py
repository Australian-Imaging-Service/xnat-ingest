from itertools import chain
import json
from pathlib import Path
import typing as ty


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

    def _ensure_read(self) -> None:
        if not self._read:
            self._dct.update(self._obj.load_metadata())
            self._read = True

    def save(self, data_dir: Path) -> None:
        with open(data_dir / self.FNAME, "w") as f:
            # 'default=str' handles values that aren't natively JSON-serialisable but
            # have a sensible string representation, e.g. pydicom's PersonName
            json.dump(self._dct, f, default=str, indent=4)

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
