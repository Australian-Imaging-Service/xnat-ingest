from itertools import chain
import json
from pathlib import Path
import typing as ty


import attrs


@attrs.define
class Metadata:
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
            if not self._read:
                self._dct.update(self._obj.load_metadata())
            try:
                return self._dct[key]
            except KeyError:
                raise KeyError(f"{self._obj} doesn't have metadata for key '{key}'")

    def __setattr__(self, key: str, val: ty.Any) -> None:
        self._dict[key] = val

    def save(self, data_dir: Path) -> None:
        with open(data_dir / self.FNAME, "w") as f:
            json.dump(self._dct, f)

    @classmethod
    def load(cls, data_dir: Path, obj: ty.Any) -> ty.Self:
        with open(data_dir / cls.FNAME) as f:
            dct = json.load(f)
        return cls(dct, obj)

    FNAME = "__METADATA__.json"


def collate_metadata(metadata_dicts: list[Metadata]) -> dict[str, ty.Any]:
    """Collates series metadata dictionaries into a single dictionary where common
    values are stored as singletons and varying values are stored in lists"""
    all_keys = [list(d.metadata.keys()) for d in metadata_dicts if d.metadata]
    common_keys = [
        k for k in set(chain(*all_keys)) if all(k in keys for keys in all_keys)
    ]
    collated = {k: metadata_dicts[0].metadata[k] for k in common_keys}
    for i, resource in enumerate(metadata_dicts[1:], start=1):
        for key in common_keys:
            if not resource.metadata:
                continue
            val = resource.metadata[key]
            if val != collated[key]:
                # Check whether the value is the same as the values in the previous
                # images in the series
                if (
                    not isinstance(collated[key], list)
                    or isinstance(val, list)
                    and not isinstance(collated[key][0], list)
                ):
                    collated[key] = [collated[key]] * i + [val]
                collated[key].append(val)
    return collated
