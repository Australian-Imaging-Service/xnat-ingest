import typing as ty
from pathlib import Path

import pytest

from xnat_ingest.helpers.metadata import Metadata, collate_metadata


class MockObj:
    """Stand-in for the object a Metadata instance lazily reads from"""

    def __init__(self, to_load: dict[str, ty.Any] | None = None) -> None:
        self.to_load = to_load if to_load is not None else {}
        self.load_calls = 0

    def load_metadata(self) -> dict[str, ty.Any]:
        self.load_calls += 1
        return self.to_load


# Metadata.__getitem__


def test_getitem_returns_preloaded_value_without_reading():
    obj = MockObj({"a": "should-not-be-used"})
    metadata = Metadata({"a": 1}, obj)

    assert metadata["a"] == 1
    assert obj.load_calls == 0


def test_getitem_lazily_reads_missing_key_from_object():
    obj = MockObj({"b": 2})
    metadata = Metadata({}, obj)

    assert metadata["b"] == 2
    assert obj.load_calls == 1


def test_getitem_only_reads_from_object_once():
    obj = MockObj({"b": 2})
    metadata = Metadata({}, obj)

    metadata["b"]
    with pytest.raises(KeyError):
        metadata["missing"]

    assert obj.load_calls == 1


def test_getitem_raises_key_error_for_unknown_key():
    obj = MockObj({"a": 1})
    metadata = Metadata({}, obj)

    with pytest.raises(KeyError, match="doesn't have metadata for key 'unknown'"):
        metadata["unknown"]


def test_getitem_does_not_reread_once_already_read():
    obj = MockObj({})
    metadata = Metadata({}, obj, True)

    with pytest.raises(KeyError):
        metadata["missing"]
    assert obj.load_calls == 0


# Metadata dict-like protocol (keys/__iter__/__len__/__bool__)


def test_keys_triggers_lazy_read():
    obj = MockObj({"a": 1, "b": 2})
    metadata = Metadata({}, obj)

    assert set(metadata.keys()) == {"a", "b"}
    assert obj.load_calls == 1


def test_iter_yields_keys():
    obj = MockObj({"a": 1, "b": 2})
    metadata = Metadata({}, obj)

    assert set(metadata) == {"a", "b"}


def test_len_reflects_number_of_keys():
    obj = MockObj({"a": 1, "b": 2, "c": 3})
    metadata = Metadata({}, obj)

    assert len(metadata) == 3


def test_bool_false_when_empty():
    obj = MockObj({})
    metadata = Metadata({}, obj)

    assert not metadata


def test_bool_true_when_populated():
    obj = MockObj({"a": 1})
    metadata = Metadata({}, obj)

    assert metadata


# Metadata.save / Metadata.load


def test_save_and_load_roundtrip(tmp_path: Path):
    obj = MockObj({})
    metadata = Metadata({"a": 1, "b": [1, 2, 3]}, obj)

    metadata.save(tmp_path)

    loaded = Metadata.load(tmp_path, obj)

    assert loaded["a"] == 1
    assert loaded["b"] == [1, 2, 3]


def test_save_writes_to_expected_filename(tmp_path: Path):
    obj = MockObj({})
    metadata = Metadata({"a": 1}, obj)

    metadata.save(tmp_path)

    assert (tmp_path / Metadata.FNAME).exists()


# collate_metadata


def test_collate_metadata_common_values_are_singletons():
    obj = MockObj({})
    metadata_dicts = [
        Metadata({"Modality": "MR", "SeriesNumber": 1}, obj, True),
        Metadata({"Modality": "MR", "SeriesNumber": 2}, obj, True),
    ]

    collated = collate_metadata(metadata_dicts)

    assert collated["Modality"] == "MR"


def test_collate_metadata_differing_values_become_lists():
    obj = MockObj({})
    metadata_dicts = [
        Metadata({"SeriesNumber": 1}, obj, True),
        Metadata({"SeriesNumber": 2}, obj, True),
        Metadata({"SeriesNumber": 3}, obj, True),
    ]

    collated = collate_metadata(metadata_dicts)

    assert collated["SeriesNumber"] == [1, 2, 3]


def test_collate_metadata_only_diverges_after_common_prefix():
    obj = MockObj({})
    metadata_dicts = [
        Metadata({"SeriesNumber": 1}, obj, True),
        Metadata({"SeriesNumber": 1}, obj, True),
        Metadata({"SeriesNumber": 2}, obj, True),
    ]

    collated = collate_metadata(metadata_dicts)

    assert collated["SeriesNumber"] == [1, 1, 2]


def test_collate_metadata_spans_union_of_keys():
    obj = MockObj({})
    metadata_dicts = [
        Metadata({"a": 1, "b": 2}, obj, True),
        Metadata({"a": 1}, obj, True),
    ]

    collated = collate_metadata(metadata_dicts)

    # "b" is missing from the second entry, but since it only ever takes a single
    # distinct (non-None) value, it collapses to a singleton rather than a series
    assert collated == {"a": 1, "b": 2}


def test_collate_metadata_empty_metadata_entry_treated_as_missing():
    obj = MockObj({})
    metadata_dicts = [
        Metadata({"a": 1}, obj, True),
        Metadata({}, obj, True),
        Metadata({"a": 1}, obj, True),
    ]

    collated = collate_metadata(metadata_dicts)

    assert collated == {"a": 1}


def test_collate_metadata_preserves_none_placeholder_within_a_series():
    obj = MockObj({})
    metadata_dicts = [
        Metadata({"x": "A"}, obj, True),
        Metadata({}, obj, True),
        Metadata({"x": "B"}, obj, True),
    ]

    collated = collate_metadata(metadata_dicts)

    assert collated["x"] == ["A", None, "B"]


def test_collate_metadata_accepts_a_generator():
    obj = MockObj({})
    metadata_dicts = (Metadata({"a": v}, obj, True) for v in (1, 2))

    collated = collate_metadata(metadata_dicts)

    assert collated["a"] == [1, 2]


def test_collate_metadata_lazily_reads_unread_entries():
    obj1 = MockObj({"a": 1})
    obj2 = MockObj({"a": 2})
    metadata_dicts = [Metadata({}, obj1), Metadata({}, obj2)]

    collated = collate_metadata(metadata_dicts)

    assert collated["a"] == [1, 2]
    assert obj1.load_calls == 1
    assert obj2.load_calls == 1


def test_collate_metadata_empty_input_returns_empty_dict():
    assert collate_metadata([]) == {}
