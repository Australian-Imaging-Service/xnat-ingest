"""Repairing a resource must never upload nothing and call it a success.

`only_files` names the files XNAT is missing, taken from the staged manifest,
which is keyed to the resource directory. The filter matches them against
`fspath.relative_to(fileset.parent)`, and FileSet.parent is commonpath() over
the staged files, so the two shapes are not guaranteed to agree.

ImagingResource.load() normally enforces that they do, but only under
check_checksums, and the same flag gates the post-upload verification. If the
filter then matches nothing, any positive batch size gives zero batches and
the code falls through to logging the resource as uploaded. So it fails closed.
"""

from pathlib import Path

import pytest

from xnat_ingest.api.upload_api import select_files_to_upload


def _flat(tmp_path: Path) -> tuple[list[Path], Path]:
    d = tmp_path / "DICOM"
    d.mkdir()
    paths = []
    for n in ("a.dcm", "b.dcm", "c.dcm"):
        p = d / n
        p.write_bytes(b"x")
        paths.append(p)
    return paths, d


def test_none_means_upload_everything(tmp_path: Path) -> None:
    paths, d = _flat(tmp_path)
    assert select_files_to_upload(paths, d, None, "proj:s:s:DICOM") == paths


def test_only_the_named_files_are_selected(tmp_path: Path) -> None:
    """The repair must not re-send what XNAT already holds."""
    paths, d = _flat(tmp_path)
    selected = select_files_to_upload(paths, d, {"b.dcm"}, "proj:s:s:DICOM")
    assert [p.name for p in selected] == ["b.dcm"]


def test_matching_nothing_raises_rather_than_uploading_nothing(
    tmp_path: Path,
) -> None:
    """THE REGRESSION. Names that match no staged path must not pass silently.

    This is the shape a nested resource directory produces when the load-time
    key check is disabled: the manifest says "sub/a.dcm", the filter computes
    "a.dcm", nothing matches.
    """
    paths, d = _flat(tmp_path)
    with pytest.raises(RuntimeError) as excinfo:
        select_files_to_upload(paths, d, {"sub/a.dcm"}, "proj:s:s:DICOM")

    msg = str(excinfo.value)
    assert "Refusing to repair" in msg
    assert "proj:s:s:DICOM" in msg
    assert "nothing would be uploaded" in msg, "say what would have happened"
    assert "Delete the resource on XNAT" in msg, "and what to do about it"


def test_a_partial_match_is_not_treated_as_a_failure(tmp_path: Path) -> None:
    """Only a total mismatch is unexplainable. Some matching is a real repair."""
    paths, d = _flat(tmp_path)
    selected = select_files_to_upload(
        paths, d, {"a.dcm", "not_staged.dcm"}, "proj:s:s:DICOM"
    )
    assert [p.name for p in selected] == ["a.dcm"]


def test_nested_layout_matches_when_the_shapes_do_agree(tmp_path: Path) -> None:
    """The nested case is fine as long as the manifest keys share the shape."""
    d = tmp_path / "DICOM"
    sub = d / "sub"
    sub.mkdir(parents=True)
    paths = []
    for n in ("a.dcm", "b.dcm"):
        p = sub / n
        p.write_bytes(b"x")
        paths.append(p)

    # commonpath collapses to sub/, so keys relative to it carry no "sub/"
    selected = select_files_to_upload(paths, sub, {"b.dcm"}, "proj:s:s:DICOM")
    assert [p.name for p in selected] == ["b.dcm"]


def test_empty_only_files_selects_nothing_without_raising(tmp_path: Path) -> None:
    """An empty set means nothing is missing, which is not a contradiction."""
    paths, d = _flat(tmp_path)
    assert select_files_to_upload(paths, d, set(), "proj:s:s:DICOM") == []
