from pathlib import Path
from unittest.mock import MagicMock

import pytest

from xnat_ingest.helpers.remotes import SessionOnlyListing, list_session_dirs


def test_list_session_dirs_includes_no_dot_dirs(tmp_path: Path) -> None:
    (tmp_path / "PROJ.SUBJ.VISIT").mkdir()
    (tmp_path / "P000065").mkdir()
    (tmp_path / "__build__").mkdir()
    names = {d.name for d in list_session_dirs(tmp_path)}
    assert "PROJ.SUBJ.VISIT" in names
    assert "P000065" in names
    assert "__build__" not in names


def test_session_only_listing_resource_paths(tmp_path: Path) -> None:
    session_dir = tmp_path / "P000065"
    (session_dir / "my-report").mkdir(parents=True)
    (session_dir / "another-resource").mkdir()
    listing = SessionOnlyListing(session_dir)
    assert listing.resource_paths == {"my-report", "another-resource"}


def test_find_xnat_session_raises_on_multiple_matches(tmp_path: Path) -> None:
    listing = SessionOnlyListing(tmp_path / "P000065")
    connection = MagicMock()
    connection.experiments.values.return_value = [
        MagicMock(label="P000065"),
        MagicMock(label="P000065"),
    ]
    with pytest.raises(RuntimeError, match="Multiple XNAT sessions"):
        listing.find_xnat_session(connection)
