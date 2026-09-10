"""The session-label upload mode must obey the same completeness rule.

SessionOnlyListing sat OUTSIDE the SessionListing hierarchy and carried its own
copy of all_uploaded that compared resource LABELS:

    uploaded = {r.label for r in xsession.resources.values()}
    return uploaded.issuperset(self.resource_paths)

That is verbatim the defect the base class was fixed for, and because the class
was a duck-typed sibling rather than a subclass, the fix could not reach it. A
staging directory whose resource held 8 files, against an XNAT resource holding
3 of them, still reported as fully uploaded, so the session was skipped with no
per-resource check, no repair and no error, and on AIS-Edge the staged copy is
then age-reclaimed while XNAT keeps a fraction.

The two modes differ only in how they FIND their session: by project and label
here, by a global label search there. They must not differ in what counts as
uploaded. So find_xnat_session is the only override, and the completeness rule
has one implementation.

The mode is selected on a dot in the staging directory name (upload_api picks
LocalSessionListing when the name has one, SessionOnlyListing when it does not),
which is why every live test of the repair so far exercised only the other path.
"""

import json
import typing as ty
from unittest import mock

from xnat_ingest.helpers.remotes import SessionListing, SessionOnlyListing

LOCAL = {f"slice{i}.dcm": f"digest{i}" for i in range(8)}


class FakeXnatResource:
    label = "DICOM"


class FakeExperiment:
    label = "SESSLABEL"

    def __init__(self) -> None:
        self.scans: dict[str, ty.Any] = {}
        self.resources = {"DICOM": FakeXnatResource()}


class FakeConnection:
    def __init__(self) -> None:
        self.experiments = {"XNAT_E1": FakeExperiment()}


def _staging(tmp_path: ty.Any, checksums: dict[str, str]) -> SessionOnlyListing:
    """A session-only staging dir: no dots in the name, one resource."""
    session_dir = tmp_path / "SESSLABEL"
    resource_dir = session_dir / "DICOM"
    resource_dir.mkdir(parents=True)
    for name in checksums:
        (resource_dir / name).write_bytes(b"x")
    (resource_dir / "__MANIFEST__.json").write_text(
        json.dumps({"checksums": checksums, "datatype": "medimage/dicom-series"})
    )
    return SessionOnlyListing(session_dir)


def test_it_is_part_of_the_hierarchy() -> None:
    """The fix must not be able to miss this class again."""
    assert issubclass(SessionOnlyListing, SessionListing)
    assert (
        "all_uploaded" not in SessionOnlyListing.__dict__
    ), "a second copy of the completeness rule is how this bug survived"
    assert (
        "find_xnat_session" in SessionOnlyListing.__dict__
    ), "the mode still resolves its session by a global label search"


def test_short_resource_is_not_uploaded(tmp_path: ty.Any) -> None:
    """THE REGRESSION: 3 of 8 files on XNAT must not count as uploaded."""
    listing = _staging(tmp_path, LOCAL)
    on_xnat = {k: LOCAL[k] for k in list(LOCAL)[:3]}

    with mock.patch(
        "xnat_ingest.helpers.remotes.get_xnat_checksums", return_value=on_xnat
    ):
        assert listing.all_uploaded(FakeConnection()) is False, (
            "the label matches but 5 files are missing; reporting this as "
            "uploaded lets the staged copy be reclaimed while XNAT holds a "
            "fraction of the session"
        )


def test_complete_resource_is_uploaded(tmp_path: ty.Any) -> None:
    """A genuinely complete session must still be skipped."""
    listing = _staging(tmp_path, LOCAL)
    with mock.patch(
        "xnat_ingest.helpers.remotes.get_xnat_checksums", return_value=dict(LOCAL)
    ):
        assert listing.all_uploaded(FakeConnection()) is True


def test_missing_session_on_xnat_is_not_uploaded(tmp_path: ty.Any) -> None:
    """The label search finding nothing still means not uploaded."""
    listing = _staging(tmp_path, LOCAL)

    class Empty:
        experiments: dict[str, ty.Any] = {}

    assert listing.all_uploaded(Empty()) is False


def test_empty_digests_do_not_make_a_complete_session_look_short(
    tmp_path: ty.Any,
) -> None:
    """XNAT reports digest '' until a catalog refresh; names still count."""
    listing = _staging(tmp_path, LOCAL)
    with mock.patch(
        "xnat_ingest.helpers.remotes.get_xnat_checksums",
        return_value={k: "" for k in LOCAL},
    ):
        assert listing.all_uploaded(FakeConnection()) is True
