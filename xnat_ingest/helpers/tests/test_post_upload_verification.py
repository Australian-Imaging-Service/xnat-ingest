"""The post-upload checksum check must survive a partly-refreshed catalog.

It compared the two checksum dicts whole. XNAT reports an empty digest until a
catalog refresh populates it, so a resource just topped up holds real digests
for the files already there and empty ones for those just added, and a whole
dict comparison calls that a mismatch when nothing is wrong.

The comparison is per file, so a file with no digest is trusted by name.
"""

import logging
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from xnat_ingest.helpers.remotes import (
    compare_resource_with_xnat,
    get_xnat_resource,
    log_name_only_comparison,
)

CALCULATED = {
    "old1.dcm": "aaa",
    "old2.dcm": "bbb",
    "new1.dcm": "ccc",
    "new2.dcm": "ddd",
}


def test_freshly_uploaded_files_with_no_digest_are_not_a_mismatch() -> None:
    """THE REGRESSION: the shape a resource holds right after a repair."""
    remote = {"old1.dcm": "aaa", "old2.dcm": "bbb", "new1.dcm": "", "new2.dcm": ""}

    assert remote != CALCULATED, "the whole-dict comparison that used to fail"

    comparison = compare_resource_with_xnat(CALCULATED, remote)
    assert comparison.complete, (
        "a file XNAT has not digested yet is not a corrupt file, and failing "
        "the upload here would fail a repair that actually worked"
    )


def test_a_real_digest_mismatch_is_still_caught() -> None:
    """Trusting empty digests must not mean trusting every digest."""
    remote = dict(CALCULATED)
    remote["new1.dcm"] = "CORRUPTED"

    comparison = compare_resource_with_xnat(CALCULATED, remote)
    assert not comparison.complete
    assert comparison.differing == {"new1.dcm"}


def test_a_file_that_never_landed_is_still_caught() -> None:
    """Empty digests do not hide a file missing from the listing entirely."""
    remote = {"old1.dcm": "", "old2.dcm": "", "new1.dcm": ""}

    comparison = compare_resource_with_xnat(CALCULATED, remote)
    assert not comparison.complete
    assert comparison.missing == {"new2.dcm"}


def test_the_mismatch_report_can_be_built_without_unpacking_a_string() -> None:
    """The old report crashed before it could be read.

    `for k, v in intersect_keys` iterated a set of file NAMES and unpacked each
    one as a pair, so every genuine mismatch raised "too many values to unpack"
    instead of saying which files disagreed.
    """
    remote = dict(CALCULATED)
    remote["old2.dcm"] = "CORRUPTED"
    remote["extra.dcm"] = "eee"
    del remote["new2.dcm"]

    comparison = compare_resource_with_xnat(CALCULATED, remote)
    report = (
        f"Extra keys were {sorted(comparison.extra)}\n"
        f"Missing keys were {sorted(comparison.missing)}\n"
        f"Mismatching files were {sorted(comparison.differing)}\n"
    )
    assert "extra.dcm" in report
    assert "new2.dcm" in report
    assert "old2.dcm" in report


def test_names_are_checked_even_when_no_digests_exist_at_all() -> None:
    """The check used to do nothing on a site without enableChecksums.

    It was gated on any(remote_checksums.values()), so when XNAT returned no
    digests at all it logged "assuming upload was successful" and compared
    nothing, not even file names, which XNAT lists regardless. That is the
    configuration where the fail-closed guard on the repair filter is the only
    other net, and the same --dont-check-checksums flag removes both.

    AIS-Edge cares: the deployment plan for the production site still lists
    "is XNAT enableChecksums on" as an open question.
    """
    remote = {k: "" for k in list(CALCULATED)[:3]}  # one file never arrived

    comparison = compare_resource_with_xnat(CALCULATED, remote)
    assert not comparison.comparable, "no digests to compare"
    assert not comparison.complete, "but a missing NAME is still detectable"
    assert comparison.missing == {"new2.dcm"}


def test_no_digests_and_all_names_present_is_a_pass() -> None:
    """Names-only must not turn into a false alarm on a healthy upload."""
    remote = {k: "" for k in CALCULATED}

    comparison = compare_resource_with_xnat(CALCULATED, remote)
    assert not comparison.comparable
    assert comparison.complete


def test_a_name_only_comparison_is_logged_at_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Resource detail must not add a warning for each comparison."""
    comparison = compare_resource_with_xnat(CALCULATED, {k: "" for k in CALCULATED})
    assert comparison.complete
    assert not comparison.comparable

    with caplog.at_level(logging.DEBUG, logger="xnat-ingest"):
        log_name_only_comparison(comparison, "1:DICOM", "PROJECT.SUBJECT.SESSION")

    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings == []
    assert "compared by file name only" in caplog.text
    assert "1:DICOM" in caplog.text and "PROJECT.SUBJECT.SESSION" in caplog.text


def test_a_comparison_with_digests_logs_no_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    comparison = compare_resource_with_xnat(CALCULATED, dict(CALCULATED))
    assert comparison.comparable

    with caplog.at_level(logging.DEBUG, logger="xnat-ingest"):
        log_name_only_comparison(comparison, "1:DICOM", "PROJECT.SUBJECT.SESSION")

    assert not [r for r in caplog.records if r.levelno == logging.WARNING]


def test_partial_digests_log_debug_without_changing_comparison(caplog):
    remote = dict(CALCULATED, **{"new1.dcm": "", "new2.dcm": ""})
    comparison = compare_resource_with_xnat(CALCULATED, remote)
    assert comparison.complete and comparison.comparable
    assert comparison.without_checksums == {"new1.dcm", "new2.dcm"}
    with caplog.at_level(logging.DEBUG, logger="xnat-ingest"):
        log_name_only_comparison(comparison, "1:DICOM", "SESSION")
    assert "2 file(s) compared by file name only" in caplog.text
    remote["old1.dcm"] = "wrong"
    assert compare_resource_with_xnat(CALCULATED, remote).differing == {"old1.dcm"}


@pytest.mark.parametrize("scan_level", [False, True])
@pytest.mark.parametrize("repair", [False, True])
def test_existing_resource_logs_debug_without_warnings(caplog, scan_level, repair):
    remote_resource = object()
    session = SimpleNamespace(
        label="SESSION",
        xnat_session=SimpleNamespace(classes=SimpleNamespace()),
        resources={"REPORT": remote_resource},
        scans={
            "1": SimpleNamespace(resources={"REPORT": remote_resource}, files=["a"])
        },
    )
    resource = SimpleNamespace(
        name="REPORT",
        path="1:REPORT" if scan_level else "REPORT",
        scan=SimpleNamespace(id="1", path="SESSION:1") if scan_level else None,
        fileset=object(),
        metadata={},
        checksums={"a": "aaa", "b": "bbb"},
    )
    before = {"a": ""} if repair else {"a": "", "b": ""}
    with (
        caplog.at_level(logging.DEBUG, logger="xnat-ingest"),
        patch("xnat_ingest.helpers.remotes.get_xnat_checksums", return_value=before),
    ):
        result = get_xnat_resource(resource, session)
        assert result == ((remote_resource, {"b"}) if repair else (None, None))
        # Both the existing-resource and post-upload details stay at DEBUG.
        complete = compare_resource_with_xnat(resource.checksums, {"a": "", "b": ""})
        log_name_only_comparison(complete, resource.path, "SESSION")
    warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "compared by file name only" in r.message
    ]
    assert warnings == []
    assert "SESSION" in caplog.text and resource.path in caplog.text
    caplog.clear()
    with caplog.at_level(logging.DEBUG, logger="xnat-ingest"):
        log_name_only_comparison(complete, resource.path, "SESSION")
    assert "compared by file name only" in caplog.text


def test_empty_remote_listing_does_not_claim_a_name_comparison(caplog):
    comparison = compare_resource_with_xnat(CALCULATED, {})
    assert comparison.missing == set(CALCULATED)
    with caplog.at_level(logging.DEBUG, logger="xnat-ingest"):
        log_name_only_comparison(comparison, "1:DICOM", "SESSION")
    assert not caplog.records
