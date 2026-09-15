"""The post-upload checksum check must survive a partly-refreshed catalog.

It compared the two checksum dicts whole. XNAT reports an empty digest until a
catalog refresh populates it, so a resource just topped up holds real digests
for the files already there and empty ones for those just added, and a whole
dict comparison calls that a mismatch when nothing is wrong.

The comparison is per file, so a file with no digest is trusted by name.
"""

from xnat_ingest.helpers.remotes import compare_resource_with_xnat

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
