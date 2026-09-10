"""Comparing a staged resource with what XNAT actually holds.

"Does this resource exist on XNAT?" was the question being asked, and it is the
wrong one: a resource holding 5 of 8 files has the same label as one holding all
8. Measured on a live XNAT by deleting 3 of 8 files from a resource, after which
every pass logged "Skipping upload of '<session>' as all the resources already
exist on XNAT".
"""

from xnat_ingest.helpers.remotes import (
    ResourceComparison,
    compare_resource_with_xnat,
)

LOCAL = {"a.dcm": "d1", "b.dcm": "d2", "c.dcm": "d3"}


def test_identical_is_complete_and_not_repairable() -> None:
    c = compare_resource_with_xnat(LOCAL, dict(LOCAL))
    assert c.complete
    assert not c.repairable, "nothing to repair when XNAT already has everything"


def test_xnat_missing_files_is_repairable() -> None:
    """THE CASE THIS EXISTS FOR: we hold a strict superset."""
    c = compare_resource_with_xnat(LOCAL, {"a.dcm": "d1"})
    assert not c.complete
    assert c.repairable
    assert c.missing == {"b.dcm", "c.dcm"}
    assert not c.extra and not c.differing


def test_extra_files_on_xnat_is_not_repairable() -> None:
    """XNAT holding files we do not cannot be resolved by uploading."""
    c = compare_resource_with_xnat(LOCAL, {**LOCAL, "surprise.dcm": "dX"})
    assert not c.complete
    assert not c.repairable, "an upload cannot remove a file we do not have"
    assert c.extra == {"surprise.dcm"}


def test_same_name_different_content_is_not_repairable() -> None:
    """Appending here would leave the wrong bytes in place."""
    c = compare_resource_with_xnat(LOCAL, {**LOCAL, "b.dcm": "DIFFERENT"})
    assert not c.complete
    assert not c.repairable
    assert c.differing == {"b.dcm"}


def test_missing_and_differing_together_is_not_repairable() -> None:
    """Missing files do not make a corrupt one safe to leave alone."""
    c = compare_resource_with_xnat(LOCAL, {"a.dcm": "d1", "b.dcm": "DIFFERENT"})
    assert c.missing == {"c.dcm"}
    assert c.differing == {"b.dcm"}
    assert not c.repairable, "content conflict must still need a human"


def test_empty_digests_fall_back_to_names_only() -> None:
    """XNAT leaves `digest` empty until a catalog refresh populates it.

    Measured against a live XNAT: every file reported digest '' immediately
    after upload, and only a refresh with the checksum option filled them in.
    Comparing content in that state would call every healthy resource corrupt.
    """
    c = compare_resource_with_xnat(LOCAL, {k: "" for k in LOCAL})
    assert not c.comparable
    assert c.complete, "names all present, so nothing is actually wrong"
    assert not c.differing, "cannot claim a difference with nothing to compare"


def test_empty_digests_still_detect_a_genuinely_missing_file() -> None:
    """Names are trustworthy even when digests are not."""
    c = compare_resource_with_xnat(LOCAL, {"a.dcm": "", "b.dcm": ""})
    assert not c.comparable
    assert c.missing == {"c.dcm"}
    assert c.repairable, "a missing name is still a missing name"


def test_default_comparison_is_complete() -> None:
    assert ResourceComparison().complete
    assert not ResourceComparison().repairable
