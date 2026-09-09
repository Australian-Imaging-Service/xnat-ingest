"""A session must not be reported as clean when resources were left incomplete.

Resources skipped as "already uploaded" never enter `to_upload`, so they can
never reach `resource_errors`, so before this the success branch was taken and
the session logged "Successfully uploaded all files" while holding a fraction of
its data. Observed on a real deployment: 170 of 383 instances in XNAT, reported
as a success on every pass.
"""

from xnat_ingest.api.upload_api import session_upload_verdict


def test_clean_session_returns_none() -> None:
    """Nothing wrong means nothing to report, and the caller logs success."""
    assert (
        session_upload_verdict(
            session_name="proj.subj.sess",
            num_attempted=5,
            failed_paths=[],
            incomplete_paths=[],
        )
        is None
    )


def test_incomplete_alone_is_not_a_clean_session() -> None:
    """THE REGRESSION. Incomplete resources with zero failures must not be clean.

    This is exactly the shape of the real incident: every resource was skipped,
    so `failed_paths` was empty, and the session claimed success anyway.
    """
    msg = session_upload_verdict(
        session_name="proj.subj.sess",
        num_attempted=0,
        failed_paths=[],
        incomplete_paths=["proj:subj:sess:2-t1:DICOM"],
    )
    assert msg is not None, "a session with an incomplete resource is not clean"
    assert "did not upload cleanly" in msg
    assert "2-t1" in msg, "the operator needs to know WHICH resource"
    assert "Delete them on XNAT" in msg, "and what to do about it"


def test_failures_alone_are_reported() -> None:
    """The pre-existing behaviour is preserved."""
    msg = session_upload_verdict(
        session_name="proj.subj.sess",
        num_attempted=3,
        failed_paths=["proj:subj:sess:1-loc:DICOM"],
        incomplete_paths=[],
    )
    assert msg is not None
    assert "1 of 3 resource(s) failed to upload" in msg
    assert "Delete them on XNAT" not in msg, "no incomplete resources to delete"


def test_both_are_reported_together() -> None:
    """Neither category masks the other."""
    msg = session_upload_verdict(
        session_name="proj.subj.sess",
        num_attempted=4,
        failed_paths=["proj:subj:sess:1-loc:DICOM"],
        incomplete_paths=["proj:subj:sess:2-t1:DICOM", "proj:subj:sess:3-t2:DICOM"],
    )
    assert msg is not None
    assert "failed to upload" in msg
    assert "2 resource(s) already on XNAT but incomplete" in msg
    assert "1-loc" in msg and "2-t1" in msg and "3-t2" in msg
