"""The completed-repair line, and the two phrases it must NOT contain.

Downstream alert rules (the AIS-Edge charts) report a repair ATTEMPT on
"Repaired <N> incomplete resource", which upload() logs before the verdict,
and an upload on "Successfully uploaded all files in". This line exists so an
alert can say a repair COMPLETED, and upload() logs it only after a clean
verdict. If it contained either phrase, a completion would be counted as a new
attempt, or reported as an ordinary upload.

As with test_incomplete_resource_alerting.py, this pins the code to itself:
the rules live in another repository. Rewording the line needs the rules
changed with it.
"""

import re

from xnat_ingest.api.upload_api import repair_completed_message

# What the downstream rules match. This line must stay clear of both.
ATTEMPT_FILTER = re.compile(r"Repaired [0-9]+ incomplete resource")
UPLOAD_FILTER = "Successfully uploaded all files in"


def test_no_repair_no_line() -> None:
    assert repair_completed_message("proj.subj.sess", []) is None


def test_line_names_the_session_and_every_resource_in_order() -> None:
    msg = repair_completed_message(
        "proj.subj.sess",
        ["proj:subj:sess:2-t2:DICOM", "proj:subj:sess:1-t1:DICOM"],
    )
    assert msg is not None
    assert msg.startswith(
        "Completed repair of 2 incomplete resource(s) on XNAT in 'proj.subj.sess'"
    )
    assert msg.index("1-t1") < msg.index("2-t2"), "resources are listed sorted"


def test_line_is_neither_an_attempt_nor_an_upload() -> None:
    msg = repair_completed_message("proj.subj.sess", ["proj:subj:sess:1-t1:DICOM"])
    assert msg is not None
    assert not ATTEMPT_FILTER.search(msg), (
        "the downstream attempt alert would count this completion as a new attempt"
    )
    assert UPLOAD_FILTER not in msg, (
        "a completed repair would be reported as an ordinary upload"
    )
