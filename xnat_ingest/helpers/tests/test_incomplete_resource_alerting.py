"""The wording of the incomplete-resource error is load-bearing.

The Loki rules shipped with the AIS-Edge charts match this message on a literal
phrase to raise XNATResourceIncompleteAndStuck, the operator alert for the one
failure mode this code deliberately does NOT repair: a resource that is short on
XNAT and also holds files the staging copy does not, which no upload can fix.

Both tiers require the same two substrings in the same message:

    | message =~ ".*already exists on XNAT with different checksums.*"
    | message =~ ".*Missing paths.*"

and the edge tier additionally uses the same phrase as an EXCLUSION in its
general error rule, so that this expected, alerted-on error does not also page
as an unexpected traceback.

Rewording it therefore does not just change a log line. It switches off the
alert, and nothing fails: the code still works, the tests still pass, and the
operator is simply never told. That happened once already, when the message was
reworded to "does not match the staged session" while clarifying it.
"""

import logging
import typing as ty
from unittest import mock

import pytest

from xnat_ingest.exceptions import IncompleteCheckumsException
from xnat_ingest.helpers.remotes import get_xnat_resource

# The literal substrings the shipped Loki rules match on.
#
# THIS PINS THE CODE TO ITSELF, WHICH IS WEAKER THAN IT LOOKS. The rules live in
# another repository, so nothing here can read them. If you reword the message,
# this test goes red, and editing these constants to match the new wording makes
# it green again while leaving the charts matching the old words and the alert
# dead. That is the most likely way this test gets defeated, so it is called out
# in the failure messages too.
#
# The enforcement that would actually hold is chart-side: a CI stage that
# extracts each match phrase from the rules files and checks it against the log
# strings of the image the chart pins. That closes rule against deployed
# artifact, which is the pair that decides whether an operator gets paged.
ALERT_PHRASE = "already exists on XNAT with different checksums"
ALERT_SECOND = "Missing paths"


class FakeResourceOnXnat:
    pass


class FakeScan:
    def __init__(self, resources: dict[str, ty.Any]) -> None:
        self.resources = resources
        self.files = ["something"]


class FakeXnatSession:
    def __init__(self, scan: FakeScan) -> None:
        self.scans = {"2": scan}
        self.xnat_session = mock.MagicMock()


class FakeStagedScan:
    id = "2"
    path = "test_project:SUBJ:SESS:2-t1_mprage_ax"


class FakeStagedResource:
    name = "DICOM"
    scan = FakeStagedScan()
    path = "test_project:SUBJ:SESS:2-t1_mprage_ax:DICOM"

    def __init__(self, checksums: dict[str, str]) -> None:
        self.checksums = checksums


def test_unrepairable_resource_logs_the_phrase_the_alert_matches(
    caplog: ty.Any,
) -> None:
    """THE REGRESSION: reword this and the operator alert stops firing."""
    staged = {f"s{i}.dcm": f"d{i}" for i in range(5)}
    on_xnat = {"s0.dcm": "d0", "unexpected.dcm": "dX"}  # short AND extra

    resource = FakeStagedResource(staged)
    xsession = FakeXnatSession(FakeScan({"DICOM": FakeResourceOnXnat()}))

    with caplog.at_level(logging.ERROR, logger="xnat-ingest"):
        with mock.patch(
            "xnat_ingest.helpers.remotes.get_xnat_checksums", return_value=on_xnat
        ):
            with pytest.raises(IncompleteCheckumsException):
                get_xnat_resource(resource, xsession)

    logged = "\n".join(r.getMessage() for r in caplog.records)
    assert ALERT_PHRASE in logged, (
        f"the shipped Loki rules match on {ALERT_PHRASE!r}; without it "
        "XNATResourceIncompleteAndStuck can never fire and the operator is "
        "never told about the case this code does not repair. IF YOU ARE "
        "REWORDING THE MESSAGE DELIBERATELY, change the rules files in ais-edge "
        "(charts/mgmt/files/loki-ruler-rules.yaml and "
        "charts/edge/files/loki-ruler-rules.yaml) in the same change. Editing "
        "ALERT_PHRASE to match the new wording turns this test green and leaves "
        "the alert dead."
    )
    assert (
        ALERT_SECOND in logged
    ), f"the rules require {ALERT_SECOND!r} in the SAME message as the phrase"


def test_both_alert_substrings_are_in_one_message(caplog: ty.Any) -> None:
    """The rules chain two matchers, so they must hold for a single line."""
    staged = {f"s{i}.dcm": f"d{i}" for i in range(3)}
    on_xnat = {"s0.dcm": "d0", "extra.dcm": "dX"}

    with caplog.at_level(logging.ERROR, logger="xnat-ingest"):
        with mock.patch(
            "xnat_ingest.helpers.remotes.get_xnat_checksums", return_value=on_xnat
        ):
            with pytest.raises(IncompleteCheckumsException):
                get_xnat_resource(
                    FakeStagedResource(staged),
                    FakeXnatSession(FakeScan({"DICOM": FakeResourceOnXnat()})),
                )

    assert any(
        ALERT_PHRASE in r.getMessage() and ALERT_SECOND in r.getMessage()
        for r in caplog.records
    ), "two matchers on separate lines match nothing"
