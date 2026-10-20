"""A matching accession is not evidence of matching patient or study."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_all_uploaded_completeness import LOCAL, RESOURCE_PATH, FakeXnat

from xnat_ingest.helpers.remotes import (
    LocalSessionListing,
    S3SessionListing,
    get_xnat_session,
)
from xnat_ingest.model.session import ImagingSession


class IdentityXnat(FakeXnat):
    def __init__(self, subject="S1", uid="1.2.3"):
        super().__init__([("1", "scan_one", {"DICOM": dict(LOCAL)})])
        self.fields = {"ID": "E1", "subject_ID": subject, "UID": uid, "project": "proj"}
        self.subjects = [{"ID": "S1", "label": "subj"}]

    def get_json(self, uri, query=None):
        if uri == "/data/experiments/E1":
            self.calls.append(uri)
            return {"items": [{"data_fields": dict(self.fields)}]}
        if uri == "/data/projects/proj/subjects":
            return {"ResultSet": {"Result": self.subjects}}
        return super().get_json(uri, query)


def staged(tmp_path, uid="1.2.3"):
    root = tmp_path / "proj.subj.sess"
    resource = root / RESOURCE_PATH
    resource.mkdir(parents=True)
    (resource / "__MANIFEST__.json").write_text(json.dumps({"checksums": LOCAL}))
    (root / "__METADATA__.json").write_text(json.dumps({"StudyInstanceUID": uid}))
    return LocalSessionListing(root)


def session(uid="1.2.3"):
    result = ImagingSession(
        uid="original-grouping-uid",
        project_id="proj",
        subject_id="subj",
        session_id="sess",
    )
    result.metadata = {"StudyInstanceUID": uid, "Modality": "MR"}
    return result


def project(connection):
    existing = SimpleNamespace(uri="/data/experiments/E1", id="E1")
    connection.classes = SimpleNamespace(SubjectData=Mock(), MrSessionData=Mock())
    return SimpleNamespace(
        id="proj", xnat_session=connection, experiments={"sess": existing}
    )


@pytest.mark.parametrize(
    "subject,uid", [("S2", "1.2.3"), ("S1", "9.8.7"), ("S1", ""), (None, "1.2.3")]
)
def test_existing_experiment_identity_mismatch_refused_without_writes(subject, uid):
    connection = IdentityXnat(subject, uid)
    xproject = project(connection)
    with pytest.raises(ValueError, match="identity"):
        get_xnat_session(session(), xproject)
    connection.classes.SubjectData.assert_not_called()


@pytest.mark.parametrize(
    "subject,uid", [("S2", "1.2.3"), ("S1", "9.8.7"), ("S1", ""), (None, "1.2.3")]
)
def test_all_uploaded_cannot_skip_other_subject_or_study(tmp_path, subject, uid):
    listing = staged(tmp_path)
    with pytest.raises(ValueError, match="identity"):
        listing.all_uploaded(IdentityXnat(subject, uid))


def test_matching_identity_and_shared_project_are_allowed(tmp_path):
    connection = IdentityXnat()
    connection.fields["project"] = "owning-project"  # shared subject's ID still matches
    listing = staged(tmp_path)
    assert listing.all_uploaded(connection)
    xproject = project(connection)
    assert get_xnat_session(session(), xproject) is xproject.experiments["sess"]
    connection.classes.SubjectData.assert_not_called()


def test_identity_is_fresh_on_every_pass(tmp_path):
    listing = staged(tmp_path)
    connection = IdentityXnat()
    assert listing.all_uploaded(connection)
    connection.fields["subject_ID"] = "S2"
    with pytest.raises(ValueError, match="identity"):
        listing.all_uploaded(connection)


@pytest.mark.parametrize("uid", [["1.2.3", "4.5.6"], {"bad": "shape"}])
def test_ambiguous_local_study_identity_is_refused(tmp_path, uid):
    with pytest.raises(ValueError, match="identity"):
        staged(tmp_path, uid).all_uploaded(IdentityXnat())


def test_unknown_local_uid_still_checks_subject(tmp_path):
    assert staged(tmp_path, None).all_uploaded(IdentityXnat(uid=""))


def test_s3_metadata_is_checked_without_downloading_data(tmp_path):
    bucket = Mock()
    bucket.download_fileobj.side_effect = lambda key, stream, **kw: stream.write(
        b'{"StudyInstanceUID": "9.8.7"}'
    )
    listing = S3SessionListing(
        "proj.subj.sess",
        bucket,
        [
            (
                ["__METADATA__.json"],
                SimpleNamespace(key="staged/proj.subj.sess/__METADATA__.json"),
            )
        ],
        tmp_path,
    )
    with pytest.raises(ValueError, match="identity"):
        listing.all_uploaded(IdentityXnat())
    assert bucket.download_fileobj.call_count == 1
    assert not listing._downloaded


def test_new_experiment_records_uid_with_creation():
    connection = IdentityXnat()
    xproject = project(connection)
    xproject.experiments = {}
    get_xnat_session(session(), xproject)
    assert connection.classes.MrSessionData.call_args.kwargs["UID"] == "1.2.3"


@pytest.mark.parametrize(
    "response",
    [{}, {"items": []}, {"items": [{"data_fields": {}}]}, {"items": [{}, {}]}],
)
def test_malformed_identity_response_is_not_proof(tmp_path, response):
    connection = IdentityXnat()
    original = connection.get_json
    connection.get_json = lambda uri, query=None: (
        response if uri == "/data/experiments/E1" else original(uri, query)
    )
    with pytest.raises(ValueError, match="identity"):
        staged(tmp_path).all_uploaded(connection)


def test_failed_subject_read_never_falls_back_to_upload(tmp_path):
    connection = IdentityXnat()
    original = connection.get_json

    def read(uri, query=None):
        if uri.endswith("/subjects"):
            raise OSError("transport unavailable")
        return original(uri, query)

    connection.get_json = read
    with pytest.raises(ValueError, match="identity"):
        staged(tmp_path).all_uploaded(connection)


def test_session_only_mode_checks_uid_when_supplied(tmp_path):
    from xnat_ingest.helpers.remotes import SessionOnlyListing

    root = tmp_path / "sess"
    root.mkdir()
    (root / "__METADATA__.json").write_text('{"StudyInstanceUID": "9.8.7"}')
    with pytest.raises(ValueError, match="identity"):
        SessionOnlyListing(root).all_uploaded(IdentityXnat())


def test_s3_metadata_download_failure_is_not_unknown_identity(tmp_path):
    bucket = Mock()
    bucket.download_fileobj.side_effect = OSError("transport unavailable")
    listing = S3SessionListing(
        "proj.subj.sess",
        bucket,
        [(["__METADATA__.json"], SimpleNamespace(key="metadata"))],
        tmp_path,
    )
    with pytest.raises(OSError, match="transport unavailable"):
        listing.all_uploaded(IdentityXnat())
