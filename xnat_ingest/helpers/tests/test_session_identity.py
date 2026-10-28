"""A matching accession is not evidence of matching patient or grouping identity."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_all_uploaded_completeness import LOCAL, RESOURCE_PATH, FakeXnat

from xnat_ingest.helpers.remotes import (
    IDENTITY_PATH,
    LocalSessionListing,
    S3SessionListing,
    get_xnat_session,
)
from xnat_ingest.model.session import ImagingSession


class IdentityXnat(FakeXnat):
    def __init__(self, subject="S1", uid="group-001"):
        super().__init__([("1", "scan_one", {"DICOM": dict(LOCAL)})])
        self.fields = {"ID": "E1", "subject_ID": subject, "UID": "unrelated-DICOM-UID"}
        self.subjects = [{"ID": "S1", "label": "subj"}]
        self.uid = uid
        self.puts = []
        self.headers = []
        self.xsi_type = "xnat:mrSessionData"

    def put(self, uri, query=None, accepted_status=None):
        self.puts.append((uri, query, accepted_status))
        assert uri == "/data/projects/proj/subjects/subj/experiments/E1"
        assert query == {f"{self.xsi_type}/{IDENTITY_PATH}": "group-001"}
        self.uid = query[f"{self.xsi_type}/{IDENTITY_PATH}"]
        return SimpleNamespace(status_code=200)

    def get_json(self, uri, query=None, headers=None):
        self.headers.append((uri, headers))
        if uri == "/data/experiments/E1":
            self.calls.append(uri)
            custom = (
                []
                if self.uid is None
                else [{"data_fields": {"name": "xnat_ingest_uid", "field": self.uid}}]
            )
            return {
                "items": [
                    {
                        "data_fields": dict(self.fields),
                        "meta": {"xsi:type": self.xsi_type},
                        "children": [{"field": "fields/field", "items": custom}],
                    }
                ]
            }
        if uri == "/data/projects/proj/subjects":
            return {"ResultSet": {"Result": self.subjects}}
        return super().get_json(uri, query)


def staged(tmp_path, uid="group-001"):
    root = tmp_path / "proj.subj.sess"
    resource = root / RESOURCE_PATH
    resource.mkdir(parents=True)
    (resource / "__MANIFEST__.json").write_text(json.dumps({"checksums": LOCAL}))
    (root / "__METADATA__.json").write_text(json.dumps({"__uid__": uid}))
    return LocalSessionListing(root)


def session(uid="group-001"):
    result = ImagingSession(
        uid="original-grouping-uid",
        project_id="proj",
        subject_id="subj",
        session_id="sess",
    )
    result.metadata = {
        "__uid__": uid,
        "Modality": "MR",
        "StudyInstanceUID": "different-DICOM-UID",
    }
    return result


def project(connection):
    existing = SimpleNamespace(uri="/data/experiments/E1", id="E1")
    connection.classes = SimpleNamespace(
        SubjectData=Mock(), MrSessionData=Mock(return_value=existing)
    )
    return SimpleNamespace(
        id="proj", xnat_session=connection, experiments={"sess": existing}
    )


@pytest.mark.parametrize(
    "subject,uid", [("S2", "group-001"), ("S1", "group-other"), (None, "group-001")]
)
def test_existing_experiment_identity_mismatch_refused_without_writes(subject, uid):
    connection = IdentityXnat(subject, uid)
    xproject = project(connection)
    with pytest.raises(ValueError, match="identity"):
        get_xnat_session(session(), xproject)
    connection.classes.SubjectData.assert_not_called()


@pytest.mark.parametrize(
    "subject,uid", [("S2", "group-001"), ("S1", "group-other"), (None, "group-001")]
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


@pytest.mark.parametrize("uid", [["group-001", "group-002"], {"bad": "shape"}])
def test_ambiguous_local_study_identity_is_refused(tmp_path, uid):
    with pytest.raises(ValueError, match="identity"):
        staged(tmp_path, uid).all_uploaded(IdentityXnat())


@pytest.mark.parametrize("subject", ["S1", "S2"])
def test_unknown_local_uid_still_checks_subject(tmp_path, subject):
    listing = staged(tmp_path, None)
    connection = IdentityXnat(subject=subject, uid="")
    if subject == "S1":
        assert listing.all_uploaded(connection)
    else:
        with pytest.raises(ValueError, match="identity"):
            listing.all_uploaded(connection)


def test_s3_metadata_is_checked_without_downloading_data(tmp_path):
    bucket = Mock()
    bucket.download_fileobj.side_effect = lambda key, stream, **kw: stream.write(
        b'{"__uid__": "group-other"}'
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
    kwargs = connection.classes.MrSessionData.call_args.kwargs
    assert kwargs[IDENTITY_PATH] == "group-001"
    assert "UID" not in kwargs
    assert connection.calls.count("/data/experiments/E1") == 1


@pytest.mark.parametrize(
    "response",
    [{}, {"items": []}, {"items": [{"data_fields": {}}]}, {"items": [{}, {}]}],
)
def test_malformed_identity_response_is_not_proof(tmp_path, response):
    connection = IdentityXnat()
    original = connection.get_json
    connection.get_json = lambda uri, query=None, headers=None: (
        response if uri == "/data/experiments/E1" else original(uri, query)
    )
    with pytest.raises(ValueError, match="identity"):
        staged(tmp_path).all_uploaded(connection)


def test_failed_subject_read_never_falls_back_to_upload(tmp_path):
    connection = IdentityXnat()
    original = connection.get_json

    def read(uri, query=None, headers=None):
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
    (root / "__METADATA__.json").write_text('{"__uid__": "group-other"}')
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


@pytest.mark.parametrize("uid", [None, ""])
def test_legacy_identity_is_adopted_only_in_upload(uid, tmp_path, caplog):
    connection = IdentityXnat(uid=uid)
    assert staged(tmp_path).all_uploaded(connection) is False
    assert connection.puts == []
    xproject = project(connection)
    assert get_xnat_session(session(), xproject) is xproject.experiments["sess"]
    assert connection.uid == "group-001"
    assert len(connection.puts) == 1
    assert connection.fields["UID"] == "unrelated-DICOM-UID"
    assert not any("pullDataFromHeaders" in uri for uri, _, _ in connection.puts)
    records = [
        r for r in caplog.records if getattr(r, "event", "") == "xnat_identity_adoption"
    ]
    assert len(records) == 1 and "unverified" in records[0].message
    assert all(
        headers == {"Cache-Control": "no-cache", "Pragma": "no-cache"}
        for uri, headers in connection.headers
        if uri == "/data/experiments/E1" or uri.endswith("/subjects")
    )
    assert staged(tmp_path / "second").all_uploaded(connection)


def test_legacy_other_subject_is_never_adopted():
    connection = IdentityXnat(subject="S2", uid=None)
    with pytest.raises(ValueError, match="identity"):
        get_xnat_session(session(), project(connection))
    assert connection.puts == []


@pytest.mark.parametrize(
    "xsi_type",
    ["xnat:mrSessionData", "xnat:ctSessionData", "xnat:otherDicomSessionData"],
)
def test_legacy_adoption_uses_fresh_type(xsi_type):
    connection = IdentityXnat(uid=None)
    connection.xsi_type = xsi_type
    xproject = project(connection)
    xproject.experiments["sess"].__xsi_type__ = "stale:wrongSessionData"
    get_xnat_session(session(), xproject)
    assert connection.puts == [
        (
            "/data/projects/proj/subjects/subj/experiments/E1",
            {f"{xsi_type}/{IDENTITY_PATH}": "group-001"},
            tuple(range(200, 300)),
        )
    ]


@pytest.mark.parametrize("updated", [None, "group-other"])
def test_identity_write_requires_matching_fresh_readback(updated):
    connection = IdentityXnat(uid=None)

    def no_update(uri, **kwargs):
        connection.puts.append(uri)
        connection.uid = updated
        return SimpleNamespace(status_code=200)

    connection.put = no_update
    with pytest.raises(ValueError, match="identity"):
        get_xnat_session(session(), project(connection))
    assert len(connection.puts) == 1


@pytest.mark.parametrize(
    "xsi_type", [None, "", "xnat:subjectData", "xnat:mrSessionData/UID"]
)
def test_legacy_adoption_refuses_invalid_fresh_type(xsi_type):
    connection = IdentityXnat(uid=None)
    connection.xsi_type = xsi_type
    with pytest.raises(ValueError, match="experiment type"):
        get_xnat_session(session(), project(connection))
    assert connection.puts == []


@pytest.mark.parametrize("status", [200, 201, 204, 302, 422, 500])
def test_identity_update_requires_successful_http(status):
    connection = IdentityXnat(uid=None)
    original = connection.put

    def answer(uri, **kwargs):
        response = original(uri, **kwargs)
        response.status_code = status
        return response

    connection.put = answer
    if 200 <= status < 300:
        assert get_xnat_session(session(), project(connection)).id == "E1"
    else:
        with pytest.raises(ValueError, match="identity update failed"):
            get_xnat_session(session(), project(connection))


def test_identity_transport_failure_is_an_identity_refusal():
    from xnat_ingest.exceptions import SessionIdentityRefused

    connection = IdentityXnat(uid=None)
    connection.put = Mock(side_effect=OSError("synthetic transport failure"))
    with pytest.raises(SessionIdentityRefused, match="identity update failed"):
        get_xnat_session(session(), project(connection))
    assert connection.uid is None


@pytest.mark.parametrize("value", [["group-001"], {"bad": "shape"}, " "])
def test_malformed_remote_uid_is_not_adopted(value):
    connection = IdentityXnat(uid=value)
    with pytest.raises(ValueError, match="identity"):
        get_xnat_session(session(), project(connection))
    assert connection.puts == []


@pytest.mark.parametrize(
    "children",
    [
        None,
        {},
        [{"field": "fields/field", "items": None}],
        [
            {
                "field": "fields/field",
                "items": [{"data_fields": {"name": "xnat_ingest_uid"}}],
            }
        ],
        [
            {
                "field": "fields/field",
                "items": [
                    {"data_fields": {"name": "xnat_ingest_uid", "field": "group-001"}}
                ]
                * 2,
            }
        ],
    ],
)
def test_ambiguous_custom_fields_refuse_before_mutation(children):
    connection = IdentityXnat()
    original = connection.get_json

    def read(uri, **kwargs):
        response = original(uri, **kwargs)
        if uri == "/data/experiments/E1":
            response["items"][0]["children"] = children
        return response

    connection.get_json = read
    with pytest.raises(ValueError, match="identity"):
        get_xnat_session(session(), project(connection))
    assert connection.puts == []


def test_creation_must_read_back_identity():
    connection = IdentityXnat(uid=None)
    xproject = project(connection)
    xproject.experiments = {}
    with pytest.raises(ValueError, match="identity"):
        get_xnat_session(session(), xproject)
    assert connection.puts == []  # No legacy adoption after failed creation.


def test_session_only_cannot_adopt_without_expected_subject(tmp_path):
    from xnat_ingest.helpers.remotes import SessionOnlyListing

    root = tmp_path / "sess"
    root.mkdir()
    (root / "__METADATA__.json").write_text('{"__uid__": "group-001"}')
    listing = SessionOnlyListing(root)
    connection = IdentityXnat(uid=None)
    assert listing.all_uploaded(connection) is False
    with pytest.raises(ValueError, match="identity"):
        listing.find_xnat_session_uri(connection)
    assert connection.puts == []


@pytest.mark.parametrize("uid", [None, "group-other", "group-001"])
def test_s3_session_only_checks_identity_without_data_download(
    tmp_path, uid, monkeypatch
):
    from xnat_ingest.helpers import remotes

    bucket = Mock()
    bucket.download_fileobj.side_effect = lambda key, stream: stream.write(
        b'{"__uid__": "group-001"}'
    )
    listing = remotes.S3SessionOnlyListing(
        "sess",
        bucket,
        [(["__METADATA__.json"], SimpleNamespace(key="metadata"))],
        tmp_path,
    )
    connection = IdentityXnat(uid=uid)
    monkeypatch.setattr(
        remotes,
        "_find_xnat_session_by_label",
        lambda *args: SimpleNamespace(uri="/data/experiments/E1"),
    )
    for lookup in [listing.find_xnat_session_uri, listing.find_xnat_session]:
        if uid == "group-001":
            assert lookup(connection) is not None
        else:
            with pytest.raises(ValueError, match="identity"):
                lookup(connection)
    assert not listing._downloaded
    assert connection.puts == []
