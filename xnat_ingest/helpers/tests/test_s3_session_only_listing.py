import typing as ty
from pathlib import Path
from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

from xnat_ingest.api.upload_api import upload
from xnat_ingest.helpers.arg_types import StoreCredentials
from xnat_ingest.helpers.remotes import (
    S3SessionListing,
    S3SessionOnlyListing,
    iterate_s3_sessions,
)

BUCKET = "session-label-test-bucket"
CREDENTIALS = StoreCredentials(access_key="key", access_secret="secret")


def _listings(tmp_path: Path) -> list[ty.Any]:
    sessions = iterate_s3_sessions(
        f"s3://{BUCKET}/desired_structure",
        CREDENTIALS,
        tmp_path,
        wait_period=0,
    )
    assert next(sessions) == 2
    return list(sessions)


def test_s3_session_label_folder_uses_session_only_listing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=BUCKET)
        for key in (
            "desired_structure/000077/radiology-doc-report/radiology-doc-report.pdf",
            "desired_structure/PROJECT.SUBJECT.SESSION/REPORT/report.pdf",
        ):
            s3.put_object(Bucket=BUCKET, Key=key, Body=b"%PDF-1.4\n")

        listings = _listings(tmp_path)

    by_name = {listing.name: listing for listing in listings}
    session_only = by_name["000077"]
    assert isinstance(session_only, S3SessionOnlyListing)
    assert session_only.session_id == "000077"
    assert session_only.is_session_only
    assert session_only.resource_paths == {"radiology-doc-report"}

    assigned = by_name["PROJECT.SUBJECT.SESSION"]
    assert type(assigned) is S3SessionListing
    assert not assigned.is_session_only
    assert assigned.project_id == "PROJECT"
    assert assigned.subject_id == "SUBJECT"
    assert assigned.session_id == "SESSION"


def _session_only_listing(tmp_path: Path) -> S3SessionOnlyListing:
    return S3SessionOnlyListing(
        name="000077",
        bucket=MagicMock(),
        objects=[],
        cache_path=tmp_path,
    )


def test_s3_session_label_finds_one_accessible_session(tmp_path: Path) -> None:
    listing = _session_only_listing(tmp_path)
    expected = MagicMock(label="000077")
    connection = MagicMock()
    connection.experiments.values.return_value = [
        MagicMock(label="another-session"),
        expected,
    ]

    assert listing.find_xnat_session(connection) is expected


def test_s3_session_label_returns_none_when_session_is_missing(tmp_path: Path) -> None:
    listing = _session_only_listing(tmp_path)
    connection = MagicMock()
    connection.experiments.values.return_value = [MagicMock(label="another-session")]

    assert listing.find_xnat_session(connection) is None


def test_s3_session_label_rejects_multiple_sessions(tmp_path: Path) -> None:
    listing = _session_only_listing(tmp_path)
    connection = MagicMock()
    connection.experiments.values.return_value = [
        MagicMock(label="000077"),
        MagicMock(label="000077"),
    ]

    with pytest.raises(RuntimeError, match="Multiple XNAT sessions found"):
        listing.find_xnat_session(connection)


class _XnatResource:
    def __init__(self) -> None:
        self.uploads: list[tuple[Path, set[str]]] = []

    def upload_dir(self, path: Path, method: ty.Any) -> None:
        self.uploads.append((path, {item.name for item in path.iterdir()}))


class _Connection:
    def __init__(self, labels: tuple[str, ...] = ("000077",)) -> None:
        self.session = MagicMock(label="000077", uri="/data/experiments/XNAT_E1")
        self.experiments = MagicMock()
        self.experiments.values.return_value = [
            self.session if label == "000077" else MagicMock(label=label)
            for label in labels
        ]
        self.created_resource = _XnatResource()
        self.put_uris: list[str] = []

    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *exc: ty.Any) -> ty.Literal[False]:
        return False

    def clearcache(self) -> None:
        pass

    def put(self, uri: str) -> None:
        self.put_uris.append(uri)

    def create_object(self, uri: str) -> _XnatResource:
        assert uri == "/data/experiments/XNAT_E1/resources/radiology-doc-report"
        return self.created_resource


class _Repo:
    def __init__(self, labels: tuple[str, ...] = ("000077",)) -> None:
        self.connection = _Connection(labels)


def test_uploads_s3_pdf_to_existing_session_resource(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=BUCKET)
        s3.put_object(
            Bucket=BUCKET,
            Key=(
                "desired_structure/000077/radiology-doc-report/radiology-doc-report.pdf"
            ),
            Body=b"%PDF-1.4\n%%EOF\n",
        )
        repo = _Repo()
        with patch.object(S3SessionOnlyListing, "all_uploaded", return_value=False):
            errors = upload(
                input_dir=f"s3://{BUCKET}/desired_structure",
                xnat_repo=repo,  # type: ignore[arg-type]
                always_include=("all",),
                store_credentials=CREDENTIALS,
                require_manifest=False,
                wait_period=0,
                s3_cache_dir=tmp_path,
                raise_errors=True,
            )

    expected_uri = "/data/experiments/XNAT_E1/resources/radiology-doc-report"
    assert errors == []
    assert repo.connection.put_uris == [expected_uri]
    assert len(repo.connection.created_resource.uploads) == 1
    assert repo.connection.created_resource.uploads[0][1] == {
        "radiology-doc-report.pdf"
    }


def test_s3_session_label_upload_fails_when_session_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=BUCKET)
        s3.put_object(
            Bucket=BUCKET,
            Key="desired_structure/000077/radiology-doc-report/report.pdf",
            Body=b"%PDF-1.4\n%%EOF\n",
        )
        repo = _Repo(labels=())
        with (
            patch.object(S3SessionOnlyListing, "all_uploaded", return_value=False),
            pytest.raises(
                RuntimeError,
                match="No XNAT session found with label '000077'",
            ),
        ):
            upload(
                input_dir=f"s3://{BUCKET}/desired_structure",
                xnat_repo=repo,  # type: ignore[arg-type]
                always_include=("all",),
                store_credentials=CREDENTIALS,
                require_manifest=False,
                wait_period=0,
                s3_cache_dir=tmp_path,
                raise_errors=True,
            )
