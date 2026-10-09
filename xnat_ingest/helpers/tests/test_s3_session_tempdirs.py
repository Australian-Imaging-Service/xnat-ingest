"""The S3 session iterator must not leave session downloads behind."""

import typing as ty
from pathlib import Path
from unittest import mock

import boto3
import pytest
from moto import mock_aws

from xnat_ingest.api.upload_api import upload
from xnat_ingest.helpers.arg_types import StoreCredentials
from xnat_ingest.helpers.remotes import S3SessionListing, iterate_s3_sessions

BUCKET = "tempdir-test-bucket"
SESSION = "PROJ.SUBJ.SESS"
CREDENTIALS = StoreCredentials(access_key="key", access_secret="secret")


@pytest.fixture
def staged_bucket(monkeypatch: pytest.MonkeyPatch) -> ty.Iterator[str]:
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=BUCKET)
        s3.put_object(
            Bucket=BUCKET, Key=f"staged/{SESSION}/1.T1/DICOM/1.dcm", Body=b"dcm"
        )
        yield f"s3://{BUCKET}/staged"


def _first_session(bucket_path: str, temp_dir: Path) -> ty.Iterator[ty.Any]:
    sessions = iterate_s3_sessions(bucket_path, CREDENTIALS, temp_dir, wait_period=0)
    assert next(sessions) == 1  # the iterator yields the session count first
    return sessions


def test_session_dir_is_removed_when_the_caller_stops_early(
    staged_bucket: str, tmp_path: Path
) -> None:
    """upload() can stop mid-iteration, e.g. on a transient error. The session
    download must still be removed."""
    sessions = _first_session(staged_bucket, tmp_path)
    listing = next(sessions)
    session_dir = tmp_path / "xnat-ingest-download" / SESSION
    assert listing.cache_path == session_dir
    assert session_dir.exists()

    sessions.close()

    assert not session_dir.exists()


def test_session_dir_is_removed_after_normal_iteration(
    staged_bucket: str, tmp_path: Path
) -> None:
    sessions = _first_session(staged_bucket, tmp_path)
    for _ in sessions:
        pass

    assert not (tmp_path / "xnat-ingest-download" / SESSION).exists()


class _Connection:
    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *exc: ty.Any) -> ty.Literal[False]:
        return False

    def clearcache(self) -> None:
        pass


class _Repo:
    connection = _Connection()


def test_upload_removes_the_session_download_when_it_fails(
    staged_bucket: str, tmp_path: Path
) -> None:
    """upload() must close the iterator itself. A caller that keeps the
    exception also keeps upload()'s frame, and so the open iterator, alive."""
    kept: ty.Optional[BaseException] = None
    with mock.patch.object(
        S3SessionListing, "all_uploaded", side_effect=RuntimeError("XNAT is down")
    ):
        try:
            upload(
                input_dir=staged_bucket,
                xnat_repo=_Repo(),  # type: ignore[arg-type]
                store_credentials=CREDENTIALS,
                s3_cache_dir=tmp_path,
                raise_errors=True,
                wait_period=0,
            )
        except RuntimeError as e:
            kept = e

    assert kept is not None
    assert not (tmp_path / "xnat-ingest-download" / SESSION).exists()
