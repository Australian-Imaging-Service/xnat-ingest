"""S3SessionListing.cache_path must download the session exactly once.

`cache_path` is read four times by api/upload_api.py (lines 172, 181, 198, 216).
It was a plain property that performed the download as a side effect, so a
single upload re-fetched the entire session from S3 four times over.

Observed on a real 1.2 GB, 399-object session: the uploader logged
"Downloading session ... from S3 bucket" twice within one upload, one second
apart. At that size the redundant transfers dominate the upload window and the
egress cost; at the few-megabyte sizes used in testing they are invisible.
"""

import threading
import typing as ty
from pathlib import Path

from xnat_ingest.helpers.remotes import S3SessionListing


class FakeObject:
    """Stands in for a boto3 ObjectSummary."""

    def __init__(self, key: str, payload: bytes) -> None:
        self.key = key
        self.payload = payload


class RecordingBucket:
    """Counts how many times each object is fetched."""

    def __init__(self) -> None:
        self.download_count = 0
        self._lock = threading.Lock()
        self._payloads: dict[str, bytes] = {}

    def register(self, obj: FakeObject) -> FakeObject:
        self._payloads[obj.key] = obj.payload
        return obj

    def download_fileobj(self, key: str, fileobj: ty.BinaryIO) -> None:
        with self._lock:
            self.download_count += 1
        fileobj.write(self._payloads[key])


def _make_listing(
    tmp_path: Path, bucket: RecordingBucket, n_objects: int = 6
) -> tuple[S3SessionListing, dict[Path, bytes]]:
    objects = []
    expected: dict[Path, bytes] = {}
    for i in range(n_objects):
        relpath = [f"{i // 3 + 1}.scan_{i // 3 + 1}", "DICOM", f"slice{i}.dcm"]
        payload = bytes([i % 251]) * 4096
        obj = bucket.register(
            FakeObject(f"staged/session/{'/'.join(relpath)}", payload)
        )
        objects.append((relpath, obj))
        expected[tmp_path.joinpath(*relpath)] = payload
    listing = S3SessionListing(
        name="test_project.SUBJ.SESS",
        objects=objects,
        bucket=bucket,
        cache_path=tmp_path,
    )
    return listing, expected


def test_cache_path_downloads_each_object_once(tmp_path: Path) -> None:
    """Reading cache_path repeatedly must not re-fetch the session.

    upload_api reads it four times for a single upload. Re-downloading is not
    merely wasteful: it is what allows two downloads to race on one path.
    """
    bucket = RecordingBucket()
    listing, expected = _make_listing(tmp_path, bucket)

    for _ in range(4):  # mirrors the four reads in upload_api.py
        assert listing.cache_path == tmp_path

    assert bucket.download_count == len(expected), (
        f"expected {len(expected)} downloads for {len(expected)} objects, got "
        f"{bucket.download_count}: cache_path re-downloads on every read"
    )
