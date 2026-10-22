"""The first nonempty XNAT listing determines the process checksum warning."""

import logging
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from xnat_ingest.helpers import remotes

WARNING = "XNAT returns no file checksums; files are compared by name only"


@pytest.fixture(autouse=True)
def reset_capability(monkeypatch):
    monkeypatch.setattr(remotes, "_xnat_has_checksums", None, raising=False)


def read_resource(digests, number=1):
    uri = f"/data/experiments/E/resources/{number}"
    rows = [
        {"Name": name, "URI": f"{uri}/files/{name}", "digest": digest}
        for name, digest in digests.items()
    ]
    response = Mock(status_code=200)
    response.json.return_value = {"ResultSet": {"Result": rows}}
    connection = Mock()
    connection.get.return_value = response
    return remotes.get_xnat_checksums(
        SimpleNamespace(uri=uri, id=str(number), xnat_session=connection)
    )


def messages(caplog):
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


def test_one_warning_across_resources_and_later_uploads(caplog):
    with caplog.at_level(logging.WARNING, logger="xnat-ingest"):
        for upload in range(3):
            for resource in range(4):
                assert read_resource({"file": ""}, upload * 4 + resource) == {
                    "file": ""
                }
    assert messages(caplog) == [WARNING]


@pytest.mark.parametrize("first", [{"a": "abc"}, {"a": "abc", "b": ""}])
def test_first_listing_with_digests_does_not_warn_later(caplog, first):
    with caplog.at_level(logging.WARNING, logger="xnat-ingest"):
        read_resource(first)
        read_resource({"new-file": ""}, 2)
    assert messages(caplog) == []


def test_empty_listing_does_not_decide_checksum_support(caplog):
    with caplog.at_level(logging.WARNING, logger="xnat-ingest"):
        read_resource({})
        assert messages(caplog) == []
        read_resource({"a": ""})
    assert messages(caplog) == [WARNING]


def test_concurrent_reads_still_warn_once(caplog):
    with caplog.at_level(logging.WARNING, logger="xnat-ingest"):
        with ThreadPoolExecutor(max_workers=4) as workers:
            list(workers.map(lambda n: read_resource({"a": ""}, n), range(12)))
    assert messages(caplog) == [WARNING]
