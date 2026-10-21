"""Identity refusals have a stable alert signal and never mutate XNAT."""

from unittest import mock

import pytest
from xnat_ingest.api.tests.test_upload_cache_invalidation import FakeRepo

from xnat_ingest.api.upload_api import upload
from xnat_ingest.exceptions import SessionIdentityRefused
from xnat_ingest.helpers.remotes import LocalSessionListing


@pytest.mark.parametrize("raise_errors", [False, True])
@pytest.mark.parametrize("identity", [False, True])
def test_only_identity_refusal_emits_identity_event(
    tmp_path, caplog, raise_errors, identity
):
    (tmp_path / "proj.subj.sess").mkdir()
    events = []
    error = (SessionIdentityRefused if identity else ValueError)("synthetic refusal")
    with mock.patch.object(LocalSessionListing, "all_uploaded", side_effect=error):
        if raise_errors:
            with pytest.raises(type(error)):
                upload(
                    str(tmp_path), FakeRepo(events), wait_period=0, raise_errors=True
                )
        else:
            assert len(upload(str(tmp_path), FakeRepo(events), wait_period=0)) == 1
    alerts = [
        r for r in caplog.records if getattr(r, "event", "") == "xnat_identity_refused"
    ]
    assert len(alerts) == int(identity)
    if identity:
        assert alerts[0].session == "proj.subj.sess"
    assert events == []
