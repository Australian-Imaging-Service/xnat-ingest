"""The explicit `modality` passed to upload selects the XNAT session class."""

from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from xnat_ingest.api import upload_api
from xnat_ingest.exceptions import UnsupportedModalityError
from xnat_ingest.helpers.remotes import get_xnat_session, xnat_session_type
from xnat_ingest.model.session import ImagingSession


class _XnatClass:
    def __init__(self, label, parent):
        self.label = label
        self.parent = parent


def _xnat_class(name):
    return type(name, (_XnatClass,), {})


def _xproject(experiments=None):
    """A project whose server knows the session classes XNAT ships with."""
    classes = SimpleNamespace(
        **{
            name: _xnat_class(name)
            for name in (
                "SubjectData",
                "MrSessionData",
                "PetSessionData",
                "CtSessionData",
                "XaSessionData",
                "OptSessionData",
                "OtherDicomSessionData",
            )
        }
    )
    return SimpleNamespace(
        xnat_session=SimpleNamespace(classes=classes),
        experiments=dict(experiments or {}),
    )


def _session(modality=None):
    session = ImagingSession(
        uid="uid", project_id="proj", subject_id="subj", session_id="sess"
    )
    session.metadata = {} if modality is None else {"Modality": modality}
    return session


@pytest.mark.parametrize(
    "modality,expected",
    [
        ("MR", "MrSessionData"),
        ("PT", "PetSessionData"),
        ("CT", "CtSessionData"),
        ("mr", "MrSessionData"),
        ("OPT", "OptSessionData"),
    ],
)
def test_explicit_modality_without_modality_metadata(modality, expected):
    xsession = get_xnat_session(_session(), _xproject(), modality=modality)
    assert type(xsession).__name__ == expected
    assert xsession.label == "sess"
    assert xsession.parent.label == "subj"


def test_explicit_modality_overrides_modality_metadata():
    xsession = get_xnat_session(_session("MR"), _xproject(), modality="CT")
    assert type(xsession).__name__ == "CtSessionData"


@pytest.mark.parametrize(
    "metadata,expected",
    [
        ("MR", "MrSessionData"),
        ("PT", "PetSessionData"),
        (["CT", "PT"], "PetSessionData"),
        (["SR", "MR"], "MrSessionData"),
        ("XA", "XaSessionData"),
        (["XA", ["OPT", "XA"]], "OptSessionData"),
        (["SR", "XA"], "XaSessionData"),
        (["KO", "PR", "SEG", "CT"], "CtSessionData"),
        (["RTIMAGE", "XA"], "XaSessionData"),
        ("US", "OtherDicomSessionData"),
        (["SR"], "OtherDicomSessionData"),
    ],
)
def test_modality_falls_back_to_metadata(metadata, expected):
    xsession = get_xnat_session(_session(metadata), _xproject())
    assert type(xsession).__name__ == expected


@pytest.mark.parametrize(
    "modality,expected",
    [("MR", "mrSessionData"), ("PT", "petSessionData"), ("opt", "optSessionData")],
)
def test_xnat_session_type(modality, expected):
    assert xnat_session_type(modality) == expected


def test_modality_without_xnat_session_type_raises():
    with pytest.raises(UnsupportedModalityError, match="xnat:usSessionData"):
        get_xnat_session(_session(), _xproject(), modality="US")


def test_no_other_dicom_session_type_raises():
    xproject = _xproject()
    del xproject.xnat_session.classes.OtherDicomSessionData
    with pytest.raises(UnsupportedModalityError, match="otherDicomSessionData"):
        get_xnat_session(_session("US"), xproject)


def test_missing_modality_raises():
    with pytest.raises(RuntimeError, match="'modality' parameter"):
        get_xnat_session(_session(), _xproject())


def test_existing_experiment_ignores_modality():
    existing = SimpleNamespace(uri="/data/experiments/E1")
    xproject = _xproject({"sess": existing})
    xproject.id = "proj"
    xproject.xnat_session.get = mock.Mock(
        side_effect=[
            SimpleNamespace(json=lambda data=data: data)
            for data in [
                {"items": [{"data_fields": {"subject_ID": "S1"}}]},
                {"ResultSet": {"Result": [{"ID": "S1", "label": "subj"}]}},
            ]
        ]
    )
    xsession = get_xnat_session(_session(), xproject, modality="CT")
    assert xsession is existing


def test_upload_passes_modality_to_get_xnat_session(tmp_path: Path):
    staging = tmp_path / "staged"
    (staging / "proj.subj.sess").mkdir(parents=True)
    session = _session()
    connection = mock.MagicMock()
    connection.projects = {"proj": SimpleNamespace(experiments={})}
    connection.get_json.return_value = {
        "ResultSet": {"Result": [{"ID": "proj", "name": "proj"}]}
    }
    repo = SimpleNamespace(server="fake://xnat", connection=connection)
    with (
        mock.patch.object(upload_api.ImagingSession, "load", return_value=session),
        mock.patch.object(upload_api.FrameSet, "load", side_effect=KeyError("none")),
        mock.patch.object(
            upload_api, "get_xnat_session", return_value=SimpleNamespace(id="E1")
        ) as get_session,
    ):
        errors = upload_api.upload(
            str(staging),
            repo,
            always_include=["all"],
            xnat_max_workers=1,
            raise_errors=True,
            modality="CT",
        )
    assert errors == []
    get_session.assert_called_once()
    assert get_session.call_args.kwargs["modality"] == "CT"
