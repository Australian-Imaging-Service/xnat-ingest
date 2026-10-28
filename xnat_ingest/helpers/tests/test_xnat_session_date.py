import datetime
import types
import typing as ty
from unittest.mock import ANY, Mock

import pytest

from xnat_ingest.helpers.remotes import get_xnat_session
from xnat_ingest.model.session import ImagingSession


class _CreatedObject:
    calls: list[dict[str, ty.Any]] = []

    def __init__(self, **kwargs: ty.Any) -> None:
        type(self).calls.append(kwargs)


class _SubjectData(_CreatedObject):
    calls: list[dict[str, ty.Any]] = []


class _MrSessionData(_CreatedObject):
    calls: list[dict[str, ty.Any]] = []


class _Classes:
    SubjectData = _SubjectData
    MrSessionData = _MrSessionData
    PetSessionData = _CreatedObject
    CtSessionData = _CreatedObject


class _Project:
    def __init__(self, experiments: dict[str, ty.Any] | None = None) -> None:
        self.xnat_session = types.SimpleNamespace(classes=_Classes)
        self.experiments = experiments or {}


@pytest.fixture(autouse=True)
def _clear_calls() -> None:
    _CreatedObject.calls.clear()
    _SubjectData.calls.clear()
    _MrSessionData.calls.clear()


def _session(date_value: ty.Any = "20260805") -> ImagingSession:
    session = ImagingSession(
        uid="uid",
        project_id="PROJECT",
        subject_id="SUBJECT",
        session_id="SESSION",
    )
    session.metadata["Modality"] = "MR"
    session.metadata["CaptureDate"] = date_value
    return session


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("20260805", "2026-08-05"),
        ("2026-08-05T13:45:00", "2026-08-05"),
        (datetime.date(2026, 8, 5), "2026-08-05"),
        (["20260805", None, ["2026-08-05T13:45:00"]], "2026-08-05"),
    ],
)
def test_new_session_date_is_read_from_metadata(value: ty.Any, expected: str) -> None:
    get_xnat_session(_session(value), _Project(), date_field="CaptureDate")

    assert _MrSessionData.calls == [
        {
            "label": "SESSION",
            "parent": ANY,
            "date": expected,
        }
    ]


def test_multiple_session_dates_are_rejected() -> None:
    with pytest.raises(
        ValueError,
        match=r"field 'CaptureDate' resolves to multiple dates: "
        r"\['2026-08-05', '2026-08-06'\]",
    ):
        get_xnat_session(
            _session(["20260805", "20260806"]),
            _Project(),
            date_field="CaptureDate",
        )


def test_missing_session_date_field_is_rejected() -> None:
    session = _session()
    del session.metadata._dct["CaptureDate"]

    with pytest.raises(ValueError, match="metadata field 'CaptureDate' was not found"):
        get_xnat_session(session, _Project(), date_field="CaptureDate")
    assert _SubjectData.calls == []


def test_existing_session_date_is_not_changed() -> None:
    existing = types.SimpleNamespace(uri="/data/experiments/E1")
    session = _session("not-a-date")
    project = _Project({"SESSION": existing})
    project.id = "PROJECT"
    project.xnat_session.get_json = Mock(
        side_effect=[
            {"items": [{"data_fields": {"ID": "E1", "subject_ID": "S1"}}]},
            {"ResultSet": {"Result": [{"ID": "S1", "label": "SUBJECT"}]}},
        ]
    )

    assert (
        get_xnat_session(
            session,
            project,
            date_field="CaptureDate",
        )
        is existing
    )
    assert _MrSessionData.calls == []
    assert _SubjectData.calls == []


def test_session_date_with_explicit_modality() -> None:
    session = _session()
    del session.metadata._dct["Modality"]

    get_xnat_session(session, _Project(), modality="MR", date_field="CaptureDate")

    assert _MrSessionData.calls == [
        {"label": "SESSION", "parent": ANY, "date": "2026-08-05"}
    ]
