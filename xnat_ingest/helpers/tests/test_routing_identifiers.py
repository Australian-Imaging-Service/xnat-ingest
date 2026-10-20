"""Escaping must not combine distinct routing labels."""

import re

import pytest

from xnat_ingest.model.session import ImagingSession


def assigned(value, field):
    metadata = {"Project": "proj", "PatientID": "patient", "AccessionNumber": "visit"}
    metadata[field] = value
    session = ImagingSession(uid="group")
    session.metadata = metadata
    session.assign("Project", "PatientID", "AccessionNumber")
    return session


@pytest.mark.parametrize(
    "field,attr",
    [
        ("Project", "project_id"),
        ("PatientID", "subject_id"),
        ("AccessionNumber", "session_id"),
    ],
)
@pytest.mark.parametrize(
    "values",
    [("A/B", "A_B", "A B", "A..B"), ("é", "è", "_"), ("x/y", "XNAT_ESC_782f79")],
)
def test_distinct_routing_values_stay_distinct(field, attr, values):
    labels = [getattr(assigned(v, field), attr) for v in values]
    assert len(set(labels)) == len(values)
    assert all(re.fullmatch(r"[A-Za-z0-9_-]+", label) for label in labels)


def test_common_safe_ids_are_stable():
    for value in ["A_B", "sub-001", "1234"]:
        assert assigned(value, "PatientID").subject_id == value


def test_assignment_is_stable_across_new_process_objects():
    assert assigned("A/B", "PatientID").name == assigned("A/B", "PatientID").name


def test_distinct_values_never_get_the_same_staging_directory():
    assert assigned("A/B", "PatientID").name != assigned("A_B", "PatientID").name
