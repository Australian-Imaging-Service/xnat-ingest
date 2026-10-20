"""Escaping must not combine distinct routing labels."""

import re

import pytest

from xnat_ingest.model.session import ImagingSession


def assigned(value, field, injective=True):
    metadata = {"Project": "proj", "PatientID": "patient", "AccessionNumber": "visit"}
    metadata[field] = value
    session = ImagingSession(uid="group")
    session.metadata = metadata
    session.assign("Project", "PatientID", "AccessionNumber", injective_ids=injective)
    return session


@pytest.mark.parametrize(
    "field,attr",
    [
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


def test_project_and_default_labels_preserve_legacy_normalization():
    assert assigned("A/B", "Project").project_id == "A_B"
    assert assigned("A/B", "PatientID", injective=False).subject_id == "A_B"
    assert assigned("A B", "AccessionNumber", injective=False).session_id == "A_B"


@pytest.mark.parametrize(
    "args,env,expected",
    [
        ([], {}, False),
        (["--injective-ids"], {}, True),
        ([], {"XINGEST_INJECTIVE_IDS": "1"}, True),
    ],
)
def test_cli_plumbs_opt_in(tmp_path, args, env, expected):
    from unittest.mock import patch

    from click.testing import CliRunner

    from xnat_ingest.cli.assign_cli import assign_cmd

    with patch("xnat_ingest.cli.assign_cli.assign") as assign:
        result = CliRunner().invoke(
            assign_cmd, [str(tmp_path), str(tmp_path / "out"), *args], env=env
        )
    assert result.exit_code == 0, result.output
    assert assign.call_args.kwargs["injective_ids"] is expected
