from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from fileformats.generic import File
from fileformats.medimage import DicomImage, DicomSeries
from fileformats.testing import MyFormat
from pydicom.dataset import FileDataset, FileMetaDataset
from pydicom.uid import ExplicitVRLittleEndian, generate_uid

from xnat_ingest.api.package_api import _package_session, package
from xnat_ingest.cli.package_cli import package_cmd
from xnat_ingest.model.scan import ImagingScan
from xnat_ingest.model.session import ImagingSession


def _dicom(path: Path, sop_class_uid: str, study_uid: str) -> DicomImage:
    meta = FileMetaDataset()
    meta.MediaStorageSOPClassUID = sop_class_uid
    meta.MediaStorageSOPInstanceUID = generate_uid()
    meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds = FileDataset(str(path), {}, file_meta=meta, preamble=b"\0" * 128)
    ds.SOPClassUID = sop_class_uid
    ds.SOPInstanceUID = meta.MediaStorageSOPInstanceUID
    ds.StudyInstanceUID = study_uid
    ds.SeriesInstanceUID = generate_uid()
    ds.Modality = "MR"
    ds.PatientID = "test"
    ds.is_little_endian = True
    ds.is_implicit_VR = False
    ds.save_as(str(path))
    return DicomImage(path)


@pytest.mark.parametrize(
    ("sop_class_uid", "expected_label"),
    [
        ("1.2.840.10008.5.1.4.1.1.4", "DICOM"),
        ("1.2.840.10008.5.1.4.1.1.7", "secondary"),
    ],
)
def test_package_auto_labels_sample_from_sop_class(
    tmp_path: Path, sop_class_uid: str, expected_label: str
) -> None:
    image = _dicom(tmp_path / "image.dcm", sop_class_uid, "1.2.3")
    session = ImagingSession(
        uid="uid",
        project_id="PROJECT",
        subject_id="SUBJECT",
        session_id="SESSION",
        scans=[
            ImagingScan(
                id="1", type="MR", resources={"SOURCE": DicomSeries([image.fspath])}
            )
        ],
    )
    with patch.object(DicomImage, "convert", return_value=image):
        packaged = _package_session(
            session,
            DicomSeries,
            {"auto": (DicomImage, {})},
            on_resource_clash="error",
        )

    assert set(packaged.scans["1"].resources) == {expected_label}


def test_package_rejects_samples_from_different_studies(tmp_path: Path) -> None:
    first = _dicom(tmp_path / "first.dcm", "1.2.840.10008.5.1.4.1.1.4", "1.2.3")
    second = _dicom(tmp_path / "second.dcm", "1.2.840.10008.5.1.4.1.1.4", "1.2.4")
    session = ImagingSession(
        uid="uid",
        project_id="PROJECT",
        subject_id="SUBJECT",
        session_id="SESSION",
        scans=[
            ImagingScan(
                id="1", type="MR", resources={"SOURCE": DicomSeries([first.fspath])}
            ),
            ImagingScan(
                id="2", type="MR", resources={"SOURCE": DicomSeries([second.fspath])}
            ),
        ],
    )
    with patch.object(DicomImage, "convert", side_effect=[first, second]):
        with pytest.raises(ValueError, match="multiple StudyInstanceUIDs"):
            _package_session(
                session,
                DicomSeries,
                {"auto": (DicomImage, {})},
                on_resource_clash="error",
            )


def test_package_rejects_mixed_studies_inside_one_series(tmp_path: Path) -> None:
    first = _dicom(tmp_path / "first.dcm", "1.2.840.10008.5.1.4.1.1.4", "1.2.3")
    second = _dicom(tmp_path / "second.dcm", "1.2.840.10008.5.1.4.1.1.4", "1.2.4")
    session = ImagingSession(
        uid="uid",
        project_id="PROJECT",
        subject_id="SUBJECT",
        session_id="SESSION",
        scans=[
            ImagingScan(
                id="1",
                type="MR",
                resources={"SOURCE": DicomSeries([first.fspath, second.fspath])},
            )
        ],
    )
    with patch.object(DicomImage, "convert", return_value=first) as converter:
        with pytest.raises(ValueError, match="exactly one StudyInstanceUID"):
            _package_session(
                session,
                DicomSeries,
                {"auto": (DicomImage, {})},
                on_resource_clash="error",
            )

    converter.assert_not_called()


def test_package_rejects_primary_sample_as_secondary(tmp_path: Path) -> None:
    image = _dicom(tmp_path / "image.dcm", "1.2.840.10008.5.1.4.1.1.4", "1.2.3")
    session = ImagingSession(
        uid="uid",
        project_id="PROJECT",
        subject_id="SUBJECT",
        session_id="SESSION",
        scans=[
            ImagingScan(
                id="1", type="MR", resources={"SOURCE": DicomSeries([image.fspath])}
            )
        ],
    )
    with patch.object(DicomImage, "convert", return_value=image):
        with pytest.raises(ValueError, match="must use XNAT resource label 'DICOM'"):
            _package_session(
                session,
                DicomSeries,
                {"secondary": (DicomImage, {})},
                on_resource_clash="error",
            )


def _staged_session(input_dir: Path) -> tuple[ImagingSession, Path]:
    source_path = input_dir.parent / "source.my"
    source_path.write_text("deidentified source")
    report_path = input_dir.parent / "report.txt"
    report_path.write_text("session report")

    session = ImagingSession(
        uid="uid",
        project_id="PROJECT",
        subject_id="SUBJECT",
        session_id="SESSION",
        scans=[
            ImagingScan(
                id="1",
                type="CT",
                resources={"SOURCE": MyFormat(source_path)},
            )
        ],
    )
    session.metadata["session-field"] = "preserved"
    session.scans["1"].metadata["scan-field"] = "preserved"
    session.scans["1"].resources["SOURCE"].metadata["resource-field"] = "preserved"
    session.add_session_resource("REPORT", File(report_path))
    _, session_dir = session.save(input_dir)
    return session, session_dir


def test_package_replaces_one_source_with_multiple_named_outputs(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    _session, source_dir = _staged_session(input_dir)

    errors = package(
        input_dir=input_dir,
        output_dir=output_dir,
        source_datatype=MyFormat,
        output_resources={
            "DICOM": (MyFormat, {}),
            "secondary": (MyFormat, {}),
        },
    )

    assert errors == []
    assert source_dir.exists(), "packaging removed its input without being asked"
    packaged = ImagingSession.load(output_dir / "PROJECT.SUBJECT.SESSION")
    scan = packaged.scans["1"]
    assert set(scan.resources) == {"DICOM", "secondary"}
    assert all(
        resource.fileset.fspath.read_text() == "deidentified source"
        for resource in scan.resources.values()
    )
    assert packaged.metadata["session-field"] == "preserved"
    assert scan.metadata["scan-field"] == "preserved"
    assert scan.resources["DICOM"].metadata["resource-field"] == "preserved"
    assert packaged.session_resources["REPORT"].fileset.fspath.read_text() == (
        "session report"
    )


def test_package_does_not_finalize_output_when_a_converter_fails(
    tmp_path: Path,
) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    _session, source_dir = _staged_session(input_dir)

    with patch.object(
        MyFormat, "convert", side_effect=RuntimeError("conversion failed")
    ):
        errors = package(
            input_dir=input_dir,
            output_dir=output_dir,
            source_datatype=MyFormat,
            output_resources={"DICOM": (MyFormat, {})},
        )

    assert len(errors) == 1
    assert "conversion failed" in errors[0]
    assert not (output_dir / "PROJECT.SUBJECT.SESSION").exists()
    assert source_dir.exists()


def test_package_unlinks_source_only_after_success(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    _session, source_dir = _staged_session(input_dir)

    errors = package(
        input_dir=input_dir,
        output_dir=output_dir,
        source_datatype=MyFormat,
        output_resources={"DICOM": (MyFormat, {})},
        unlink_source="all",
    )

    assert errors == []
    assert not source_dir.exists()
    assert (output_dir / "PROJECT.SUBJECT.SESSION").exists()


def test_package_rejects_old_layout_in_existing_output(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    _staged_session(input_dir)
    assert (
        package(
            input_dir,
            output_dir,
            MyFormat,
            {"DICOM": (MyFormat, {}), "secondary": (MyFormat, {})},
        )
        == []
    )

    errors = package(
        input_dir,
        output_dir,
        MyFormat,
        {"DICOM-zip": (MyFormat, {}), "DICOM": (MyFormat, {})},
    )

    assert len(errors) == 1
    assert "fresh output directory" in errors[0]
    old_session = ImagingSession.load(output_dir / "PROJECT.SUBJECT.SESSION")
    assert set(old_session.scans["1"].resources) == {"DICOM", "secondary"}


def test_package_cli_accepts_multiple_named_outputs(tmp_path: Path) -> None:
    input_dir = tmp_path / "input"
    output_dir = tmp_path / "output"
    input_dir.mkdir()
    _staged_session(input_dir)

    result = CliRunner().invoke(
        package_cmd,
        [
            str(input_dir),
            str(output_dir),
            MyFormat.mime_like,
            "--output-resource",
            "DICOM",
            MyFormat.mime_like,
            "--output-resource",
            "secondary",
            MyFormat.mime_like,
        ],
    )

    assert result.exit_code == 0, result.output
    packaged = ImagingSession.load(output_dir / "PROJECT.SUBJECT.SESSION")
    assert set(packaged.scans["1"].resources) == {"DICOM", "secondary"}
