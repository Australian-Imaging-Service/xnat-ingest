"""Tests for ImagingSession.from_orthanc's study-selection logic: which
studies in Orthanc get staged is controlled by two labels --

* to_process_label: applied *externally* (e.g. by the modality/scanner
  integration) to mark a study as ready to be pulled. If None, every study
  is a candidate.
* processed_label: applied *by this script* after a study has been staged,
  so it isn't picked up again on the next run.

A study is staged only if it has the to_process_label (when one is given)
and does NOT already have the processed_label.

There's no Orthanc test server available (unlike xnat4tests for XNAT), so
the Orthanc REST API is faked in-memory: FakeOrthanc below implements just
enough of /tools/find, /studies, /series, /instances and study labels to
drive from_orthanc end to end, including the hardlink step (real DICOM
bytes are copied into the fake store_dir so checksums are genuine).
"""

import hashlib
import json
import typing as ty
import logging
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
from zipfile import ZIP_STORED, ZipFile

import pytest
from fileformats.application import Zip
from fileformats.core import FileSet
from fileformats.medimage import DicomSeries
from medimages4tests.dummy.dicom.pet.topogram.siemens.biograph_vision.vr20b import (
    get_image as get_topogram_image,  # type: ignore[import-untyped]
)

from xnat_ingest.model.session import ImagingSession

ORTHANC_URL = "http://orthanc-test:8042"
ORTHANC_USER = "orthanc-user"
ORTHANC_PASSWORD = "orthanc-pass"


class FakeOrthanc:
    """Minimal in-memory stand-in for the Orthanc REST endpoints that
    from_orthanc talks to."""

    def __init__(self, store_dir: Path) -> None:
        self.store_dir = store_dir
        self.studies: ty.Dict[str, ty.Dict[str, ty.Any]] = {}
        self.series: ty.Dict[str, ty.Dict[str, ty.Any]] = {}
        self.instances: ty.Dict[str, ty.Dict[str, ty.Any]] = {}
        self.labelled: ty.List[ty.Tuple[str, str]] = []
        self.unlabelled: ty.List[ty.Tuple[str, str]] = []

    def add_study(
        self,
        study_id: str,
        dcm_path: Path,
        study_tags: ty.Dict[str, str],
        patient_tags: ty.Dict[str, str],
        series_tags: ty.Dict[str, str],
        labels: ty.Iterable[str] = (),
    ) -> None:
        series_id = f"{study_id}-series1"
        instance_id = f"{study_id}-instance1"
        sop_uid = series_tags.pop("SOPInstanceUID", f"{study_id}-sop-uid")
        content = dcm_path.read_bytes()
        uuid = f"{instance_id}-uuid"
        dest = self.store_dir / uuid[0:2] / uuid[2:4] / uuid
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
        self.instances[instance_id] = {
            "sop_uid": sop_uid,
            "uuid": uuid,
            "md5": hashlib.md5(content).hexdigest(),
            "size": len(content),
        }
        self.series[series_id] = {"tags": series_tags, "instances": [instance_id]}
        self.studies[study_id] = {
            "tags": study_tags,
            "patient_tags": patient_tags,
            "series": [series_id],
            "labels": set(labels),
        }

    def find(self, body: ty.Dict[str, ty.Any]) -> ty.List[ty.Any]:
        assert body["Level"] == "Study"
        labels = body.get("Labels") or []
        if not labels:
            found = sorted(self.studies)
        elif body.get("LabelsConstraint") == "None":
            found = sorted(
                sid for sid, s in self.studies.items() if not s["labels"] & set(labels)
            )
        else:
            found = sorted(
                sid for sid, s in self.studies.items() if set(labels) <= s["labels"]
            )
        if body.get("Expand"):
            return [{"ID": sid, **self.get(f"/studies/{sid}")} for sid in found]
        return found

    def get(self, path: str) -> ty.Any:
        parts = path.strip("/").split("/")
        if parts[0] == "studies" and len(parts) == 2:
            s = self.studies[parts[1]]
            return {
                "MainDicomTags": s["tags"],
                "PatientMainDicomTags": s["patient_tags"],
                "Series": s["series"],
                "Labels": sorted(s["labels"]),
                "LastUpdate": s.get("last_update", "20000101T000000"),
            }
        if parts[0] == "series" and len(parts) == 3 and parts[2] == "instances":
            return [
                {
                    "ID": iid,
                    "MainDicomTags": {"SOPInstanceUID": self.instances[iid]["sop_uid"]},
                }
                for iid in self.series[parts[1]]["instances"]
            ]
        if parts[0] == "series" and len(parts) == 2:
            return {"MainDicomTags": self.series[parts[1]]["tags"]}
        if parts[0] == "instances" and path.endswith("/attachments/dicom/info"):
            inst = self.instances[parts[1]]
            return {
                "CompressedSize": inst["size"],
                "UncompressedSize": inst["size"],
                "Uuid": inst["uuid"],
                "UncompressedMD5": inst["md5"],
            }
        raise AssertionError(f"Unexpected Orthanc GET request: {path}")

    def label(self, study_id: str, label: str) -> None:
        self.studies[study_id]["labels"].add(label)
        self.labelled.append((study_id, label))

    def unlabel(self, study_id: str, label: str) -> None:
        self.studies[study_id]["labels"].discard(label)
        self.unlabelled.append((study_id, label))


def _mock_response(payload: ty.Any) -> MagicMock:
    response = MagicMock()
    response.json.return_value = payload
    response.raise_for_status = MagicMock()
    return response


def _patch_requests(fake: FakeOrthanc) -> ty.Any:
    def fake_post(url: str, auth: ty.Any = None, json: ty.Any = None) -> MagicMock:
        assert url == f"{ORTHANC_URL}/tools/find"
        return _mock_response(fake.find(json))

    def fake_get(url: str, auth: ty.Any = None) -> MagicMock:
        assert url.startswith(ORTHANC_URL)
        return _mock_response(fake.get(url[len(ORTHANC_URL) :]))

    def fake_put(url: str, auth: ty.Any = None) -> MagicMock:
        assert url.startswith(f"{ORTHANC_URL}/studies/")
        _, study_id, _, label = url[len(ORTHANC_URL) :].strip("/").split("/")
        fake.label(study_id, label)
        return _mock_response(None)

    def fake_delete(url: str, auth: ty.Any = None) -> MagicMock:
        assert url.startswith(f"{ORTHANC_URL}/studies/")
        _, study_id, _, label = url[len(ORTHANC_URL) :].strip("/").split("/")
        fake.unlabel(study_id, label)
        return _mock_response(None)

    return patch.multiple(
        "xnat_ingest.model.session.requests",
        post=MagicMock(side_effect=fake_post),
        get=MagicMock(side_effect=fake_get),
        put=MagicMock(side_effect=fake_put),
        delete=MagicMock(side_effect=fake_delete),
    )


def _processed(fake: FakeOrthanc) -> ty.List[ty.Tuple[str, str]]:
    """The (study, label) pairs that mark a study as processed, without the
    snapshot labels that are written beside them."""
    return [(sid, lb) for sid, lb in fake.labelled if not lb.startswith("done-")]


def _make_fake_orthanc(tmp_path: Path) -> ty.Tuple[FakeOrthanc, Path]:
    """Three studies, mirroring the states a real deployment sees:

    * study-ready: labelled with the external "ready" (to-process) label,
      not yet processed -- should be staged.
    * study-done: labelled "ready" AND already "done" (processed) --
      should be skipped even though it has the to-process label.
    * study-plain: has neither label -- only a candidate when no
      to_process_label filter is given.
    """
    store_dir = tmp_path / "store"
    store_dir.mkdir()
    fake = FakeOrthanc(store_dir)

    for i, (study_id, accession, labels) in enumerate(
        [
            ("study-ready", "ACC-READY", ["ready"]),
            # Processed by this version: the snapshot label names its LastUpdate.
            ("study-done", "ACC-DONE", ["ready", "done", "done-20000101T000000-0123456789abcdef01234567"]),
            ("study-plain", "ACC-PLAIN", []),
        ]
    ):
        dcm_dir = tmp_path / study_id
        study_uid = f"1.2.3.{i}"
        get_topogram_image(
            dcm_dir,
            StudyInstanceUID=study_uid,
            SeriesInstanceUID=f"{study_uid}.1",
            AccessionNumber=accession,
            PatientID="subject1",
        )
        dcm_path = next(dcm_dir.iterdir())
        fake.add_study(
            study_id,
            dcm_path,
            study_tags={
                "StudyInstanceUID": study_uid,
                "AccessionNumber": accession,
            },
            patient_tags={"PatientID": "subject1"},
            series_tags={"SeriesNumber": "1", "SeriesDescription": "Topogram"},
            labels=labels,
        )
    return fake, store_dir


def test_from_orthanc_stages_only_to_process_and_not_yet_processed(
    tmp_path: Path,
) -> None:
    fake, store_dir = _make_fake_orthanc(tmp_path)
    output_dir = tmp_path / "staged"

    with _patch_requests(fake):
        staged = ImagingSession.from_orthanc(
            url=ORTHANC_URL,
            output_dir=output_dir,
            store_dir=store_dir,
            user=ORTHANC_USER,
            password=ORTHANC_PASSWORD,
            to_process_label="ready",
            processed_label="done",
        )

    assert len(staged) == 1
    assert staged[0].metadata["AccessionNumber"] == "ACC-READY"
    # The staged study must now be labelled as processed, so it isn't
    # picked up again on the next run
    assert ("study-ready", "done") in fake.labelled
    # study-done was excluded even though it has the "ready" label, and
    # study-plain was excluded for lacking it -- neither should be touched
    assert not any(sid == "study-done" for sid, _ in fake.labelled)
    assert not any(sid == "study-plain" for sid, _ in fake.labelled)


def test_from_orthanc_processes_all_studies_when_to_process_label_is_none(
    tmp_path: Path,
) -> None:
    fake, store_dir = _make_fake_orthanc(tmp_path)
    output_dir = tmp_path / "staged"

    with _patch_requests(fake):
        staged = ImagingSession.from_orthanc(
            url=ORTHANC_URL,
            output_dir=output_dir,
            store_dir=store_dir,
            user=ORTHANC_USER,
            password=ORTHANC_PASSWORD,
            to_process_label=None,
            processed_label="done",
        )

    accessions = {s.metadata["AccessionNumber"] for s in staged}
    # study-done is already processed so it's excluded; both study-ready
    # and study-plain lack the "done" label and should be staged even
    # though only one of them has the "ready" label
    assert accessions == {"ACC-READY", "ACC-PLAIN"}


def test_from_orthanc_stages_nothing_when_no_study_has_the_label(
    tmp_path: Path,
) -> None:
    fake, store_dir = _make_fake_orthanc(tmp_path)
    output_dir = tmp_path / "staged"

    with _patch_requests(fake):
        staged = ImagingSession.from_orthanc(
            url=ORTHANC_URL,
            output_dir=output_dir,
            store_dir=store_dir,
            user=ORTHANC_USER,
            password=ORTHANC_PASSWORD,
            to_process_label="nonexistent-label",
            processed_label="done",
        )

    assert staged == []
    assert _processed(fake) == []


def test_from_orthanc_waits_for_recent_studies(tmp_path: Path) -> None:
    fake, store_dir = _make_fake_orthanc(tmp_path)
    fake.studies["study-ready"]["last_update"] = datetime.now(UTC).strftime(
        "%Y%m%dT%H%M%S"
    )

    with _patch_requests(fake):
        staged = ImagingSession.from_orthanc(
            url=ORTHANC_URL,
            output_dir=tmp_path / "staged",
            store_dir=store_dir,
            user=ORTHANC_USER,
            password=ORTHANC_PASSWORD,
            to_process_label="ready",
            processed_label="done",
            wait_period=300,
        )

    assert staged == []
    assert ("study-ready", "done") not in fake.labelled


def test_from_orthanc_does_not_label_study_when_staged_session_is_invalid(
    tmp_path: Path,
) -> None:
    fake, store_dir = _make_fake_orthanc(tmp_path)

    with (
        _patch_requests(fake),
        patch.object(
            ImagingSession, "load", side_effect=RuntimeError("invalid staged session")
        ),
        pytest.raises(RuntimeError, match="invalid staged session"),
    ):
        ImagingSession.from_orthanc(
            url=ORTHANC_URL,
            output_dir=tmp_path / "staged",
            store_dir=store_dir,
            user=ORTHANC_USER,
            **{"password": ORTHANC_PASSWORD},
            to_process_label="ready",
            processed_label="done",
        )

    assert ("study-ready", "done") not in fake.labelled


def test_from_orthanc_does_not_label_study_that_changes_during_staging(
    tmp_path: Path,
) -> None:
    fake, store_dir = _make_fake_orthanc(tmp_path)
    original_load = ImagingSession.load

    def load_then_update_study(session_dir: Path) -> ImagingSession:
        session = original_load(session_dir)
        fake.studies["study-ready"]["last_update"] = "20990101T000000"
        return session

    with (
        _patch_requests(fake),
        patch.object(ImagingSession, "load", side_effect=load_then_update_study),
        pytest.raises(RuntimeError, match="changed while it was being staged"),
    ):
        ImagingSession.from_orthanc(
            url=ORTHANC_URL,
            output_dir=tmp_path / "staged",
            store_dir=store_dir,
            user=ORTHANC_USER,
            **{"password": ORTHANC_PASSWORD},
            to_process_label="ready",
            processed_label="done",
        )

    assert ("study-ready", "done") not in fake.labelled


def test_from_orthanc_retry_keeps_existing_files_in_manifest(tmp_path: Path) -> None:
    fake, store_dir = _make_fake_orthanc(tmp_path)
    fake.studies = {"study-ready": fake.studies["study-ready"]}
    output_dir = tmp_path / "staged"

    for _ in range(2):
        with _patch_requests(fake):
            ImagingSession.from_orthanc(
                url=ORTHANC_URL,
                output_dir=output_dir,
                store_dir=store_dir,
                user=ORTHANC_USER,
                **{"password": ORTHANC_PASSWORD},
                to_process_label="ready",
                processed_label=None,
            )

    manifests = list(output_dir.rglob("__MANIFEST__.json"))
    assert len(manifests) == 1
    with open(manifests[0]) as f:
        manifest = json.load(f)
    assert len(manifest["checksums"]) == 1


def _fake_zip_converter(tmp_path: Path) -> ty.Callable[..., Zip]:
    """There is no DICOM-series converter that works without external tools
    (e.g. dcm2niix), so stand one in to check the conversion plumbing."""

    def convert(fileset: FileSet, **options: ty.Any) -> Zip:
        zip_path = tmp_path / "converted" / "series.zip"
        zip_path.parent.mkdir(exist_ok=True)
        with ZipFile(zip_path, "w", compression=options["compression"]) as zf:
            for fspath in fileset.fspaths:
                zf.write(fspath, fspath.name)
        return Zip(zip_path)

    return convert


def test_from_orthanc_converts_series_before_labelling(tmp_path: Path) -> None:
    fake, store_dir = _make_fake_orthanc(tmp_path)
    fake.studies = {"study-ready": fake.studies["study-ready"]}
    output_dir = tmp_path / "staged"

    with (
        _patch_requests(fake),
        patch.object(Zip, "convert", side_effect=_fake_zip_converter(tmp_path)),
    ):
        staged = ImagingSession.from_orthanc(
            url=ORTHANC_URL,
            output_dir=output_dir,
            store_dir=store_dir,
            user=ORTHANC_USER,
            **{"password": ORTHANC_PASSWORD},
            to_process_label="ready",
            processed_label="done",
            conversion_map={DicomSeries: (Zip, {"compression": ZIP_STORED})},
        )

    assert len(staged) == 1
    (resource,) = [r for s in staged[0].scans.values() for r in s.resources.values()]
    assert resource.name == "DICOM"
    assert isinstance(resource.fileset, Zip)
    with ZipFile(resource.fileset.fspath) as zf:
        assert zf.namelist() == ["study-ready-sop-uid.dcm"]
    assert not list(output_dir.glob("__convert_*"))
    assert ("study-ready", "done") in fake.labelled


def test_from_orthanc_does_not_label_study_when_conversion_fails(
    tmp_path: Path,
) -> None:
    fake, store_dir = _make_fake_orthanc(tmp_path)
    fake.studies = {"study-ready": fake.studies["study-ready"]}
    output_dir = tmp_path / "staged"

    with (
        _patch_requests(fake),
        patch.object(Zip, "convert", side_effect=RuntimeError("conversion failed")),
        pytest.raises(RuntimeError, match="conversion failed"),
    ):
        ImagingSession.from_orthanc(
            url=ORTHANC_URL,
            output_dir=output_dir,
            store_dir=store_dir,
            user=ORTHANC_USER,
            **{"password": ORTHANC_PASSWORD},
            to_process_label="ready",
            processed_label="done",
            conversion_map={DicomSeries: (Zip, {})},
        )

    assert ("study-ready", "done") not in fake.labelled
    assert not list(output_dir.glob("__convert_*"))


def _add_initial_series(fake, tmp_path):
    dcm_dir = tmp_path / "second-series"
    get_topogram_image(
        dcm_dir,
        StudyInstanceUID="1.2.3.0",
        SeriesInstanceUID="1.2.3.0.2",
        AccessionNumber="ACC-READY",
        PatientID="subject1",
    )
    fake.add_study(
        "second",
        next(dcm_dir.iterdir()),
        study_tags={"StudyInstanceUID": "1.2.3.0"},
        patient_tags={},
        series_tags={"SeriesNumber": "2", "SeriesDescription": "Second"},
    )
    fake.studies["study-ready"]["series"].extend(fake.studies.pop("second")["series"])


def _publication_kwargs(output_dir, store_dir):
    return dict(
        url=ORTHANC_URL,
        output_dir=output_dir,
        store_dir=store_dir,
        user=ORTHANC_USER,
        password=ORTHANC_PASSWORD,
        to_process_label="ready",
        processed_label="done",
    )


def test_initial_build_is_hidden_from_assign(tmp_path):
    from xnat_ingest.api.assign_api import assign
    from xnat_ingest.helpers.remotes import list_session_dirs

    fake, store = _make_fake_orthanc(tmp_path)
    _add_initial_series(fake, tmp_path)
    output = tmp_path / "staged"
    original_get = fake.get
    observations = []

    def get(path):
        if path.startswith("/instances/"):
            assert list(list_session_dirs(output)) == []
            assert (
                assign(
                    output,
                    tmp_path / "assigned",
                    "StudyID",
                    "PatientID",
                    "AccessionNumber",
                    unlink_source="all",
                    raise_errors=True,
                )
                == []
            )
            observations.append(path)
        return original_get(path)

    fake.get = get
    with _patch_requests(fake):
        staged = ImagingSession.from_orthanc(**_publication_kwargs(output, store))
    assert len(observations) == 2 and len(staged) == 1
    assert len(staged[0].scans) == 2
    for resource in staged[0].resources:
        assert all(
            path.exists() and "__build__" not in path.parts
            for path in resource.fileset.fspaths
        )
        resource.check_checksums()
    assert _processed(fake) == [("study-ready", "done")]


def _pause_complete_build(fake, kwargs, writer, label_marker):
    import threading

    def pause(path):
        writer.send(str(path))
        threading.Event().wait(30)
        raise RuntimeError("test must terminate the paused producer")

    fake.label = lambda *args: label_marker.write_text("label applied")
    with _patch_requests(fake), patch.object(ImagingSession, "load", side_effect=pause):
        ImagingSession.from_orthanc(**kwargs)


def test_stopped_initial_build_is_hidden_and_retry_publishes_all_series(tmp_path):
    import multiprocessing

    from xnat_ingest.helpers.remotes import list_session_dirs

    fake, store = _make_fake_orthanc(tmp_path)
    _add_initial_series(fake, tmp_path)
    output, marker = tmp_path / "staged", tmp_path / "label-marker"
    kwargs = _publication_kwargs(output, store)
    context = multiprocessing.get_context("spawn")
    reader, writer = context.Pipe(duplex=False)
    child = context.Process(
        target=_pause_complete_build, args=(fake, kwargs, writer, marker)
    )
    child.start()
    try:
        assert reader.poll(30), "producer did not reach the complete-build boundary"
        build = Path(reader.recv())
        child.kill()
        child.join(10)
        assert child.exitcode is not None and child.exitcode != 0
        assert list(list_session_dirs(output)) == [] and not marker.exists()
        assert len(list(build.rglob("*.dcm"))) == 2
        with _patch_requests(fake):
            staged = ImagingSession.from_orthanc(**kwargs)
        assert len(staged) == 1 and len(staged[0].scans) == 2
        assert not build.exists() and len(list(output.glob("_.*"))) == 1
        for resource in staged[0].resources:
            resource.check_checksums()
        assert _processed(fake) == [("study-ready", "done")]
    finally:
        if child.is_alive():
            child.kill()
            child.join(10)
        reader.close()
        writer.close()


def test_existing_group_destination_is_preserved_and_next_study_runs(tmp_path, caplog):
    fake, store = _make_fake_orthanc(tmp_path)
    _add_initial_series(fake, tmp_path)
    output = tmp_path / "staged"
    refused = output / "_.1_2_3_2"
    refused.mkdir(parents=True)
    sentinel = refused / "preserve.txt"
    sentinel.write_bytes(b"existing generation")
    kwargs = _publication_kwargs(output, store)
    kwargs["to_process_label"] = None
    with _patch_requests(fake):
        staged = ImagingSession.from_orthanc(**kwargs)
    assert [session.metadata["AccessionNumber"] for session in staged] == ["ACC-READY"]
    assert len(staged[0].scans) == 2
    assert list(refused.iterdir()) == [sentinel]
    assert sentinel.read_bytes() == b"existing generation"
    assert _processed(fake) == [("study-ready", "done")]
    assert "study-plain" in caplog.text and str(refused) in caplog.text


def _snapshot_kwargs(output: Path, store: Path) -> ty.Dict[str, ty.Any]:
    kwargs = _publication_kwargs(output, store)
    kwargs.update(to_process_label="ready", processed_label="done")
    return kwargs


def test_staging_writes_the_snapshot_label(tmp_path):
    fake, store = _make_fake_orthanc(tmp_path)
    fake.studies["study-ready"]["last_update"] = "20261019T011324"
    with _patch_requests(fake):
        staged = ImagingSession.from_orthanc(**_snapshot_kwargs(tmp_path / "staged", store))
    assert len(staged) == 1
    # Snapshot first, processed last: a processed study always has a snapshot.
    # The snapshot names the LastUpdate and a digest of the staged instance ids.
    import hashlib
    ids = sorted(fake.series[s]["instances"][0] for s in fake.studies["study-ready"]["series"])
    digest = hashlib.sha256(("\n".join(ids) + "\n").encode()).hexdigest()[:24]
    assert fake.labelled[-2:] == [
        ("study-ready", f"done-20261019T011324-{digest}"),
        ("study-ready", "done"),
    ]


def test_processed_study_with_matching_snapshot_is_left_alone(tmp_path):
    fake, store = _make_fake_orthanc(tmp_path)
    done = fake.studies["study-done"]
    done["labels"] = {"ready", "done", "done-20261019T011324-0123456789abcdef01234567"}
    done["last_update"] = "20261019T011324"
    with _patch_requests(fake):
        staged = ImagingSession.from_orthanc(**_snapshot_kwargs(tmp_path / "staged", store))
    assert [s.name for s in staged] != ["study-done"]
    assert not any(sid == "study-done" for sid, _ in fake.labelled)
    assert fake.unlabelled == []


def test_processed_study_changed_after_staging_is_staged_again(tmp_path):
    fake, store = _make_fake_orthanc(tmp_path)
    done = fake.studies["study-done"]
    done["labels"] = {"ready", "done", "done-20261019T011324-0123456789abcdef01234567"}
    done["last_update"] = "20261019T014500"  # a series arrived after the staging
    with _patch_requests(fake):
        staged = ImagingSession.from_orthanc(**_snapshot_kwargs(tmp_path / "staged", store))
    assert any(sid == "study-done" for sid, _ in fake.labelled)
    assert ("study-done", "done") in fake.unlabelled
    assert ("study-done", "done-20261019T011324-0123456789abcdef01234567") in fake.unlabelled
    mine = [lb for sid, lb in fake.labelled if sid == "study-done"]
    assert mine[0].startswith("done-20261019T014500-") and mine[1] == "done"
    assert "done" in done["labels"] and len([lb for lb in done["labels"] if lb.startswith("done-")]) == 1
    assert "done-20261019T011324-0123456789abcdef01234567" not in done["labels"]
    assert len(staged) == 2


def test_processed_study_without_snapshot_is_staged_again(tmp_path):
    # Labelled by a version that wrote no snapshot label: export state unknown.
    fake, store = _make_fake_orthanc(tmp_path)
    fake.studies["study-done"]["labels"] = {"ready", "done"}
    fake.studies["study-done"]["last_update"] = "20261019T011324"
    with _patch_requests(fake):
        staged = ImagingSession.from_orthanc(**_snapshot_kwargs(tmp_path / "staged", store))
    assert ("study-done", "done") in fake.unlabelled
    assert any(sid == "study-done" and lb.startswith("done-20261019T011324-") for sid, lb in fake.labelled)
    assert len(staged) == 2


def test_changed_processed_study_without_ready_label_is_not_staged(tmp_path):
    # Under an engine that gates on the ready label, a changed study without it
    # waits for the hook: nothing identifiable may leave Orthanc early.
    fake, store = _make_fake_orthanc(tmp_path)
    done = fake.studies["study-done"]
    done["labels"] = {"done", "done-20261019T011324-0123456789abcdef01234567"}
    done["last_update"] = "20261019T014500"
    with _patch_requests(fake):
        staged = ImagingSession.from_orthanc(**_snapshot_kwargs(tmp_path / "staged", store))
    assert not any(sid == "study-done" for sid, _ in fake.labelled + fake.unlabelled)
    assert len(staged) == 1


def test_processed_study_with_a_malformed_snapshot_is_staged_again(tmp_path):
    # A timestamp without a digest is not a receipt this version wrote.
    fake, store = _make_fake_orthanc(tmp_path)
    done = fake.studies["study-done"]
    done["labels"] = {"ready", "done", "done-20261019T011324"}
    done["last_update"] = "20261019T011324"
    with _patch_requests(fake):
        staged = ImagingSession.from_orthanc(**_snapshot_kwargs(tmp_path / "staged", store))
    assert ("study-done", "done-20261019T011324") in fake.unlabelled
    assert len(staged) == 2


def test_processed_study_with_two_snapshots_is_staged_again(tmp_path):
    fake, store = _make_fake_orthanc(tmp_path)
    done = fake.studies["study-done"]
    done["labels"] = {"ready", "done", "done-20261019T011324-0123456789abcdef01234567",
                      "done-20261019T011300-0123456789abcdef01234567"}
    done["last_update"] = "20261019T011324"
    with _patch_requests(fake):
        staged = ImagingSession.from_orthanc(**_snapshot_kwargs(tmp_path / "staged", store))
    assert len(staged) == 2
    assert len([lb for lb in done["labels"] if lb.startswith("done-")]) == 1


def test_malformed_expanded_find_reply_is_an_error(tmp_path):
    fake, store = _make_fake_orthanc(tmp_path)
    real_find = fake.find

    def bad_find(body):
        return {} if body.get("Expand") else real_find(body)

    fake.find = bad_find
    with _patch_requests(fake), pytest.raises(ValueError, match="expanded study query"):
        ImagingSession.from_orthanc(**_snapshot_kwargs(tmp_path / "staged", store))
    assert fake.labelled == [] and fake.unlabelled == []


def test_processed_label_forces_a_one_second_wait(tmp_path, caplog):
    # Without a wait a late instance can share the staged LastUpdate's second.
    fake, store = _make_fake_orthanc(tmp_path)
    fake.studies["study-ready"]["last_update"] = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    kwargs = _snapshot_kwargs(tmp_path / "staged", store)
    kwargs["wait_period"] = 0
    with _patch_requests(fake), caplog.at_level(logging.INFO):
        staged = ImagingSession.from_orthanc(**kwargs)
    assert [s.name for s in staged] == []
    assert "wait period of 1 second" in caplog.text


def test_too_long_processed_label_is_refused_before_staging(tmp_path):
    fake, store = _make_fake_orthanc(tmp_path)
    kwargs = _snapshot_kwargs(tmp_path / "staged", store)
    kwargs["processed_label"] = "p" * 45
    with _patch_requests(fake), pytest.raises(ValueError, match="too long"):
        ImagingSession.from_orthanc(**kwargs)
    assert fake.labelled == []


@pytest.mark.parametrize(
    "field, value",
    [("Labels", "ready"), ("Labels", [1, 2]), ("LastUpdate", None), ("LastUpdate", "yesterday")],
)
def test_malformed_study_fields_in_the_expanded_reply_are_an_error(tmp_path, field, value):
    fake, store = _make_fake_orthanc(tmp_path)
    real_find = fake.find

    def bad_find(body):
        result = real_find(body)
        if body.get("Expand"):
            for entry in result:
                entry[field] = value
        return result

    fake.find = bad_find
    with _patch_requests(fake), pytest.raises(ValueError, match="Unexpected Labels or LastUpdate"):
        ImagingSession.from_orthanc(**_snapshot_kwargs(tmp_path / "staged", store))
    assert fake.labelled == [] and fake.unlabelled == []
