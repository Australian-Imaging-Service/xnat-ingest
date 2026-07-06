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
import typing as ty
from pathlib import Path
from unittest.mock import MagicMock, patch

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

    def find(self, body: ty.Dict[str, ty.Any]) -> ty.List[str]:
        assert body["Level"] == "Study"
        labels = body.get("Labels") or []
        if not labels:
            return sorted(self.studies)
        if body.get("LabelsConstraint") == "None":
            return sorted(
                sid for sid, s in self.studies.items() if not s["labels"] & set(labels)
            )
        return sorted(
            sid for sid, s in self.studies.items() if set(labels) <= s["labels"]
        )

    def get(self, path: str) -> ty.Any:
        parts = path.strip("/").split("/")
        if parts[0] == "studies" and len(parts) == 2:
            s = self.studies[parts[1]]
            return {
                "MainDicomTags": s["tags"],
                "PatientMainDicomTags": s["patient_tags"],
                "Series": s["series"],
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

    return patch.multiple(
        "xnat_ingest.model.session.requests",
        post=MagicMock(side_effect=fake_post),
        get=MagicMock(side_effect=fake_get),
        put=MagicMock(side_effect=fake_put),
    )


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
            ("study-done", "ACC-DONE", ["ready", "done"]),
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
    assert fake.labelled == []
