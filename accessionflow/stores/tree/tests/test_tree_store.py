import hashlib
import json
import os
import uuid
from pathlib import Path

import fsspec
import pytest
from click.testing import CliRunner
from fileformats.core import FileSet
from fileformats.generic import File

from accessionflow.cli import cli
from accessionflow.helpers.metadata import Metadata
from accessionflow.model.accession import Accession
from accessionflow.model.acquisition import Acquisition
from accessionflow.model.resource import Resource
from accessionflow.stores.tree.api import check_store, store

CLEAN_ENV = {key: None for key in os.environ if key.startswith("ACFLOW_")}

LEVELS = {"site": "SiteID", "subject": "PatientID", "visit": "Visit"}
LABELS = ("SYD", "P01", "V1")


def _staged(
    tmp_path: Path,
    levels: dict[str, str] = LEVELS,
    store_type: str = "tree",
) -> Path:
    """A staging directory holding one labelled accession, with two acquisitions (one
    with two resources), a session resource and fields"""
    src = tmp_path / "src"
    src.mkdir()
    files = {}
    for name in ("t1.nii", "t1.json", "bold.nii", "notes.txt"):
        files[name] = src / name
        files[name].write_text(f"contents of {name}")
    accession = Accession(
        uid="1.2.3",
        scans=[
            Acquisition(
                "1",
                "T1w",
                resources={
                    "NIFTI": Resource("NIFTI", File(files["t1.nii"])),
                    "JSON": Resource("JSON", File(files["t1.json"])),
                },
            ),
            Acquisition(
                "2",
                "bold",
                resources={"NIFTI": Resource("NIFTI", File(files["bold.nii"]))},
            ),
        ],
        session_resources={"NOTES": Resource("NOTES", File(files["notes.txt"]))},
    )
    for key, value in {
        "SiteID": "SYD",
        "PatientID": "P01",
        "Visit": "V1",
        "Extra": "X",
    }.items():
        accession.metadata[key] = value
    accession.fields["scanner"] = "Prisma"
    assert accession.label(levels, store_type=store_type) == {}
    staged = tmp_path / "staged"
    accession.save(staged, copy_mode=FileSet.CopyMode.copy)
    return staged


@pytest.fixture(params=["local", "memory"])
def dest(request, tmp_path: Path):
    """The root of a store on a local path, or on an fsspec file system that isn't
    local (as S3 or SFTP would be)"""
    if request.param == "local":
        yield str(tmp_path / "store")
    else:
        root = f"memory://store-{uuid.uuid4().hex}"
        yield root
        fs, path = fsspec.core.url_to_fs(root)
        if fs.exists(path):
            fs.rm(path, recursive=True)


def _fs(dest: str):
    fs, root = fsspec.core.url_to_fs(dest)
    return fs, root.rstrip("/")


def test_deposit_layout(tmp_path: Path, dest: str) -> None:
    staged = _staged(tmp_path)
    assert store(staged, dest) == []
    fs, root = _fs(dest)
    accession_dir = "/".join([root, *LABELS])
    found = sorted(p[len(accession_dir) + 1 :] for p in fs.find(accession_dir))
    assert found == [
        "1.T1w/JSON/__MANIFEST__.json",
        "1.T1w/JSON/t1.json",
        "1.T1w/NIFTI/__MANIFEST__.json",
        "1.T1w/NIFTI/t1.nii",
        "2.bold/NIFTI/__MANIFEST__.json",
        "2.bold/NIFTI/bold.nii",
        "NOTES/__MANIFEST__.json",
        "NOTES/notes.txt",
        "__FIELDS__.json",
    ]
    assert not any(Metadata.FNAME in p for p in found)  # the metadata isn't kept
    assert fs.cat_file(f"{accession_dir}/1.T1w/NIFTI/t1.nii") == b"contents of t1.nii"
    assert json.loads(fs.cat_file(f"{accession_dir}/__FIELDS__.json")) == {
        "scanner": "Prisma"
    }
    assert check_store(staged, dest) == []


@pytest.mark.parametrize(
    "levels,labels",
    [
        ({"site": "SiteID", "subject": "PatientID"}, ("SYD", "P01")),
        (
            {
                "site": "SiteID",
                "extra": "Extra",
                "subject": "PatientID",
                "visit": "Visit",
            },
            ("SYD", "X", "P01", "V1"),
        ),
    ],
)
def test_deposit_depth_follows_levels(
    tmp_path: Path, dest: str, levels: dict[str, str], labels: tuple[str, ...]
) -> None:
    staged = _staged(tmp_path, levels)
    assert store(staged, dest) == []
    fs, root = _fs(dest)
    assert fs.exists("/".join([root, *labels, "2.bold", "NIFTI", "bold.nii"]))


def test_deposit_again_skips_what_is_there(
    tmp_path: Path, dest: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    staged = _staged(tmp_path)
    assert store(staged, dest) == []
    fs, _ = _fs(dest)
    put_file = type(fs).put_file
    copied: list[str] = []

    def recording_put_file(self, lpath, rpath, *args, **kwargs):  # type: ignore[no-untyped-def]
        copied.append(rpath)
        return put_file(self, lpath, rpath, *args, **kwargs)

    monkeypatch.setattr(type(fs), "put_file", recording_put_file)
    assert store(staged, dest) == []
    assert copied == []


def test_interrupted_deposit_is_finished(tmp_path: Path, dest: str) -> None:
    staged = _staged(tmp_path)
    assert store(staged, dest) == []
    fs, root = _fs(dest)
    resource_dir = "/".join([root, *LABELS, "1.T1w", "NIFTI"])
    # A deposit that stopped before writing the manifest, leaving a stray file
    fs.rm_file(f"{resource_dir}/{Resource.MANIFEST_FNAME}")
    fs.pipe_file(f"{resource_dir}/partial.tmp", b"half")
    assert check_store(staged, dest)  # reported as incomplete
    assert store(staged, dest) == []
    assert fs.exists(f"{resource_dir}/{Resource.MANIFEST_FNAME}")
    assert not fs.exists(f"{resource_dir}/partial.tmp")
    assert check_store(staged, dest) == []


def _change_staged_file(staged: Path) -> None:
    """Change a staged file, and its manifest to match"""
    resource_dir = next(staged.iterdir()) / "1.T1w" / "NIFTI"
    (resource_dir / "t1.nii").write_text("new contents")
    manifest_fpath = resource_dir / Resource.MANIFEST_FNAME
    manifest = json.loads(manifest_fpath.read_text())
    manifest["checksums"]["t1.nii"] = hashlib.md5(b"new contents").hexdigest()
    manifest_fpath.write_text(json.dumps(manifest))


def test_changed_resource_needs_overwrite(tmp_path: Path, dest: str) -> None:
    staged = _staged(tmp_path)
    assert store(staged, dest) == []
    _change_staged_file(staged)
    errors = store(staged, dest)
    assert len(errors) == 1 and "--overwrite" in errors[0]
    fs, root = _fs(dest)
    t1 = "/".join([root, *LABELS, "1.T1w", "NIFTI", "t1.nii"])
    assert fs.cat_file(t1) == b"contents of t1.nii"  # left as it was
    assert store(staged, dest, overwrite=True) == []
    assert fs.cat_file(t1) == b"new contents"


def test_check_store_finds_missing_and_corrupted_files(
    tmp_path: Path, dest: str
) -> None:
    staged = _staged(tmp_path)
    assert store(staged, dest) == []
    fs, root = _fs(dest)
    accession_dir = "/".join([root, *LABELS])

    # Corrupted, at the same size: only found by checksum
    fs.pipe_file(f"{accession_dir}/2.bold/NIFTI/bold.nii", b"CONTENTS OF BOLD.NII")
    problems = check_store(staged, dest)
    assert len(problems) == 1 and "corrupted" in problems[0]
    assert check_store(staged, dest, checksums=False) == []

    # Truncated: found by size too
    fs.pipe_file(f"{accession_dir}/2.bold/NIFTI/bold.nii", b"short")
    problems = check_store(staged, dest, checksums=False)
    assert len(problems) == 1 and "wrong size" in problems[0]

    # Missing
    fs.rm_file(f"{accession_dir}/NOTES/notes.txt")
    problems = check_store(staged, dest, checksums=False)
    assert any("notes.txt' is missing" in p for p in problems)


def test_accession_labelled_for_another_store_is_refused(
    tmp_path: Path, dest: str
) -> None:
    staged = _staged(
        tmp_path,
        {"project": "SiteID", "subject": "PatientID", "session": "Visit"},
        store_type="xnat",
    )
    errors = store(staged, dest)
    assert len(errors) == 1 and "'xnat' data store, not a tree" in errors[0]
    fs, root = _fs(dest)
    assert not fs.exists("/".join([root, *LABELS]))


def test_label_and_store_tree_cli(tmp_path: Path) -> None:
    """From a collated accession to a tree in a (non-local) store, through the CLI"""
    staged = _staged(tmp_path)
    accession_dir = next(staged.iterdir())
    # Make it a collated, not-yet-labelled accession again
    collated = tmp_path / "collated"
    collated.mkdir()
    accession_dir.rename(collated / f"{Accession.PRE_LABEL_PREFIX}1.2.3")
    metadata_fpath = collated / f"{Accession.PRE_LABEL_PREFIX}1.2.3" / Metadata.FNAME
    metadata = json.loads(metadata_fpath.read_text())
    for key in (Accession.LABELS_METADATA_KEY, Accession.STORE_TYPE_METADATA_KEY):
        metadata.pop(key)
    metadata_fpath.write_text(json.dumps(metadata))

    dest = f"memory://cli-{uuid.uuid4().hex}"
    runner = CliRunner()
    labelled = tmp_path / "labelled"
    result = runner.invoke(
        cli,
        ["label", "tree", str(collated), str(labelled)]
        + ["--level", "site", "SiteID", "--level", "subject", "PatientID"],
        env=CLEAN_ENV,
    )
    assert result.exit_code == 0, result.output
    assert [p.name for p in labelled.iterdir()] == ["SYD.P01"]
    result = runner.invoke(cli, ["store", "tree", str(labelled), dest], env=CLEAN_ENV)
    assert result.exit_code == 0, result.output
    fs, root = _fs(dest)
    assert fs.exists(f"{root}/SYD/P01/1.T1w/NIFTI/t1.nii")
    result = runner.invoke(
        cli, ["check-store", "tree", str(labelled), dest], env=CLEAN_ENV
    )
    assert result.exit_code == 0, result.output
    fs.rm_file(f"{root}/SYD/P01/1.T1w/NIFTI/t1.nii")
    result = runner.invoke(
        cli, ["check-store", "tree", str(labelled), dest], env=CLEAN_ENV
    )
    assert result.exit_code == 1
    fs.rm(root, recursive=True)
