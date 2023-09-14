from pathlib import Path
import re
import shutil
import typing as ty
from collections import defaultdict
import hashlib
from click.testing import CliRunner
import click
from tqdm import tqdm
import pydicom
from xnat import connect
from .base import cli
from ..utils import show_cli_trace, log, log_error

HASH_CHUNK_SIZE = 2**20


@cli.command(
    name="upload-exported",
    help=""""upload" uploads all exported scans in the export directory and deletes them
after the upload is complete (and checked)

EXPORT_DIR is the directory that the session data has been exported to from the ICS console

SERVER is address of the XNAT server to upload the scans up to. Can alternatively provided
by setting the "XNAT_HOST" environment variable.

USER is the XNAT user to connect with, alternatively the "XNAT_USER" env. var

PASSWORD is the password for the XNAT user, alternatively "XNAT_PASS" env. var
""",
)
@click.argument("export_dir", type=click.Path(path_type=Path))
@click.argument("server", type=str, envvar="XNAT_HOST")
@click.argument("user", type=str, envvar="XNAT_USER")
@click.argument("password", type=str, envvar="XNAT_PASS")
@click.option(
    "--dry-run/--live-run",
    type=bool,
    default=False,
    help="whether to actually delete the scans or just record what will be deleted",
)
@click.option(
    "--overwrite/--dont-overwrite",
    default=False,
    help="flags whether to overwrite existing sessions if present",
)
def upload_exported(
    export_dir,
    server,
    user,
    password,
    dry_run,
    overwrite,
):
    with connect(server=server, user=user, password=password) as xlogin:
        session_dirs = [
            d for d in export_dir.iterdir() if d.is_dir() and not d.name.startswith(".")
        ]
        for session_dir in tqdm(session_dirs, "Uploading exported directories"):
            dicom_files = list(session_dir.glob("*.IMA"))
            if not dicom_files:
                dicom_files = list(session_dir.glob("*.dcm"))
            if not dicom_files:
                log_error(
                    f"Did not find any dicom files (*.IMA) in directory '{str(session_dir)}",
                )
                continue
            by_scan_id = defaultdict(list)
            session_id_dct = defaultdict(list)
            subject_id_dct = defaultdict(list)
            project_id_dct = defaultdict(list)
            for dcm_file in dicom_files:
                dcm = pydicom.dcmread(dcm_file)
                by_scan_id[dcm.SeriesNumber].append(dcm_file)
                project_id_dct[dcm.StudyID].append(dcm_file)
                subject_id_dct[dcm.PatientID].append(dcm_file)
                session_id_dct[dcm.AccessionNumber].append(dcm_file)
            project_ids = list(project_id_dct)
            subject_ids = list(subject_id_dct)
            session_ids = list(session_id_dct)
            if len(list(project_ids)) > 1:
                log_error(
                    f"Incosistent project IDs (StudyID - 0020,0010) found in "
                    f"'{str(session_dir)}' directory: {list(session_ids)}:\n{project_id_dct}"
                )
                continue
            if len(subject_ids) > 1:
                log_error(
                    f"Incosistent subject IDs (PatientID - 0010,0020) found in "
                    f"'{str(session_dir)}' directory: {list(subject_ids)}:\n{subject_id_dct}"
                )
                continue
            if len(session_ids) > 1:
                log_error(
                    f"Incosistent session IDs (AccessionNumber - 0008,0050) found in "
                    f"'{str(session_dir)}' directory: {list(session_ids)}\n{session_id_dct}"
                )
                continue
            project_id = project_ids[0]
            subject_id = subject_ids[0].replace(" ", "_")  # space is present in test data
            session_id = session_ids[0]
            if not project_id:
                log_error(
                    f"Project ID (StudyID - 0020,0010) not provided in "
                    f"'{str(session_dir)}' directory"
                )
                continue
            if not subject_id:
                log_error(
                    f"Subject ID (PatientID - 0010,0020) not provided in "
                    f"'{str(session_dir)}' directory"
                )
                continue
            if not session_id:
                log_error(
                    f"Session ID (AccessionNumber - 0008,0050) not provided in "
                    f"'{str(session_dir)}' directory"
                )
                continue
            xproject = xlogin.projects[project_id]
            xsubject = xlogin.classes.SubjectData(label=subject_id, parent=xproject)
            try:
                xsession = xproject.experiments[session_id]
            except KeyError:
                xsession = xlogin.classes.MrSessionData(
                    label=session_id, parent=xsubject
                )
            errors = False
            to_upload_dir = session_dir / "TO_UPLOAD"
            if to_upload_dir.exists():
                shutil.rmtree(to_upload_dir)
            to_upload_dir.mkdir()
            # Anonymise DICOMs and save to directory prior to upload
            for scan_id, dicom_files in by_scan_id.items():
                resource_dir = to_upload_dir / str(scan_id) / "DICOM"
                resource_dir.mkdir(parents=True)
                for dicom_file in dicom_files:
                    dcm = pydicom.dcmread(dicom_file)
                    for field in FIELDS_TO_DELETE:
                        try:
                            del dcm[field]
                        except KeyError:
                            pass
                    dcm.save_as(resource_dir / dicom_file.name)
            # Extract scan and resource labels from raw data files to link to upload
            # directory
            for raw_file in sorted(session_dir.glob("*.ptd")):
                label_comps = re.findall(r".*PET(\w+)", raw_file.name)
                if not label_comps:
                    log_error(
                        "Could not extract scan label from raw file name '%s'"
                        "in '%s', and therefore cannot upload",
                        raw_file.name,
                        str(session_dir),
                    )
                    errors = True
                    continue
                scan_id = "_".join(label_comps).strip("_").lower()
                resource_label = label_comps[-1].strip("_").lower()
                resource_dir = to_upload_dir / scan_id / resource_label
                index = 1
                while resource_dir.exists():
                    index += 1
                    resource_dir = to_upload_dir / f"{scan_id}{index}" / resource_label
                resource_dir.mkdir(parents=True)
                target_path = resource_dir / raw_file.name
                target_path.hardlink_to(raw_file)
            if errors:
                log(f"Was not able to parse IDs and labels for '{session_dir}', skipping")
            else:
                some_uploaded = False
                for scan_dir in to_upload_dir.iterdir():
                    scan_id = scan_dir.name
                    xscan = xlogin.classes.MrScanData(
                        id=scan_id, type=scan_id, parent=xsession
                    )
                    for resource_dir in scan_dir.iterdir():
                        xresource = xscan.create_resource(resource_dir.name)
                        xresource.upload_dir(resource_dir)
                        remote_checksums = get_checksums(xresource)
                        calc_checksums = calculate_checksums(resource_dir)
                        if remote_checksums != calc_checksums:
                            log_error(
                                f"Checksums do not match files uploaded for '{str(session_dir)}':\n"
                                f"{remote_checksums}\n\nvs\n\n{calc_checksums}",
                            )
                            errors = True
                        else:
                            some_uploaded = True
                    log(f"Uploaded '{raw_file}' in '{session_dir}'")
                if errors:
                    if some_uploaded:
                        log_error(
                            f"Some files did not upload correctly from "
                            f"'{session_dir}', a partial upload has been created"
                        )
                    else:
                        log_error(f"Could not upload any files from '{session_dir}'")
                else:
                    log(f"Succesfully uploaded all files in '{session_dir}', deleting")
                    shutil.rmtree(session_dir)


def get_checksums(resource):
    """
    Downloads the MD5 digests associated with the files in a resource.
    These are saved with the downloaded files in the cache and used to
    check if the files have been updated on the server
    """
    result = resource.xnat_session.get(resource.uri + "/files")
    if result.status_code != 200:
        raise RuntimeError(
            "Could not download metadata for resource {}. Files "
            "may have been uploaded but cannot check checksums".format(resource.id)
        )
    return dict((r["Name"], r["digest"]) for r in result.json()["ResultSet"]["Result"])


def calculate_checksums(resource_dir: Path) -> ty.Dict[str, str]:
    checksums = {}
    for fpath in resource_dir.iterdir():
        try:
            hsh = hashlib.md5()
            with open(fpath, "rb") as f:
                for chunk in iter(lambda: f.read(HASH_CHUNK_SIZE), b""):
                    hsh.update(chunk)
            checksum = hsh.hexdigest()
        except OSError:
            raise RuntimeError("Could not create digest of '{}' ".format(fpath))
        checksums[fpath.name] = checksum
    return checksums



FIELDS_TO_MODIFY = [
    ("0010", "0030"),  # Patient's Birth Date
    ("0010", "1010"),  # Patient's Age
]


FIELDS_TO_DELETE = [
    ("0008", "0014"),  # Instance Creator UID
    ("0008", "1111"),  # Referenced Performed Procedure Step SQ
    ("0008", "1120"),  # Referenced Patient SQ
    ("0008", "1140"),  # Referenced Image SQ
    ("0008", "0096"),  # Referring Physician Identification SQ
    ("0008", "1032"),  # Procedure Code SQ
    ("0008", "1048"),  # Physician(s) of Record
    ("0008", "1049"),  # Physician(s) of Record Identification SQ
    ("0008", "1050"),  # Performing Physicians' Name
    ("0008", "1052"),  # Performing Physician Identification SQ
    ("0008", "1060"),  # Name of Physician(s) Reading Study
    ("0008", "1062"),  # Physician(s) Reading Study Identification SQ
    ("0008", "1110"),  # Referenced Study SQ
    ("0008", "1111"),  # Referenced Performed Procedure Step SQ
    ("0008", "1250"),  # Related Series SQ
    ("0008", "9092"),  # Referenced Image Evidence SQ
    ("0008", "0080"),  # Institution Name
    ("0008", "0081"),  # Institution Address
    ("0008", "0082"),  # Institution Code Sequence
    ("0008", "0092"),  # Referring Physician's Address
    ("0008", "0094"),  # Referring Physician's Telephone Numbers
    ("0008", "009C"),  # Consulting Physician's Name
    ("0008", "1070"),  # Operators' Name
    ("0010", "4000"),  # Patient Comments
    ("0010", "0010"),  # Patient's Name
    ("0010", "0021"),  # Issuer of Patient ID
    ("0010", "0032"),  # Patient's Birth Time
    ("0010", "0050"),  # Patient's Insurance Plan Code SQ
    ("0010", "0101"),  # Patient's Primary Language Code SQ
    ("0010", "1000"),  # Other Patient IDs
    ("0010", "1001"),  # Other Patient Names
    ("0010", "1002"),  # Other Patient IDs SQ
    ("0010", "1005"),  # Patient's Birth Name
    ("0010", "1010"),  # Patient's Age
    ("0010", "1040"),  # Patient's Address
    ("0010", "1060"),  # Patient's Mother's Birth Name
    ("0010", "1080"),  # Military Rank
    ("0010", "1081"),  # Branch of Service
    ("0010", "1090"),  # Medical Record Locator
    ("0010", "2000"),  # Medical Alerts
    ("0010", "2110"),  # Allergies
    ("0010", "2150"),  # Country of Residence
    ("0010", "2152"),  # Region of Residence
    ("0010", "2154"),  # Patient's Telephone Numbers
    ("0010", "2160"),  # Ethnic Group
    ("0010", "2180"),  # Occupation
    ("0010", "21A0"),  # Smoking Status
    ("0010", "21B0"),  # Additional Patient History
    ("0010", "21C0"),  # Pregnancy Status
    ("0010", "21D0"),  # Last Menstrual Date
    ("0010", "21F0"),  # Patient's Religious Preference
    ("0010", "2203"),  # Patient's Sex Neutered
    ("0010", "2297"),  # Responsible Person
    ("0010", "2298"),  # Responsible Person Role
    ("0010", "2299"),  # Responsible Organization
    ("0020", "9221"),  # Dimension Organization SQ
    ("0020", "9222"),  # Dimension Index SQ
    ("0038", "0010"),  # Admission ID
    ("0038", "0011"),  # Issuer of Admission ID
    ("0038", "0060"),  # Service Episode ID
    ("0038", "0061"),  # Issuer of Service Episode ID
    ("0038", "0062"),  # Service Episode Description
    ("0038", "0500"),  # Patient State
    ("0038", "0100"),  # Pertinent Documents SQ
    ("0040", "0260"),  # Performed Protocol Code SQ
    ("0088", "0130"),  # Storage Media File-Set ID
    ("0088", "0140"),  # Storage Media File-Set UID
    ("0400", "0561"),  # Original Attributes Sequence
    ("5200", "9229"),  # Shared Functional Groups SQ
]

if __name__ == "__main__":
    runner = CliRunner()
    result = runner.invoke(
        upload_exported,
        [
            "/a/export/dir" "https://xnat.sydney.edu.au",
            "--dry-run",
        ],
        catch_exceptions=False,
    )
    if result.exit_code:
        print(show_cli_trace(result))
