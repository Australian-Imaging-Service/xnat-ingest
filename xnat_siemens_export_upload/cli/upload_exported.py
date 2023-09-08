import os
from pathlib import Path
import re
from traceback import format_exc
from click.testing import CliRunner
import click
import logging
from tqdm import tqdm
import pydicom
from xnat import connect
from .base import cli
from ..utils import show_cli_trace


logger = logging.getLogger("xnat-siemens-export-upload")


@cli.command(
    name="upload-exported",
    help=""""upload" uploads all exported scans in the export directory and deletes them
after the upload is complete (and checked)

EXPORT_DIR is the directory of the 

SERVER is address of the XNAT server to upload the scans up to
""",
)
@click.argument("export_dir", type=click.Path(path_type=Path))
@click.argument("server", type=str)
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
    dry_run,
    overwrite,
):
    host = os.environ["XNAT_HOST"]
    user = os.environ["XNAT_USER"]
    passwd = os.environ["XNAT_PASS"]

    with connect(server=host, user=user, password=passwd) as xlogin:
        session_dirs = [d for d in export_dir.iterdir() if not session_dir.is_dir() or session_dir.name.startswith(".")]
        for session_dir in tqdm(session_dirs, "Uploading exported directories"):
            dicom_files = list(session_dir.glob("*.IMA"))
            if not dicom_files:
                logger.warning(
                    "Did not find any dicom files (*.IMA) in directory '%s', skipping",
                    str(session_dir),
                )
                continue
            dcm = pydicom.dcmread(dicom_files[0])
            project_id = dcm.AccessionNumber
            if project_id is None:
                logger.error(
                    "Did not find project ID DICOM file (AccessionNumber - 0008,0050) in '%s' directory  "
                    "and therefore cannot upload", str(session_dir)
                )
                continue
            subject_id = dcm.PatientID
            if subject_id is None:
                logger.error(
                    "Did not find subject ID in DICOM file (PatientID - 0010,0020) in '%s' directory  "
                    "and therefore cannot upload", str(session_dir)
                )
                continue
            session_id = dcm.ReferringPhysicianName
            if session_id is None:
                logger.error(
                    "Did not find session ID in DICOM file (ReferringPhysicianName - 0008,0090) "
                    "in '%s' directory, and therefore cannot upload", str(session_dir)
                )
                continue
            xproject = xlogin.projects[project_id]
            xsubject = xlogin.classes.SubjectData(label=subject_id, parent=xproject)
            try:
                xsession = xproject.experiments[session_id]
            except KeyError:
                xsession = xclasses.MrSessionData(label=session_id, parent=xsubject)
            errors = False
            # TODO: Need to extract scan ID and use this instead of modality
            for modality in ("PT", "CT"):
                anonymised_dir = (session_dir / f"ANONYMISED-{modality}")
                anonymised_dir.mkdir()
                for dicom_file in session_dir.glob(f"*.{modality}.*.IMA"):
                    dcm = pydicom.dcmread(dicom_file)
                    for field in FIELDS_TO_DELETE:
                        del dcm[field]
                    dcm.save_as(anonymised_dir / dicom_file.name)
            for raw_file in session_dir.glob("*.ptd"):
                label_comps = re.findall(r".*PET(\w+)", raw_file.name)
                if not label_comps:
                    logger.error(
                        "Could not extract scan label from raw file name '%s'"
                        "in '%s', and therefore cannot upload",
                        raw_file.name, str(session_dir)
                    )
                    errors = True
                    continue
                scan_id = "_".join(label_comps).strip("_").lower()
                resource_label = label_comps[-1]
                xscan = xsession.classes.MrScanData(id=scan_id, type=scan_id, parent=xsession)
                xresource = xscan.create_resource(resource_label)
                xresource.upload(raw_file, raw_file.name)
                remote_checksum = get_checksums(xresource)[raw_file.name]
                calc_checksum = calculate_checksum(raw_file)
                if remote_checksum != calc_checksum:
                    logger.error(
                        "Remote checksum of '%s' in '%s', %s, does not match calculated, %s",
                        str(raw_file), str(session_dir), remote_checksum, calc_checksum
                    )
                    errors = True
                    continue
                logger.debug(
                    "Uploaded '%s' in '%s'", str(raw_file), str(session_dir)
                )
            if not errors:
                logger.info(
                    "Succesfully uploaded all files in '%s'", str(session_dir)
                )


def get_checksums(resource):
    """
    Downloads the MD5 digests associated with the files in a resource.
    These are saved with the downloaded files in the cache and used to
    check if the files have been updated on the server
    """
    result = resource.xnat_session.get(resource.uri + '/files')
    if result.status_code != 200:
        raise XnatUtilsError(
            "Could not download metadata for resource {}. Files "
            "may have been uploaded but cannot check checksums"
            .format(resource.id))
    return dict((r['Name'], r['digest'])
                for r in result.json()['ResultSet']['Result'])


def calculate_checksum(fname):
    try:
        file_hash = hashlib.md5()
        with open(fname, 'rb') as f:
            for chunk in iter(lambda: f.read(HASH_CHUNK_SIZE), b''):
                file_hash.update(chunk)
        return file_hash.hexdigest()
    except OSError:
        raise XnatUtilsDigestCheckFailedError(
            "Could not check digest of '{}' ".format(fname))


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
            "/a/export/dir"
            "https://xnat.sydney.edu.au",
            "--dry-run",
        ],
        catch_exceptions=False,
    )
    if result.exit_code:
        print(show_cli_trace(result))
