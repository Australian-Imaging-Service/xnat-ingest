# - [ ] Email errors from upload script - emails configurable via env var
# - [ ] add delete option to upload script
# - [ ] add option to manually specify project/subject/session IDs in YAML file
# - [ ] handle partial re-uploads (override, merge, error), option in YAML file
# - [ ] Generalise to handle different types of raw data files
# - [ ] Send instructions to Dean/Fang on how it should be configured
# - [ ] Pull info from DICOM headers/OHIF viewer
from pathlib import Path
import re
import json
import shutil
import traceback
import time
import typing as ty
from collections import defaultdict
import hashlib
import logging.config
import logging.handlers
import yaml
import click
from tqdm import tqdm
import pydicom
from xnat import connect
from .base import cli
from ..utils import logger


HASH_CHUNK_SIZE = 2**20


class LoggerEmail:
    def __init__(self, address, loglevel, subject):
        self.address = address
        self.loglevel = loglevel
        self.subject = subject

    @classmethod
    def split_envvar_value(cls, envvar):
        return [cls(*entry.split(",")) for entry in envvar.split(";")]

    def __str__(self):
        return self.address


class MailServer:
    def __init__(self, host, sender_email, user, password):
        self.host = host
        self.sender_email = sender_email
        self.user = user
        self.password = password

    @classmethod
    def split_envvar_value(cls, envvar):
        return cls(*envvar.split(";"))


class NonDicom(str):
    @classmethod
    def split_envvar_value(cls, envvar):
        return [cls(entry) for entry in envvar.split("%")]


class DicomField:
    def __init__(self, keyword_or_tag):
        # Get the tag associated with the keyword
        try:
            self.tag = pydicom.datadict.tag_for_keyword(keyword_or_tag)
        except ValueError:
            try:
                self.keyword = pydicom.datadict.dictionary_description(keyword_or_tag)
            except ValueError:
                raise ValueError(
                    f'Could not parse "{keyword_or_tag}" as a DICOM keyword or tag'
                )
            else:
                self.tag = keyword_or_tag
        else:
            self.keyword = keyword_or_tag

    def __str__(self):
        return f"'{self.keyword}' field ({','.join(self.tag)})"


@cli.command(
    help="""uploads all scans found in the export directory to XNAT. It assumes
that each session is saved in a separate sub-directory and contains DICOM files with
metadata fields containing the XNAT project, subject and session IDs to upload the data to

Non-dicom data found in the directory will be uploaded if it matches any of the regular
expressions passed to the `--non-dicom` flags.

EXPORT_DIR is the directory that the session data has been exported to from the ICS console

SERVER is address of the XNAT server to upload the scans up to. Can alternatively provided
by setting the "XNAT_EXPORTED_SCANS_HOST" environment variable.

USER is the XNAT user to connect with, alternatively the "XNAT_EXPORTED_SCANS_USER" env. var

PASSWORD is the password for the XNAT user, alternatively "XNAT_EXPORTED_SCANS_PASS" env. var
""",
)
@click.argument("export_dir", type=click.Path(path_type=Path))
@click.argument("server", type=str, envvar="XNAT_EXPORTED_SCANS_HOST")
@click.argument("user", type=str, envvar="XNAT_EXPORTED_SCANS_USER")
@click.argument("password", type=str, envvar="XNAT_EXPORTED_SCANS_PASS")
@click.option(
    "--project-field",
    type=DicomField,
    default="StudyID",
    envvar="XNAT_EXPORTED_SCANS_PROJECT",
    help=("The keyword or tag of the DICOM field to extract the XNAT project ID from "),
)
@click.option(
    "--subject-field",
    type=DicomField,
    default="PatientID",
    envvar="XNAT_EXPORTED_SCANS_SUBJECT",
    help=("The keyword or tag of the DICOM field to extract the XNAT subject ID from "),
)
@click.option(
    "--session-field",
    type=DicomField,
    default="AccessionNumber",
    envvar="XNAT_EXPORTED_SCANS_SESSION",
    help=(
        "The keyword or tag of the DICOM field to extract the XNAT imaging session ID from "
    ),
)
@click.option(
    "--dicom-export-dir",
    type=click.Path(path_type=Path),
    default=None,
    help=("Location of exported DICOMs if different from the export_dir"),
)
@click.option(
    "--non-dicom",
    "non_dicoms",
    multiple=True,
    type=NonDicom,
    envvar="XNAT_EXPORTED_SCANS_NONDICOM",
    help=(
        "Regular expressions to extract id and labels from non-DICOM file names. When "
        "provided in a environment variable, multiple patterns are delimited by the '%' "
        "symbol"
    ),
)
@click.option(
    "--dicom-ext",
    type=str,
    default=".dcm",
    envvar="XNAT_EXPORTED_SCANS_DICOMEXT",
    help=("The extension of the DICOM files to look for"),
)
@click.option(
    "--delete/--dont-delete",
    default=False,
    envvar="XNAT_EXPORTED_SCANS_DELETE",
    help="Whether to delete the session directories after they have been uploaded or not",
)
@click.option(
    "--log-file",
    default=None,
    type=click.Path(path_type=Path),
    envvar="XNAT_EXPORTED_SCANS_LOGFILE",
    help=(
        'Location to write the output logs to, defaults to "upload-logs" in the '
        "export directory"
    ),
)
@click.option(
    "--log-email",
    "log_emails",
    type=LoggerEmail,
    metavar="<address> <loglevel> <subject-preamble>",
    multiple=True,
    envvar="XNAT_EXPORTED_SCANS_LOGEMAIL",
    help=(
        "Email(s) to send logs to. When provided in an environment variable, "
        "mail and log level are delimited by ',' and separate destinations by ';'"
    ),
)
@click.option(
    "--mail-server",
    type=MailServer,
    metavar="<host> <sender-email> <user> <password>",
    default=None,
    envvar="XNAT_EXPORTED_SCANS_MAILSERVER",
    help=(
        "the mail server to send logger emails to. When provided in an environment variable, "
        "args are delimited by ';'"
    ),
)
@click.option(
    "--staging-dir-name",
    default="XNAT_UPLOAD_STAGING",
    type=str,
    envvar="XNAT_EXPORTED_SCANS_STAGINGDIRNAME",
    help=(
        "The name of the directory that files are staged in before they are uploaded to "
        "XNAT"
    ),
)
@click.option(
    "--exclude-dicoms/--include-dicoms",
    default=False,
    type=bool,
    envvar="XNAT_EXPORTED_SCANS_EXCLUDEDICOM",
    help=("Whether to exclude DICOM scans from upload or not"),
)
@click.option(
    "--ignore",
    type=str,
    multiple=True,
    help=(
        "File patterns (regular expressions) to ignore, all files must either be "
        "explicitly included or ignored"
    ),
)
def upload(
    export_dir,
    server,
    user,
    password,
    project_field,
    subject_field,
    session_field,
    dicom_export_dir,
    non_dicoms,
    dicom_ext,
    delete,
    log_file,
    log_emails,
    mail_server,
    staging_dir_name,
    exclude_dicoms,
    ignore,
):
    # Configure the email logger
    if log_emails:
        if not mail_server:
            raise ValueError(
                "Mail server needs to be provided, either by `--mail-server` option or "
                "XNAT_EXPORTED_SCANS_MAILSERVER environment variable if logger emails "
                "are provided:" + ", ".join(log_emails)
            )
        for log_email in log_emails:
            smtp_handler = logging.handlers.SMTPHandler(
                mailhost=mail_server.host,
                fromaddr=mail_server.sender_email,
                toaddrs=[log_email.address],
                subject=log_email.subject,
                credentials=(mail_server.user, mail_server.password),
                secure=None,
            )
            logger.addHandler(smtp_handler)
            logger.info(f"Email logger configured for {log_email}")

    # Configure the file logger
    if log_file is None:
        log_file = (
            export_dir
            / "upload-logs"
            / (time.strftime("%Y%m%d", time.localtime()) + ".log")
        )
    log_file.parent.mkdir(exist_ok=True)
    log_file_hdle = logging.FileHandler(log_file)
    log_file_hdle.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(log_file_hdle)

    non_dicom_res = []
    re_errors = []
    for non_dicom in non_dicoms:
        try:
            non_dicom_re = re.compile(non_dicom)
        except Exception:
            re_errors.append((non_dicom, traceback.format_exc()))
        else:
            non_dicom_res.append(non_dicom_re)
    if re_errors:
        raise RuntimeError(
            "Could not parse regular expressions for the following non-dicom patterns:\n"
            "\n\n".join("\n---\n".join(err) for err in re_errors)
        )

    session_dirs = [
        d
        for d in (dicom_export_dir if dicom_export_dir else export_dir).iterdir()
        if d.is_dir() and not d.name.startswith(".")
    ]

    with connect(server=server, user=user, password=password) as xlogin:
        for session_dir in tqdm(
            session_dirs, f"Parsing scan export directories in '{export_dir}'"
        ):
            dicom_files = list(session_dir.glob("*" + dicom_ext))
            if not dicom_files:
                logger.error(
                    f"Did not find any dicom files (*{dicom_ext}) in directory '{session_dir}",
                )
                continue
            upload_staging_dir = session_dir / staging_dir_name
            upload_staging_dir.mkdir(exist_ok=True)
            spec_file = upload_staging_dir / "UPLOAD-SPECIFICATION.yaml"
            if spec_file.exists():
                try:
                    with open(spec_file) as f:
                        spec = yaml.load(f, Loader=yaml.SafeLoader)
                except Exception:
                    logger.error(
                        f"Could not load specification file from '{spec_file}', please "
                        f"correct it and try again, skipping session:\n"
                        + traceback.format_exc()
                    )
                    continue
                loaded_ids = [
                    i
                    for i in ("project", "subject", "session")
                    if spec.get(i) is not None
                ]
                logger.info(f"Loaded IDs for {loaded_ids} from '{spec_file}'")
            else:
                logger.info(f"Did not find manual specification file at '{spec_file}'")
                project_id = subject_id = session_id = None
            by_scan_id = defaultdict(list)
            session_id_dct = defaultdict(list)
            subject_id_dct = defaultdict(list)
            project_id_dct = defaultdict(list)
            for dcm_file in dicom_files:
                dcm = pydicom.dcmread(dcm_file)
                by_scan_id[dcm.SeriesNumber].append(dcm_file)
                project_id_dct[dcm.get(project_field.keyword)].append(dcm_file)
                subject_id_dct[dcm.get(subject_field.keyword)].append(dcm_file)
                session_id_dct[dcm.get(session_field.keyword)].append(dcm_file)
            id_errors = False
            if project_id is None:
                project_ids = list(project_id_dct)
                if len(list(project_ids)) > 1:
                    logger.error(
                        f"Incosistent project IDs found in {project_field}:\n"
                        + json.dumps(project_id_dct, indent=4)
                    )
                    id_errors = True
                else:
                    project_id = project_ids[0]
                    if not project_id:
                        logger.error(f"Project ID ({project_field}) not provided")
                        id_errors = True
            if subject_id is None:
                subject_ids = list(subject_id_dct)
                if len(subject_ids) > 1:
                    logger.error(
                        f"Incosistent subject IDs found in {subject_field}:\n"
                        + json.dumps(subject_id_dct, indent=4)
                    )
                    id_errors = True
                else:
                    # FIXME: space is present in test data, but shouldn't be in prod
                    subject_id = subject_ids[0].replace(" ", "_")
                    if not subject_id:
                        logger.error(f"Subject ID ({subject_field}) not provided")
                        id_errors = True
            if session_id is None:
                session_ids = list(session_id_dct)
                if len(session_ids) > 1:
                    logger.error(
                        f"Incosistent session IDs found in {session_field}:\n"
                        + json.dumps(session_id_dct, indent=4)
                    )
                    id_errors = True
                else:
                    session_id = session_ids[0]
                    if not session_id:
                        logger.error(f"Session ID ({session_field}) not provided")
                        id_errors = True
            if id_errors:
                logger.error(
                    f"Aborting upload of '{session_dir}' directory due to errors "
                    "extracting the IDs (see above)"
                )
                spec = {
                    "project": project_id,
                    "subject": subject_id,
                    "session": session_id,
                    "overwrite": False,
                }
                yaml.dump(spec, spec_file)
                continue  # Skip this session, will require manual editing of specs
            if dicom_export_dir:
                non_dicom_dir = None
            else:
                non_dicom_dir = None
            xproject = xlogin.projects[project_id]
            xsubject = xlogin.classes.SubjectData(label=subject_id, parent=xproject)
            try:
                xsession = xproject.experiments[session_id]
            except KeyError:
                xsession = xlogin.classes.MrSessionData(
                    label=session_id, parent=xsubject
                )
            session_path = f"{project_id}:{subject_id}:{session_id}"
            # Anonymise DICOMs and save to directory prior to upload
            if not exclude_dicoms:
                logger.info(f"Uploading DICOMS from '{session_dir}' to {session_path}")
                for scan_id, dicom_files in by_scan_id.items():
                    resource_dir = upload_staging_dir / str(scan_id) / "DICOM"
                    resource_dir.mkdir(parents=True)
                    for dicom_file in dicom_files:
                        dcm = pydicom.dcmread(dicom_file)
                        for field in FIELDS_TO_DELETE:
                            try:
                                del dcm[field]
                            except KeyError:
                                pass
                        dcm.save_as(resource_dir / dicom_file.name)
            else:
                logger.info("Omitting DICOMS as `--exclude-dicoms` is set")
            parsing_errors = False
            # Extract scan and resource labels from raw data files to link to upload
            # directory
            for non_dicom_re in non_dicom_res:
                non_dicom_files = [
                    p for p in session_dir.iterdir() if non_dicom_re.match(p.name)
                ]
                for non_dicom_file in non_dicom_files:
                    match = non_dicom_re.match(non_dicom_file.name)
                    label_comps = list(match.groups())
                    scan_id = "_".join(label_comps).strip("_").lower()
                    resource_label = label_comps[-1].strip("_").lower()
                    resource_dir = upload_staging_dir / scan_id / resource_label
                    index = 1
                    while resource_dir.exists():
                        index += 1
                        resource_dir = (
                            upload_staging_dir / f"{scan_id}{index}" / resource_label
                        )
                    resource_dir.mkdir(parents=True)
                    target_path = resource_dir / non_dicom_file.name
                    target_path.hardlink_to(non_dicom_file)
            if parsing_errors:
                logger.info(
                    f"Aborting upload of '{session_dir}' because was not able to labels "
                    f"for of non-DICOM files with patterns: {non_dicoms}"
                )
                continue
            upload_errors = None
            partial_upload = False
            for scan_dir in tqdm(
                list(upload_staging_dir.iterdir()),
                f"Uploading scans found in {session_dir}",
            ):
                scan_id = scan_dir.name
                xscan = xlogin.classes.MrScanData(
                    id=scan_id, type=scan_id, parent=xsession
                )
                for resource_dir in scan_dir.iterdir():
                    resource_name = resource_dir.name
                    xresource = xscan.create_resource(resource_name)
                    xresource.upload_dir(resource_dir)
                    remote_checksums = get_checksums(xresource)
                    calc_checksums = calculate_checksums(resource_dir)
                    if remote_checksums != calc_checksums:
                        mismatching = [
                            k
                            for k, v in remote_checksums.items()
                            if v != calc_checksums[k]
                        ]
                        logger.error(
                            "Checksums do not match after upload of "
                            f"'{session_dir}:{scan_id}:{resource_name}' resource. "
                            f"Mismatching files were {mismatching}"
                        )
                        upload_errors = True
                    else:
                        partial_upload = True
                if upload_errors:
                    if not partial_upload:
                        logger.info(
                            f"Error on first upload from '{session_dir}', aborting"
                        )
                        break
                else:
                    logger.info(f"Uploaded '{scan_id}' in '{session_dir}'")
            if upload_errors:
                if partial_upload:
                    logger.error(
                        f"Some files did not upload correctly from "
                        f"'{session_dir}', a partial upload has been created"
                    )
                else:
                    logger.error(f"Could not upload any files from '{session_dir}'")
            else:
                msg = f"Succesfully uploaded all files in '{session_dir}'"
                if delete:
                    msg += ", deleting"
                logger.info(msg)
                if delete:
                    shutil.rmtree(session_dir)
                    if non_dicom_dir:
                        shutil.rmtree(non_dicom_dir)
                    logger.info(f"Successfully deleted '{session_dir}' after upload")


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
