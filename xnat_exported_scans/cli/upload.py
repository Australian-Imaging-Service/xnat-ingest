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
import attrs
import click
from tqdm import tqdm
import pydicom
from xnat import connect
from .base import cli
from ..utils import logger
from 


HASH_CHUNK_SIZE = 2**20


class DicomParseError(Exception):
    def __init__(self, msg):
        self.msg = msg


class UnsupportedModalityError(Exception):
    def __init__(self, msg):
        self.msg = msg


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


@attrs.define
class SessionMetadata:
    project_id: str
    subject_id: str
    session_id: str
    non_dicom_dir_name: str


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
    "--session-dir-pattern",
    type=str,
    default="{FirstName}_{LastName}_{StudyDate}.*",
    help="Pattern by which to recognise the corresponding non-dicom dir in the export dir"
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
                logger.info(f"Loaded IDs {session_dir} from '{spec_file}':\n{spec}")
            else:
                logger.info(f"Did not find manual specification file at '{spec_file}'")
                spec = {}
            try:
                dicom_scans, metadata = parse_dicom_headers(
                    dicom_files, project_field, subject_field, session_field, spec
                )
            except DicomParseError as e:
                logger.error(
                    f"Aborting upload of '{session_dir}' directory due to errors:\n"
                    + e.msg
                )
                yaml.dump(
                    {
                        "project": metadata.project_id,
                        "subject": metadata.subject_id,
                        "session": metadata.session_id,
                        "overwrite": False,
                    },
                    spec_file,
                )
                continue  # Skip this session, will require manual editing of specs
            if dicom_export_dir:
                non_dicom_dir = None
            else:
                non_dicom_dir = None
            xproject = xlogin.projects[metadata.project_id]
            xsubject = xlogin.classes.SubjectData(label=metadata.subject_id, parent=xproject)
            try:
                xsession = xproject.experiments[metadata.session_id]
            except KeyError:
                modalities = set(s.modality for s in dicom_scans.values())
                if "MR" in modalities:
                    SessionClass = xlogin.classes.MrSessionData
                elif "PT" in modalities:
                    SessionClass = xlogin.classes.PetSessionData
                elif "CT" in modalities:
                    SessionClass = xlogin.classes.CtSessionData
                else:
                    logger.error(
                        f"Found the following unsupported modalities in {session_dir}: {modalities}"
                    )
                    continue
                xsession = SessionClass(label=metadata.session_id, parent=xsubject)
            session_path = f"{metadata.project_id}:{metadata.subject_id}:{metadata.session_id}"
            # Anonymise DICOMs and save to directory prior to upload
            if exclude_dicoms:
                logger.info("Omitting DICOMS as `--exclude-dicoms` is set")
            else:
                logger.info(f"Uploading DICOMS from '{session_dir}' to {session_path}")
                for scan_id, dicom_scan in dicom_scans.items():
                    scan_dir = upload_staging_dir / str(scan_id)
                    scan_dir.mkdir()
                    modality_path = scan_dir / "MODALITY"
                    modality_path.write_text(dicom_scan.modality)
                    resource_dir = scan_dir / "DICOM"
                    resource_dir.mkdir()
                    for dicom_file in dicom_scan.files:
                        dcm = pydicom.dcmread(dicom_file)
                        dcm.PatientBirthDate = dcm.PatientBirthDate[:4] + "0101"
                        for field in FIELDS_TO_DELETE:
                            try:
                                del dcm[field]
                            except KeyError:
                                pass
                        dcm.save_as(resource_dir / dicom_file.name)
            # Extract scan and resource labels from raw data files to link to upload
            # directory
            parsing_errors = False
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
                modality_path = scan_dir / "MODALITY"
                if modality_path.exists():
                    modality = modality_path.read_text()
                    if modality == "SC":
                        ScanClass = xlogin.classes.ScScanData
                    elif modality == "MR":
                        ScanClass = xlogin.classes.MrScanData
                    elif modality == "PT":
                        ScanClass = xlogin.classes.PetScanData
                    elif modality == "CT":
                        ScanClass = xlogin.classes.CtScanData
                    else:
                        upload_errors = True
                        logger.error(
                            f"Unsupported modality for {scan_id} in {session_dir}: {modality}"
                        )
                xscan = ScanClass(
                    id=scan_id, type=scan_id, parent=xsession
                )
                for resource_dir in scan_dir.iterdir():
                    if not resource_dir.is_dir():
                        continue
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
                # Extract DICOM metadata
                xlogin.put(f"/data/experiments/{xsession.id}?pullDataFromHeaders=true")
                xlogin.put(f"/data/experiments/{xsession.id}?fixScanTypes=true")
                xlogin.put(f"/data/experiments/{xsession.id}?triggerPipelines=true")

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

