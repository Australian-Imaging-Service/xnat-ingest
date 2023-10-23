# - [ ] Email errors from upload script - emails configurable via env var
# - [ ] add delete option to upload script
# - [ ] add option to manually specify project/subject/session IDs in YAML file
# - [ ] handle partial re-uploads (override, merge, error), option in YAML file
# - [ ] Generalise to handle different types of raw data files
# - [ ] Send instructions to Dean/Fang on how it should be configured
# - [ ] Pull info from DICOM headers/OHIF viewer
from pathlib import Path
import shutil
import traceback
import time
import typing as ty
import hashlib
import logging.config
import logging.handlers
import click
from tqdm import tqdm
import pydicom
from fileformats.core import from_mime
from arcana.core.data.set import Dataset
from arcana.xnat import Xnat
from .base import cli
from ..session import DicomSession
from ..utils import logger, add_exc_note


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


class NonDicomType(str):

    def __init__(self, mime):
        self.type = from_mime(mime)

    @classmethod
    def split_envvar_value(cls, envvar):
        return [cls(entry) for entry in envvar.split(";")]


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

DICOMS_DIR is the directory that the session data has been exported to from the ICS console

STAGING_DIR is the directory that the files for each session are collated to before they
are uploaded to XNAT

SERVER is address of the XNAT server to upload the scans up to. Can alternatively provided
by setting the "XNAT_INGEST_HOST" environment variable.

USER is the XNAT user to connect with, alternatively the "XNAT_INGEST_USER" env. var

PASSWORD is the password for the XNAT user, alternatively "XNAT_INGEST_PASS" env. var
""",
)
@click.argument("dicoms_dir", type=click.Path(path_type=Path))
@click.argument("staging_dir", type=click.Path(path_type=Path))
@click.argument("server", type=str, envvar="XNAT_INGEST_HOST")
@click.argument("user", type=str, envvar="XNAT_INGEST_USER")
@click.argument("password", type=str, envvar="XNAT_INGEST_PASS")
@click.option(
    "--project-field",
    type=DicomField,
    default="StudyID",
    envvar="XNAT_INGEST_PROJECT",
    help=("The keyword or tag of the DICOM field to extract the XNAT project ID from "),
)
@click.option(
    "--subject-field",
    type=DicomField,
    default="PatientID",
    envvar="XNAT_INGEST_SUBJECT",
    help=("The keyword or tag of the DICOM field to extract the XNAT subject ID from "),
)
@click.option(
    "--session-field",
    type=DicomField,
    default="AccessionNumber",
    envvar="XNAT_INGEST_SESSION",
    help=(
        "The keyword or tag of the DICOM field to extract the XNAT imaging session ID from "
    ),
)
@click.option(
    "--non-dicoms-pattern",
    type=str,
    default=None,
    envvar="XNAT_INGEST_NONDICOMSPATTERN",
    help=(
        "Glob pattern by which to detect non-DICOM files that "
        "corresponding to DICOM sessions. Can contain string templates corresponding to "
        "DICOM metadata fields, which are substituted before the glob is called. For "
        "example, \"/path/to/non-dicoms/{PatientName.given_name}_{PatientName.family_name}/*)\" "
        "will find all files under the subdirectory within '/path/to/non-dicoms/' that matches "
        "<GIVEN-NAME>_<FAMILY-NAME>. Will be interpreted as being relative to `dicoms_dir` "
        "if a relative path is provided."
    ),
)
@click.option(
    "--dicom-ext",
    type=str,
    default=".dcm",
    envvar="XNAT_INGEST_DICOMEXT",
    help=("The extension of the DICOM files to look for"),
)
@click.option(
    "--delete/--dont-delete",
    default=False,
    envvar="XNAT_INGEST_DELETE",
    help="Whether to delete the session directories after they have been uploaded or not",
)
@click.option(
    "--log-file",
    default=None,
    type=click.Path(path_type=Path),
    envvar="XNAT_INGEST_LOGFILE",
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
    envvar="XNAT_INGEST_LOGEMAIL",
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
    envvar="XNAT_INGEST_MAILSERVER",
    help=(
        "the mail server to send logger emails to. When provided in an environment variable, "
        "args are delimited by ';'"
    ),
)
@click.option(
    "--staging-dir-name",
    default="XNAT_UPLOAD_STAGING",
    type=str,
    envvar="XNAT_INGEST_STAGINGDIRNAME",
    help=(
        "The name of the directory that files are staged in before they are uploaded to "
        "XNAT"
    ),
)
@click.option(
    "--exclude-dicoms/--include-dicoms",
    default=False,
    type=bool,
    envvar="XNAT_INGEST_EXCLUDEDICOM",
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
@click.option(
    "--logs-dir",
    envvar="XNAT_INGEST_LOGSDIR",
)
def upload(
    dicoms_dir: Path,
    staging_dir: Path,
    server: str,
    user: str,
    password: str,
    project_field: str,
    subject_field: str,
    session_field: str,
    non_dicoms_pattern: str,
    dicom_ext: str,
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
                "XNAT_INGEST_MAILSERVER environment variable if logger emails "
                "are provided: " + ", ".join(log_emails)
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
            dicoms_dir
            / "upload-logs"
            / (time.strftime("%Y%m%d", time.localtime()) + ".log")
        )
    log_file.parent.mkdir(exist_ok=True)
    log_file_hdle = logging.FileHandler(log_file)
    log_file_hdle.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(log_file_hdle)

    sessions = DicomSession.from_paths(
        dicom_paths=dicoms_dir.glob(f"**/*{dicom_ext}"),
        project_field=project_field,
        subject_field=subject_field,
        session_field=session_field,
    )

    xnat_repo = Xnat(server=server, user=user, password=password)

    with xnat_repo:
        for session in tqdm(
            sessions, f"Processing DICOM sessions found in {dicoms_dir}"
        ):
            try:
                session_staging_dir = staging_dir / session.name
                session_staging_dir.mkdir(exist_ok=True)
                spec_file = session_staging_dir / "UPLOAD-SPECIFICATION.yaml"
                if spec_file.exists():
                    try:
                        session.override_ids(spec_file)
                    except Exception:
                        logger.error(
                            f"Could not load specification file from '{spec_file}', please "
                            f"correct it and try again, skipping session:\n"
                            + traceback.format_exc()
                        )
                        continue
                else:
                    logger.info(f"Did not find manual specification file at '{spec_file}'")

                # Anonymise DICOMs and save them to the staging directory
                dicom_staging_dir = session_staging_dir / "dicoms"
                staged_session = session.anonymise(dicom_staging_dir)

                # Create corresponding session on XNAT
                xproject = xnat_repo.connection.projects[session.project_id]
                xsubject = xnat_repo.connection.classes.SubjectData(
                    label=session.subject_id, parent=xproject
                )
                try:
                    xsession = xproject.experiments[session.session_id]
                except KeyError:
                    if "MR" in session.modalities:
                        SessionClass = xnat_repo.connection.classes.MrSessionData
                    elif "PT" in session.modalities:
                        SessionClass = xnat_repo.connection.classes.PetSessionData
                    elif "CT" in session.modalities:
                        SessionClass = xnat_repo.connection.classes.CtSessionData
                    else:
                        logger.error(
                            "Found the following unsupported modalities in "
                            f"{session.name}: {session.modalities}"
                        )
                        continue
                    xsession = SessionClass(label=session.session_id, parent=xsubject)
                session_path = (
                    f"{session.project_id}:{session.subject_id}:{session.session_id}"
                )

                # Access Arcana dataset associated with project
                try:
                    dataset = Dataset.load(session.project_id, xnat_repo)
                except Exception as e:
                    add_exc_note(
                        e,
                        f"Did not load dataset definition from {session.project_id} project "
                        f"on {server}. Please set one up using the Arcana command line tool "
                        "in order to check presence of required scans and associated "
                        "files (e.g. raw-data exports)")
                    raise e
                else:
                    if non_dicoms_pattern is not None:
                        non_dicom_paths = dicoms_dir.glob(
                            non_dicoms_pattern.format(**session.metadata))
                        non_dicom_staging_dir = session_staging_dir / "non-dicoms"
                        non_dicom_staging_dir.mkdir(exist_ok=True)
                    else:
                        non_dicom_paths = []

                    for column in dataset.columns:
                        pass

                # Anonymise DICOMs and save to directory prior to upload
                if exclude_dicoms:
                    logger.info("Omitting DICOMS as `--exclude-dicoms` is set")
                else:
                    logger.info(f"Uploading DICOMS from '{session.name}' to {session_path}")
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
                            ScanClass = xnat_repo.connection.classes.ScScanData
                        elif modality == "MR":
                            ScanClass = xnat_repo.connection.classes.MrScanData
                        elif modality == "PT":
                            ScanClass = xnat_repo.connection.classes.PetScanData
                        elif modality == "CT":
                            ScanClass = xnat_repo.connection.classes.CtScanData
                        else:
                            upload_errors = True
                            logger.error(
                                f"Unsupported modality for {scan_id} in {session_dir}: {modality}"
                            )
                    xscan = ScanClass(id=scan_id, type=scan_id, parent=xsession)
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
            except Exception as e:
                logger.error(f"Could not upload '{session.name}': {e}")


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
