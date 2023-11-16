import re
import logging
import traceback
from collections import Counter
from pathlib import Path
import sys
import typing as ty
import hashlib
import pydicom
from fileformats.core import from_mime
from fileformats.core import FileSet

logger = logging.getLogger("xnat-ingest")


class LogEmail:
    def __init__(self, address, loglevel, subject):
        self.address = address
        self.loglevel = loglevel
        self.subject = subject

    @classmethod
    def split_envvar_value(cls, envvar):
        return [cls(*entry.split(",")) for entry in envvar.split(";")]

    def __str__(self):
        return self.address


class LogFile:
    def __init__(self, path, loglevel):
        self.path = Path(path)
        self.loglevel = loglevel

    @classmethod
    def split_envvar_value(cls, envvar):
        return [cls(*entry.split(",")) for entry in envvar.split(";")]

    def __str__(self):
        return str(self.path)

    def __fspath__(self):
        return self.path


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


def set_logger_handling(
    log_level: str, log_emails: LogEmail, log_file: LogFile, mail_server: MailServer
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
            smtp_hdle = logging.handlers.SMTPHandler(
                mailhost=mail_server.host,
                fromaddr=mail_server.sender_email,
                toaddrs=[log_email.address],
                subject=log_email.subject,
                credentials=(mail_server.user, mail_server.password),
                secure=None,
            )
            smtp_hdle.setLevel(getattr(logging, log_email.loglevel.upper()))
            logger.addHandler(smtp_hdle)

    # Configure the file logger
    if log_file is not None:
        log_file.path.parent.mkdir(exist_ok=True)
        log_file_hdle = logging.FileHandler(log_file)
        log_file_hdle.setLevel(getattr(logging, log_file.loglevel.upper()))
        log_file_hdle.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(log_file_hdle)

    console_hdle = logging.StreamHandler(sys.stdout)
    console_hdle.setLevel(getattr(logging, log_level.upper()))
    console_hdle.setFormatter(
        logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    )
    logger.addHandler(console_hdle)


def get_checksums(xresource) -> ty.Dict[str, str]:
    """
    Downloads the MD5 digests associated with the files in a resource.

    Parameters
    ----------
    xresource : xnat.classes.Resource
        XNAT resource to retrieve the checksums from

    Returns
    -------
    dict[str, str]
        the checksums calculated by XNAT
    """
    result = xresource.xnat_session.get(xresource.uri + "/files")
    if result.status_code != 200:
        raise RuntimeError(
            "Could not download metadata for resource {}. Files "
            "may have been uploaded but cannot check checksums".format(xresource.id)
        )
    return dict((r["Name"], r["digest"]) for r in result.json()["ResultSet"]["Result"])


def calculate_checksums(scan: FileSet) -> ty.Dict[str, str]:
    """
    Calculates the MD5 digests associated with the files in a fileset.

    Parameters
    ----------
    scan : FileSet
        the file-set to calculate the checksums for

    Returns
    -------
    dict[str, str]
        the calculated checksums
    """
    checksums = {}
    for fspath in scan.fspaths:
        try:
            hsh = hashlib.md5()
            with open(fspath, "rb") as f:
                for chunk in iter(lambda: f.read(HASH_CHUNK_SIZE), b""):
                    hsh.update(chunk)
            checksum = hsh.hexdigest()
        except OSError:
            raise RuntimeError(f"Could not create digest of '{fspath}' ")
        checksums[str(fspath.relative_to(scan.parent))] = checksum
    return checksums


HASH_CHUNK_SIZE = 2**20


def show_cli_trace(result):
    """Show the exception traceback from CLIRunner results"""
    return "".join(traceback.format_exception(*result.exc_info))


class RegexExtractor:
    """Helper callable for extracting a substring from a string with a predefined pattern"""

    def __init__(self, regex: str):
        self.regex = re.compile(regex)

    def __call__(self, to_match: str) -> str:
        match = self.regex.match(to_match)
        if not match:
            raise RuntimeError(
                f"'{to_match}' did not match regular expression '{self.regex}'"
            )
        try:
            extracted = match.group("extract")
        except KeyError:
            if len(match.groups()) != 1:
                raise RuntimeError(
                    f"'{to_match}' did not match any groups in regular expression "
                    f"'{self.regex}'"
                )
            extracted = list(match.groups())[0]
        return extracted


def add_exc_note(e, note):
    """Adds a note to an exception in a Python <3.11 compatible way

    Parameters
    ----------
    e : Exception
        the exception to add the note to
    note : str
        the note to add

    Returns
    -------
    Exception
        returns the exception again
    """
    if hasattr(e, "add_note"):
        e.add_note(note)
    else:
        e.args = (e.args[0] + "\n" + note,)
    return e


def transform_paths(
    fspaths: list[Path],
    glob_pattern: str,
    old_values: dict[str, str],
    new_values: dict[str, str],
) -> list[Path]:
    """Applys the transforms FS paths matching `glob_pattern` by replacing the template values
    found in the `old_values` dict to the values in `new_values`. Used to strip any identifying
    information from file names before they are uploaded by replacing it with values from the
    de-identified metadata.

    Parameters
    ----------
    fspaths : list[Path]
        the file path to be transformed
    glob_pattern : str
        The glob-pattern, which was used to match `fspath`
    old_values : dict[str, str]
        the values used to parameterise the existing file paths
    new_values : dict[str, str]
        the new values to parameterise the transformed file paths

    Returns
    -------
    transformed : list[Path]
        the transformed paths
    """
    # Convert glob-syntax to equivalent regex
    expr = glob_to_re(glob_pattern)
    expr = expr.replace(r"\{", "{")
    expr = expr.replace(r"\}", "}")
    templ_attr_re = re.compile(r"\{([\w\.]+)\\\.([^\}]+)\}")
    while templ_attr_re.findall(expr):
        expr = templ_attr_re.sub(r"{\1.\2}", expr)

    group_count = Counter()

    # Create regex groups for string template args
    def str_templ_to_regex_group(match) -> str:
        fieldname = match.group(0)[1:-1]
        if "." in fieldname:
            fieldname, attr_name = fieldname.split(".")
        else:
            attr_name = ""
        groupname = fieldname
        old_val = old_values[fieldname]
        if attr_name:
            groupname += "__" + attr_name
            old_val = getattr(old_val, attr_name)
        groupname += "__" + str(group_count[fieldname])
        group_str = f"(?P<{groupname}>{old_val})"
        group_count[fieldname] += 1
        return group_str

    transform_path_pattern = _str_templ_replacement.sub(str_templ_to_regex_group, expr)
    transform_path_re = re.compile(transform_path_pattern + "$")

    # Define a custom replacement function
    def replace_named_groups(match):
        return new_values.get(match.lastgroup, match.group())

    transformed = []
    for fspath in fspaths:
        fspath_str = str(fspath)
        match = transform_path_re.match((str(fspath)))
        assert match
        prev_index = 0
        new_fspath = ""
        for groupname, group in match.groupdict().items():
            fieldname, remaining = groupname.split("__", maxsplit=1)
            if "__" in remaining:
                attr_name = remaining.split("__")[0]
            else:
                attr_name = ""
            match_start = match.start(groupname)
            match_end = match.end(groupname)
            new_fspath += fspath_str[prev_index:match_start]
            new_val = new_values[fieldname]
            if attr_name:
                new_val = getattr(new_val, attr_name)
            new_fspath += new_val
            prev_index = match_end
        new_fspath += fspath_str[match_end:]
        stripped_fspath = None
        strip_start_re = re.compile(r"^[\._\-]+")
        strip_end_re = re.compile(r"[\._\-]+$")
        for part in Path(new_fspath).parts:
            part = strip_start_re.sub("", part)
            part = strip_end_re.sub("", part)
            if stripped_fspath is None:
                stripped_fspath = Path(part)
            else:
                stripped_fspath /= part
        new_fspath = stripped_fspath
        # Use re.sub() with the custom replacement function
        transformed.append(Path(new_fspath))
    return transformed


# Taken from StackOverflow answer https://stackoverflow.com/a/63212852
def glob_to_re(glob_pattern: str) -> str:
    return _escaped_glob_replacement.sub(
        lambda match: _escaped_glob_tokens_to_re[match.group(0)],
        re.escape(glob_pattern),
    )


_escaped_glob_tokens_to_re = dict(
    (
        # Order of ``**/`` and ``/**`` in RE tokenization pattern doesn't matter because
        # ``**/`` will be caught first no matter what, making ``/**`` the only option later on.
        # W/o leading or trailing ``/`` two consecutive asterisks will be treated as literals.
        # Edge-case #1. Catches recursive globs in the middle of path. Requires edge
        # case #2 handled after this case.
        ("/\*\*", "(?:/.+?)*"),
        # Edge-case #2. Catches recursive globs at the start of path. Requires edge
        # case #1 handled before this case. ``^`` is used to ensure proper location for ``**/``.
        ("\*\*/", "(?:^.+?/)*"),
        # ``[^/]*`` is used to ensure that ``*`` won't match subdirs, as with naive
        # ``.*?`` solution.
        ("\*", "[^/]*"),
        ("\?", "."),
        ("\[\*\]", "\*"),  # Escaped special glob character.
        ("\[\?\]", "\?"),  # Escaped special glob character.
        # Requires ordered dict, so that ``\[!`` preceded ``\[`` in RE pattern. Needed
        # mostly to differentiate between ``!`` used within character class ``[]`` and
        # outside of it, to avoid faulty conversion.
        ("\[!", "[^"),
        ("\[", "["),
        ("\]", "]"),
    )
)

_escaped_glob_replacement = re.compile(
    "(%s)" % "|".join(_escaped_glob_tokens_to_re).replace("\\", "\\\\\\")
)

_str_templ_replacement = re.compile(r"\{[\w\.]+\}")
