import re
import logging
import traceback
from collections import Counter
from pathlib import Path
import sys
import typing as ty
import itertools
import attrs
import click.types
import click.testing
import discord
from fileformats.core import DataType, FileSet, from_mime


logger = logging.getLogger("xnat-ingest")


def datatype_converter(
    datatype_str: ty.Union[str, ty.Type[DataType]],
) -> ty.Type[DataType]:
    if isinstance(datatype_str, str):
        return from_mime(datatype_str)
    return datatype_str


class classproperty(object):
    def __init__(self, f: ty.Callable[..., ty.Any]) -> None:
        self.f = f

    def __get__(self, obj: object, owner: ty.Any) -> ty.Any:
        return self.f(owner)


class CliType(click.types.ParamType):

    is_composite = True

    def __init__(
        self,
        type_: ty.Type[ty.Union["CliTyped", "MultiCliTyped"]],
        multiple: bool = False,
    ):
        self.type = type_
        self.multiple = multiple

    def convert(
        self, value: ty.Any, param: click.Parameter | None, ctx: click.Context | None
    ) -> ty.Any:
        if isinstance(value, self.type):
            return value
        if len(attrs.fields(self.type)) == 1:
            return self.type(value)
        return self.type(*value)

    @property
    def arity(self) -> int:  # type: ignore[override]
        return len(attrs.fields(self.type))

    @property
    def name(self) -> str:  # type: ignore[override]
        return type(self).__name__.lower()

    def split_envvar_value(self, envvar: str) -> ty.Any:
        if self.multiple:
            return list(
                itertools.chain(
                    *(
                        entry.split(",", maxsplit=self.arity - 1)
                        for entry in envvar.split(";")
                    )
                )
            )
        else:
            return envvar.split(",", maxsplit=self.arity - 1)


@attrs.define
class CliTyped:

    @classproperty
    def cli_type(cls) -> CliType:
        return CliType(cls)  # type: ignore[arg-type]


@attrs.define
class MultiCliTyped:

    @classproperty
    def cli_type(cls) -> CliType:
        return CliType(cls, multiple=True)  # type: ignore[arg-type]


def to_upper(value: str) -> str:
    return value.upper()


@attrs.define
class LoggerConfig(MultiCliTyped):

    type: str
    loglevel: str
    location: str

    @property
    def loglevel_int(self) -> int:
        return getattr(logging, self.loglevel.upper())  # type: ignore[no-any-return]


@attrs.define
class UploadMethod(MultiCliTyped):

    method: str = attrs.field(
        validator=attrs.validators.in_(
            {"per_file", "tar_memory", "tgz_memory", "tar_file", "tgz_file"}
        )
    )
    datatype: ty.Type[FileSet] = attrs.field(converter=datatype_converter)


@attrs.define
class AssociatedFiles(CliTyped):

    datatype: ty.Type[FileSet] = attrs.field(converter=datatype_converter)
    glob: str = attrs.field()
    identity_pattern: str = attrs.field()


@attrs.define
class XnatLogin(CliTyped):

    host: str
    user: str
    password: str


@attrs.define
class StoreCredentials(CliTyped):

    access_key: str
    access_secret: str


@attrs.define
class MimeType(str, MultiCliTyped):

    mime: str

    @property
    def datatype(self) -> ty.Type[FileSet]:
        return from_mime(self.mime)


def set_logger_handling(
    logger_configs: ty.Sequence[LoggerConfig],
    additional_loggers: ty.Sequence[str] = (),
) -> None:
    """Set up logging for the application"""

    if not logger_configs:
        logger_configs = [LoggerConfig("stream", "info", "stdout")]

    loggers = [logger]
    for log in additional_loggers:
        loggers.append(logging.getLogger(log))

    min_log_level = min(ll.loglevel_int for ll in logger_configs)

    for logr in loggers:
        logr.setLevel(min_log_level)

    # Configure the file logger
    for config in logger_configs:
        log_handle: logging.Handler
        if config.type == "file":
            Path(config.location).parent.mkdir(parents=True, exist_ok=True)
            log_handle = logging.FileHandler(config.location)
        elif config.type == "stream":
            stream = sys.stderr if config.location == "stderr" else sys.stdout
            log_handle = logging.StreamHandler(stream)
        elif config.type == "discord":
            log_handle = DiscordHandler(config.location)
        else:
            raise ValueError(f"Unknown logger type: {config.type}")
        log_handle.setLevel(config.loglevel_int)
        log_handle.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        for logr in loggers:
            logr.addHandler(log_handle)


def show_cli_trace(result: click.testing.Result) -> str:
    """Show the exception traceback from CLIRunner results"""
    assert result.exc_info is not None
    exc_type, exc, tb = result.exc_info
    return "".join(traceback.format_exception(exc_type, value=exc, tb=tb))


class DiscordHandler(logging.Handler):
    """A logging handler that sends log messages to a Discord webhook"""

    def __init__(self, webhook_url: str):
        super().__init__()
        self.webhook_url = webhook_url
        self.client = discord.Webhook.from_url(webhook_url)

    def emit(self, record: logging.LogRecord) -> None:
        self.client.send(record.msg)


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


def add_exc_note(e: Exception, note: str) -> Exception:
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
    spaces_to_underscores: bool = False,
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
    spaces_to_underscores: bool
        whether to replace spaces with underscores in the transformed paths

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

    group_count: Counter[str] = Counter()

    # Create regex groups for string template args
    def str_templ_to_regex_group(match: re.Match[str]) -> str:
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
        if spaces_to_underscores:
            old_val = old_val.replace(" ", "_")
        groupname += "__" + str(group_count[fieldname])
        group_str = f"(?P<{groupname}>{old_val})"
        group_count[fieldname] += 1
        return group_str

    transform_path_pattern = _str_templ_replacement.sub(str_templ_to_regex_group, expr)
    transform_path_re = re.compile(transform_path_pattern + "$")

    # Define a custom replacement function
    def replace_named_groups(match: re.Match[str]) -> str:
        assert match.lastgroup is not None
        return new_values.get(match.lastgroup, match.group())

    transformed = []
    for fspath in fspaths:
        fspath_str = str(fspath)
        match = transform_path_re.match(fspath_str)
        assert match
        prev_index = 0
        new_fspath = ""
        match_end = 0
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
        assert stripped_fspath is not None
        new_fspath = str(stripped_fspath)
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
        (r"/\*\*", "(?:/.+?)*"),
        # Edge-case #2. Catches recursive globs at the start of path. Requires edge
        # case #1 handled before this case. ``^`` is used to ensure proper location for ``**/``.
        (r"\*\*/", "(?:^.+?/)*"),
        # ``[^/]*`` is used to ensure that ``*`` won't match subdirs, as with naive
        # ``.*?`` solution.
        (r"\*", "[^/]*"),
        (r"\?", "."),
        (r"\[\*\]", r"\*"),  # Escaped special glob character.
        (r"\[\?\]", r"\?"),  # Escaped special glob character.
        # Requires ordered dict, so that ``\[!`` preceded ``\[`` in RE pattern. Needed
        # mostly to differentiate between ``!`` used within character class ``[]`` and
        # outside of it, to avoid faulty conversion.
        (r"\[!", "[^"),
        (r"\[", "["),
        (r"\]", "]"),
    )
)

_escaped_glob_replacement = re.compile(
    "(%s)" % "|".join(_escaped_glob_tokens_to_re).replace("\\", "\\\\\\")
)

_str_templ_replacement = re.compile(r"\{[\w\.]+\}")

invalid_path_chars_re = re.compile(r'[\-<>:"/\\|?*\x00-\x1F]')
