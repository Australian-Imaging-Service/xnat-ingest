import re
import logging
import traceback
from collections import Counter
from pathlib import Path


logger = logging.getLogger("xnat-upload-exported-scans")
logger.setLevel(logging.INFO)


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
    fspaths: list[Path], glob_pattern: str, old_values: dict[str, str], new_values: dict[str, str]
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
    expr = expr.replace(r'\{', '{')
    expr = expr.replace(r'\}', '}')

    group_count = Counter()

    # Create regex groups for string template args
    def str_templ_to_regex_group(match) -> str:
        fieldname = match.group(0)[1:-1]
        groupname = fieldname + "__" + str(group_count[fieldname])
        group_str = f"(?P<{groupname}>{old_values[fieldname]})"
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
            fieldname = groupname.split('__')[0]
            match_start = match.start(groupname)
            match_end = match.end(groupname)
            new_fspath += fspath_str[prev_index:match_start]
            new_fspath += new_values[fieldname]
            prev_index = match_end
        new_fspath += fspath_str[match_end:]
        # Use re.sub() with the custom replacement function
        transformed.append(Path(new_fspath))
    return transformed


# Taken from StackOverflow answer https://stackoverflow.com/a/63212852
def glob_to_re(glob_pattern: str) -> str:
    return _escaped_glob_replacement.sub(
        lambda match: _escaped_glob_tokens_to_re[match.group(0)], re.escape(glob_pattern)
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

_str_templ_replacement = re.compile(r"\{\w+\}")
