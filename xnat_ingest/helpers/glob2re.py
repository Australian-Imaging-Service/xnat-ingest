"""Helper functions for transforming file paths matching a glob pattern by replacing template values
with new values from de-identified metadata. This is used to strip identifying information from file
names before they are uploaded to XNAT."""

import re
from collections import Counter
from pathlib import Path


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
