import re
import logging
import traceback
import difflib

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


def pattern_replacement(
    fspaths: list[str], source_pattern: str, target_pattern: str
) -> list[str]:
    """Applys the transform from the `source` to `target` glob-like patterns and applies
    it to the `to_transform` string provided

    Parameters
    ----------
    fspaths : str
        the file path to be transformed
    source_pattern : str
        source (glob-like) pattern, which was used to match `fspath`
    target_pattern : str
        target (glob-like) pattern, which the transformed fspath should match

    Returns
    -------
    transformed : str
        the transformed paths
    """
    chardiffs = difflib.ndiff(source_pattern, target_pattern)
    sections: list[str | tuple[str, str]] = []
    word = ""
    op = None
    addition = ""
    for diff in chardiffs:
        new_op = diff[0]
        ch = diff[2]
        # If contiguous with previous operation append onto the current word
        if new_op == op:
            word += ch
        # If the operation is different from the previous append the word to the sections
        else:
            if op == " ":
                sections.append(word)
            elif op == "+":
                if new_op == " ":
                    sections.append((word, ""))
                elif new_op == "-":
                    addition = word
                else:
                    assert False, f"Unrecognised op '{op}"
            elif op == "-":
                assert new_op == " "
                sections.append((addition, word))
                addition = ""
            else:
                assert False, f"Unrecognised op '{op}"
            word = ch
    return transformed


# Taken from StackOverflow answer https://stackoverflow.com/a/63212852

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


def glob_to_re(pattern):
    return _escaped_glob_replacement.sub(
        lambda match: _escaped_glob_tokens_to_re[match.group(0)], re.escape(pattern)
    )
