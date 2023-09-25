import re
import logging
import traceback

logger = logging.getLogger("xnat-upload-exported-scans")
logger.setLevel(logging.INFO)


def show_cli_trace(result):
    """Show the exception traceback from CLIRunner results"""
    return "".join(traceback.format_exception(*result.exc_info))


class RegexExtractor:
    """Helper callable for extracting a substring from a string with a predefined pattern
    """

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
