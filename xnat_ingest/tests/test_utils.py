import re
from xnat_ingest.utils import glob_to_re, pattern_replacement


def test_glob_to_re():
    validity_paths_globs = (
        (True, "foo.py", "foo.py"),
        (True, "foo.py", "fo[o].py"),
        (True, "fob.py", "fo[!o].py"),
        (True, "*foo.py", "[*]foo.py"),
        (True, "foo.py", "**/foo.py"),
        (True, "baz/duck/bar/bam/quack/foo.py", "**/bar/**/foo.py"),
        (True, "bar/foo.py", "**/foo.py"),
        (True, "bar/baz/foo.py", "bar/**"),
        (False, "bar/baz/foo.py", "bar/*"),
        (False, "bar/baz/foo.py", "bar**/foo.py"),
        (True, "bar/baz/foo.py", "bar/**/foo.py"),
        (True, "bar/baz/wut/foo.py", "bar/**/foo.py"),
    )

    for validity, path, glb in validity_paths_globs:
        regex = glob_to_re(glb)
        assert bool(re.match(regex + "$", path)) is validity


def test_pattern_replacement():
    replacements = {"first": ("FIRST", "1st"), "second": ("SECOND", "2nd")}
    paths_globs = (
        (
            "**/bar/**/{first}{second}.dat",
            "baz/duck/bar/FIRST_SECOND/quack/FIRSTSECOND.dat",
            "baz/duck/bar/FIRST_SECOND/quack/1st2nd.dat",
        ),
        # ('bar/foo.py', '**/foo.py'),
        # ('bar/baz/foo.py', 'bar/**/foo.py'),
        # ('bar/baz/wut/foo.py', 'bar/**/foo.py'),
    )

    for glb, path, replaced in paths_globs:
        regex = pattern_replacement(glb, )
        assert re.match(regex + "$", path)
