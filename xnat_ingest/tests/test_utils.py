import re

from xnat_ingest.utils import glob_to_re, transform_paths


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
    old_values = {"first": "FIRST", "second": "SECOND"}
    new_values = {"first": "1st", "second": "2nd"}
    paths_globs = (
        (
            "baz/duck/bar/FIRST_SECOND/quack/FIRSTSECOND.dat",
            "**/bar/**/{first}_*/**/{first}{second}.dat",
            "baz/duck/bar/1st_SECOND/quack/1st2nd.dat",
        ),
        ("bar/FIRST.py", "**/{first}.py", "bar/1st.py"),
        ("bar/baz/SECOND.py", "bar/**/{second}.py", "bar/baz/2nd.py"),
        (
            "bar/baz/SECOND/wut/foo.py",
            "bar/**/{second}/**/foo.py",
            "bar/baz/2nd/wut/foo.py",
        ),
    )

    for paths, glb, transformed in paths_globs:
        assert (
            str(transform_paths([paths], glb, old_values, new_values)[0]) == transformed
        )
        assert (
            str(transform_paths([paths], glb, old_values, new_values)[0]) == transformed
        )
