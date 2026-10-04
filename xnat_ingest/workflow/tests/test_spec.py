from pathlib import Path

import pytest
import yaml

from xnat_ingest.workflow.errors import WorkflowSpecError
from xnat_ingest.workflow.spec import load_spec


def _write(tmp_path: Path, name: str, content: dict) -> Path:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(content))
    return path


def _minimal_stages() -> list:
    return [
        {
            "name": "grp",
            "command": "group",
            "args": {"input_paths": ["/data/in"]},
        },
        {
            "name": "asn",
            "command": "assign",
            "input": "grp",
            "args": {
                "project": "StudyComments",
                "subject": "PatientID",
                "session": "AccessionNumber",
            },
        },
        {
            "name": "up",
            "command": "upload",
            "input": "asn",
            "args": {"server": "https://xnat.example.org"},
        },
    ]


def test_load_minimal_valid_spec(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "name": "acemid",
            "stages": _minimal_stages(),
        },
    )
    spec = load_spec(path)
    assert spec.name == "acemid"
    assert len(spec.stages) == 3
    assert spec.stages[2].args["server"] == "https://xnat.example.org"


def test_work_dir_top_level_field_rejected(tmp_path: Path) -> None:
    # work_dir is a --work-dir CLI/API runtime argument (see workflow.runner),
    # not part of the spec's own YAML/params - a literal 'work_dir:' at the top
    # level should be rejected the same as any other unknown field.
    path = _write(
        tmp_path,
        "spec.yaml",
        {"work_dir": "/some/path", "stages": _minimal_stages()},
    )
    with pytest.raises(WorkflowSpecError, match="unknown top-level"):
        load_spec(path)


def test_load_defaults_name_to_filename(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "my-pipeline.yaml",
        {
            "stages": [
                {
                    "name": "asn",
                    "command": "assign",
                    "args": {
                        "input_dir": "/x",
                        "project": "StudyComments",
                        "subject": "PatientID",
                        "session": "AccessionNumber",
                    },
                }
            ]
        },
    )
    spec = load_spec(path)
    assert spec.name == "my-pipeline"


def test_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(WorkflowSpecError, match="not found"):
        load_spec(tmp_path / "nonexistent.yaml")


def test_no_stages_raises(tmp_path: Path) -> None:
    path = _write(tmp_path, "spec.yaml", {"name": "x"})
    with pytest.raises(WorkflowSpecError, match="stages"):
        load_spec(path)


def test_stage_missing_name_raises(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        {"stages": [{"command": "assign", "args": {"input_dir": "/x"}}]},
    )
    with pytest.raises(WorkflowSpecError, match="missing required 'name'"):
        load_spec(path)


def test_stage_unknown_command_raises(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        {"stages": [{"name": "a", "command": "bogus", "args": {"input_dir": "/x"}}]},
    )
    with pytest.raises(WorkflowSpecError, match="unknown command 'bogus'"):
        load_spec(path)


def test_duplicate_stage_names_raise(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "stages": [
                {"name": "a", "command": "assign", "args": {"input_dir": "/x"}},
                {"name": "a", "command": "assign", "args": {"input_dir": "/y"}},
            ]
        },
    )
    with pytest.raises(WorkflowSpecError, match="duplicate stage name"):
        load_spec(path)


def test_unknown_input_reference_raises(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        {"stages": [{"name": "a", "command": "assign", "input": "nonexistent"}]},
    )
    with pytest.raises(WorkflowSpecError, match="unknown stage 'nonexistent'"):
        load_spec(path)


def test_dependency_cycle_raises(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "stages": [
                {"name": "a", "command": "assign", "input": "b"},
                {"name": "b", "command": "assign", "input": "a"},
            ]
        },
    )
    with pytest.raises(WorkflowSpecError, match="dependency cycle"):
        load_spec(path)


def test_missing_stage_input_raises(tmp_path: Path) -> None:
    path = _write(
        tmp_path, "spec.yaml", {"stages": [{"name": "a", "command": "assign"}]}
    )
    with pytest.raises(WorkflowSpecError, match="input_dir"):
        load_spec(path)


def test_upload_missing_server_raises(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "stages": [
                {"name": "u", "command": "upload", "args": {"input_dir": "/staged"}}
            ]
        },
    )
    with pytest.raises(WorkflowSpecError, match="needs 'server'"):
        load_spec(path)


def test_upload_with_server_does_not_flag_xnat_repo_as_missing(
    tmp_path: Path,
) -> None:
    # xnat_repo is injected by run_stage() at run time, not supplied via args: -
    # it must not be flagged as a missing required argument for upload().
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "stages": [
                {
                    "name": "u",
                    "command": "upload",
                    "args": {
                        "input_dir": "/staged",
                        "server": "https://xnat.example.org",
                    },
                }
            ]
        },
    )
    load_spec(path)  # must not raise


def test_missing_required_stage_arg_raises_clearly(tmp_path: Path) -> None:
    # group-orthanc's 'store_dir' has no default in group_orthanc() - omitting
    # it should be caught at check/load time, not just at actual run time.
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "stages": [
                {
                    "name": "g",
                    "command": "group-orthanc",
                    "args": {
                        "url": "https://orthanc.example.org",
                        "user": "u",
                        "password": "p",
                    },
                }
            ]
        },
    )
    with pytest.raises(WorkflowSpecError, match="store_dir"):
        load_spec(path)


def test_bad_composite_arg_raises_with_stage_context(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "stages": [
                {
                    "name": "grp",
                    "command": "group",
                    "args": {
                        "input_paths": ["/x"],
                        "on_resource_clash": [
                            {"policy": "bogus-policy", "scope": "all"}
                        ],
                    },
                }
            ]
        },
    )
    with pytest.raises(WorkflowSpecError, match="grp"):
        load_spec(path)


def test_unknown_arg_key_raises(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "stages": [
                {
                    "name": "grp",
                    "command": "group",
                    "args": {"input_paths": ["/x"], "not_a_real_arg": 123},
                }
            ]
        },
    )
    with pytest.raises(WorkflowSpecError, match="unknown argument"):
        load_spec(path)


def test_unknown_top_level_field_raises(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        {"stages": _minimal_stages(), "bogus_top_level": True},
    )
    with pytest.raises(WorkflowSpecError, match="unknown top-level"):
        load_spec(path)


# ── ${NAME} placeholder resolution: params: / --param / environment ──────────


def test_placeholder_resolves_from_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TEST_XNAT_PASSWORD", "sekret")
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "stages": [
                {
                    "name": "u",
                    "command": "upload",
                    "args": {
                        "input_dir": "/staged",
                        "server": "https://x",
                        "password": "${TEST_XNAT_PASSWORD}",
                    },
                }
            ]
        },
    )
    spec = load_spec(path)
    assert spec.stages[0].args["password"] == "sekret"


def test_undeclared_placeholder_undefined_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("TEST_UNDEFINED_VAR", raising=False)
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "stages": [
                {
                    "name": "u",
                    "command": "upload",
                    "args": {"server": "${TEST_UNDEFINED_VAR}"},
                }
            ]
        },
    )
    with pytest.raises(WorkflowSpecError, match="TEST_UNDEFINED_VAR"):
        load_spec(path)


def test_declared_param_default_used_when_unset(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "params": {
                "xnat_server": {
                    "default": "https://default-server",
                    "secret": True,
                }
            },
            "stages": [
                {
                    "name": "u",
                    "command": "upload",
                    "args": {"input_dir": "/staged", "server": "${xnat_server}"},
                }
            ],
        },
    )
    spec = load_spec(path)
    assert spec.stages[0].args["server"] == "https://default-server"
    assert spec.params["xnat_server"].required is False


def test_declared_param_required_without_default_raises_clear_error(
    tmp_path: Path,
) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "params": {
                "xnat_server": {
                    "description": "The XNAT server to upload to",
                    "secret": True,
                }
            },
            "stages": [
                {
                    "name": "u",
                    "command": "upload",
                    "args": {"input_dir": "/staged", "server": "${xnat_server}"},
                }
            ],
        },
    )
    with pytest.raises(WorkflowSpecError, match="xnat_server") as exc_info:
        load_spec(path)
    assert "The XNAT server to upload to" in str(exc_info.value)


def test_param_override_wins_over_default(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "params": {
                "xnat_server": {
                    "default": "https://default-server",
                    "secret": True,
                }
            },
            "stages": [
                {
                    "name": "u",
                    "command": "upload",
                    "args": {"input_dir": "/staged", "server": "${xnat_server}"},
                }
            ],
        },
    )
    spec = load_spec(path, param_overrides={"xnat_server": "https://overridden"})
    assert spec.stages[0].args["server"] == "https://overridden"


def test_param_override_wins_over_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XNAT_SERVER", "https://from-env")
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "params": {"XNAT_SERVER": {"secret": True}},
            "stages": [
                {
                    "name": "u",
                    "command": "upload",
                    "args": {"input_dir": "/staged", "server": "${XNAT_SERVER}"},
                }
            ],
        },
    )
    spec = load_spec(path, param_overrides={"XNAT_SERVER": "https://from-param"})
    assert spec.stages[0].args["server"] == "https://from-param"


def test_environment_wins_over_declared_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XNAT_SERVER", "https://from-env")
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "params": {
                "XNAT_SERVER": {"default": "https://from-default", "secret": True}
            },
            "stages": [
                {
                    "name": "u",
                    "command": "upload",
                    "args": {"input_dir": "/staged", "server": "${XNAT_SERVER}"},
                }
            ],
        },
    )
    spec = load_spec(path)
    assert spec.stages[0].args["server"] == "https://from-env"


def test_undeclared_placeholder_resolves_via_param_override(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "stages": [
                {
                    "name": "u",
                    "command": "upload",
                    "args": {
                        "input_dir": "/staged",
                        "server": "${SOME_UNDECLARED_NAME}",
                    },
                }
            ]
        },
    )
    spec = load_spec(
        path, param_overrides={"SOME_UNDECLARED_NAME": "https://works-anyway"}
    )
    assert spec.stages[0].args["server"] == "https://works-anyway"


def test_secret_param_parsed() -> None:
    from xnat_ingest.workflow.spec import ParamSpec

    p = ParamSpec(name="xnat_password", secret=True)
    assert p.required is True
    assert p.secret is True


# ── extends: merging params: across files ─────────────────────────────────


def test_extends_merges_shared_param_declarations(tmp_path: Path) -> None:
    common = _write(
        tmp_path,
        "common.yaml",
        {"params": {"xnat_user": {"default": "shared-user", "secret": True}}},
    )
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "extends": common.name,
            "stages": [
                {
                    "name": "u",
                    "command": "upload",
                    "args": {
                        "input_dir": "/staged",
                        "server": "https://x",
                        "user": "${xnat_user}",
                    },
                }
            ],
        },
    )
    spec = load_spec(path)
    assert spec.stages[0].args["user"] == "shared-user"


def test_extends_child_overrides_shared_param_default(tmp_path: Path) -> None:
    common = _write(
        tmp_path,
        "common.yaml",
        {"params": {"xnat_server": {"default": "https://parent", "secret": True}}},
    )
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "extends": common.name,
            "params": {"xnat_server": {"default": "https://child", "secret": True}},
            "stages": [
                {
                    "name": "u",
                    "command": "upload",
                    "args": {"input_dir": "/staged", "server": "${xnat_server}"},
                }
            ],
        },
    )
    spec = load_spec(path)
    assert spec.stages[0].args["server"] == "https://child"


def test_extends_circular_raises(tmp_path: Path) -> None:
    a = tmp_path / "a.yaml"
    b = tmp_path / "b.yaml"
    a.write_text(yaml.safe_dump({"extends": "b.yaml", "stages": _minimal_stages()}))
    b.write_text(yaml.safe_dump({"extends": "a.yaml", "stages": _minimal_stages()}))
    with pytest.raises(WorkflowSpecError, match="circular"):
        load_spec(a)


def test_disabled_stage_defaults_true(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "stages": [
                {
                    "name": "d",
                    "command": "deidentify",
                    "args": {
                        "input_dir": "/x",
                        "spec_dir": "/specs",
                        "reid_dir": "/reid",
                    },
                    "enabled": False,
                }
            ]
        },
    )
    spec = load_spec(path)
    assert spec.stages[0].enabled is False


def test_retries_parsed(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "stages": [
                {
                    "name": "a",
                    "command": "assign",
                    "args": {
                        "input_dir": "/x",
                        "project": "StudyComments",
                        "subject": "PatientID",
                        "session": "AccessionNumber",
                    },
                    "retries": 3,
                    "retry_delay_seconds": 5,
                }
            ]
        },
    )
    spec = load_spec(path)
    assert spec.stages[0].retries == 3
    assert spec.stages[0].retry_delay_seconds == 5.0


# ── deferred (Prefect-native) params: non-secret, plain-argument placeholders ──


def _upload_spec(params: dict, upload_args: dict) -> dict:
    return {
        "params": params,
        "stages": [
            {
                "name": "u",
                "command": "upload",
                "args": {"input_dir": "/staged", **upload_args},
            }
        ],
    }


def test_non_secret_plain_arg_is_deferred_not_resolved(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        _upload_spec(
            {"xnat_server": {"description": "server"}},
            {"server": "${xnat_server}"},
        ),
    )
    spec = load_spec(path)
    # left as the literal placeholder - substituted at actual flow-run time, not
    # baked in at load time.
    assert spec.stages[0].args["server"] == "${xnat_server}"
    assert "xnat_server" in spec.deferred_defaults


def test_deferred_param_missing_value_does_not_raise_at_load_time(
    tmp_path: Path,
) -> None:
    # unlike a secret/composite placeholder, a deferred param with no --param/env/
    # default doesn't block load_spec()/check - it's a required Prefect parameter,
    # left for 'serve'/'deploy' to ask for when actually triggered.
    path = _write(
        tmp_path,
        "spec.yaml",
        _upload_spec(
            {"xnat_server": {"description": "server"}},
            {"server": "${xnat_server}"},
        ),
    )
    spec = load_spec(path)
    from xnat_ingest.workflow.spec import NO_DEFAULT

    assert spec.deferred_defaults["xnat_server"] is NO_DEFAULT


def test_deferred_param_default_resolved_from_declared_default(
    tmp_path: Path,
) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        _upload_spec(
            {"xnat_server": {"default": "https://default-server"}},
            {"server": "${xnat_server}"},
        ),
    )
    spec = load_spec(path)
    assert spec.deferred_defaults["xnat_server"] == "https://default-server"


def test_deferred_param_default_resolved_from_param_override(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        _upload_spec(
            {"xnat_server": {"default": "https://default-server"}},
            {"server": "${xnat_server}"},
        ),
    )
    spec = load_spec(path, param_overrides={"xnat_server": "https://overridden"})
    assert spec.deferred_defaults["xnat_server"] == "https://overridden"


def test_deferred_param_default_resolved_from_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XNAT_SERVER_ENV_TEST", "https://from-env")
    path = _write(
        tmp_path,
        "spec.yaml",
        _upload_spec(
            {"XNAT_SERVER_ENV_TEST": {}},
            {"server": "${XNAT_SERVER_ENV_TEST}"},
        ),
    )
    spec = load_spec(path)
    assert spec.deferred_defaults["XNAT_SERVER_ENV_TEST"] == "https://from-env"


def test_composite_arg_placeholder_stays_eager_even_if_non_secret(
    tmp_path: Path,
) -> None:
    # a placeholder inside a *coerced* arg (group's 'datatypes' is parsed into
    # FileSet types) can't be deferred without losing check-time validation - it
    # always resolves eagerly, whatever the backing param's secret: flag.
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "params": {"the_datatype": {"default": "image/png"}},
            "stages": [
                {
                    "name": "g",
                    "command": "group",
                    "args": {
                        "input_paths": ["/data"],
                        "datatypes": ["${the_datatype}"],
                    },
                }
            ],
        },
    )
    spec = load_spec(path)
    assert spec.stages[0].args["datatypes"] == ["image/png"]
    assert spec.deferred_defaults == {}


def test_placeholder_embedded_in_larger_string_stays_eager(tmp_path: Path) -> None:
    # only a *whole-value* placeholder can be deferred; one embedded in a bigger
    # string is always resolved eagerly, even for a non-secret param in a plain arg.
    path = _write(
        tmp_path,
        "spec.yaml",
        _upload_spec(
            {"xnat_host": {"default": "xnat.example.org"}},
            {"server": "https://${xnat_host}/"},
        ),
    )
    spec = load_spec(path)
    assert spec.stages[0].args["server"] == "https://xnat.example.org/"
    assert spec.deferred_defaults == {}


def test_secret_param_in_plain_arg_stays_eager(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        _upload_spec(
            {"xnat_password": {"default": "hunter2", "secret": True}},
            {"server": "https://x", "password": "${xnat_password}"},
        ),
    )
    spec = load_spec(path)
    assert spec.stages[0].args["password"] == "hunter2"
    assert spec.deferred_defaults == {}


def test_deferred_placeholder_in_nested_list_still_deferred(tmp_path: Path) -> None:
    # input_paths is a list; the placeholder is one element of it, not the whole
    # arg value - deferral works at the leaf, not just the top-level arg value.
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "params": {"input_dir": {}},
            "stages": [
                {
                    "name": "g",
                    "command": "group",
                    "args": {"input_paths": ["${input_dir}"]},
                }
            ],
        },
    )
    spec = load_spec(path)
    assert spec.stages[0].args["input_paths"] == ["${input_dir}"]
    assert "input_dir" in spec.deferred_defaults


def test_check_dry_run_tolerates_deferred_param_with_no_value(
    tmp_path: Path,
) -> None:
    # a bad composite arg elsewhere in the *same* stage must still be caught, even
    # though this stage also has an unresolvable deferred param - the dry run
    # substitutes a dummy for the deferred one rather than failing on it.
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "params": {"input_dir": {}},
            "stages": [
                {
                    "name": "g",
                    "command": "group",
                    "args": {
                        "input_paths": ["${input_dir}"],
                        "datatypes": ["not-a-real-mime-type"],
                    },
                }
            ],
        },
    )
    with pytest.raises(WorkflowSpecError, match="not-a-real-mime-type"):
        load_spec(path)
