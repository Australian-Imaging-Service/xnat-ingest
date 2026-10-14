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
        {"name": "asn", "command": "assign", "input": "grp"},
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
        {"stages": [{"name": "asn", "command": "assign", "args": {"input_dir": "/x"}}]},
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
            "params": {"xnat_server": {"default": "https://default-server"}},
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
            "params": {"xnat_server": {"description": "The XNAT server to upload to"}},
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
            "params": {"xnat_server": {"default": "https://default-server"}},
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
            "params": {"XNAT_SERVER": {}},
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
            "params": {"XNAT_SERVER": {"default": "https://from-default"}},
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
        {"params": {"xnat_user": {"default": "shared-user"}}},
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
        {"params": {"xnat_server": {"default": "https://parent"}}},
    )
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "extends": common.name,
            "params": {"xnat_server": {"default": "https://child"}},
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
                    "args": {"input_dir": "/x", "spec_dir": "/specs"},
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
                    "args": {"input_dir": "/x"},
                    "retries": 3,
                    "retry_delay_seconds": 5,
                }
            ]
        },
    )
    spec = load_spec(path)
    assert spec.stages[0].retries == 3
    assert spec.stages[0].retry_delay_seconds == 5.0
