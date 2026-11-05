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
        {"name": "up", "command": "upload", "input": "asn"},
    ]


def test_load_minimal_valid_spec(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "name": "acemid",
            "work_dir": str(tmp_path / "work"),
            "xnat": {
                "server": "https://xnat.example.org",
                "user": "u",
                "password": "p",
            },
            "stages": _minimal_stages(),
        },
    )
    spec = load_spec(path)
    assert spec.name == "acemid"
    assert len(spec.stages) == 3
    assert spec.xnat is not None and spec.xnat.server == "https://xnat.example.org"
    assert spec.work_dir == tmp_path / "work"


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


def test_upload_without_xnat_block_raises(tmp_path: Path) -> None:
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "stages": [
                {"name": "u", "command": "upload", "args": {"input_dir": "/staged"}}
            ]
        },
    )
    with pytest.raises(WorkflowSpecError, match="'xnat:' block"):
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


def test_env_var_interpolation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_XNAT_PASSWORD", "sekret")
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "xnat": {
                "server": "https://x",
                "user": "u",
                "password": "${TEST_XNAT_PASSWORD}",
            },
            "stages": [
                {"name": "u", "command": "upload", "args": {"input_dir": "/staged"}}
            ],
        },
    )
    spec = load_spec(path)
    assert spec.xnat is not None
    assert spec.xnat.password == "sekret"


def test_env_var_undefined_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("TEST_UNDEFINED_VAR", raising=False)
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "xnat": {"server": "https://x", "password": "${TEST_UNDEFINED_VAR}"},
            "stages": [
                {"name": "u", "command": "upload", "args": {"input_dir": "/staged"}}
            ],
        },
    )
    with pytest.raises(WorkflowSpecError, match="TEST_UNDEFINED_VAR"):
        load_spec(path)


def test_extends_merges_common_config(tmp_path: Path) -> None:
    common = _write(
        tmp_path,
        "common.yaml",
        {"xnat": {"server": "https://x", "user": "shared-user", "password": "p"}},
    )
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "extends": common.name,
            "stages": [
                {"name": "u", "command": "upload", "args": {"input_dir": "/staged"}}
            ],
        },
    )
    spec = load_spec(path)
    assert spec.xnat is not None
    assert spec.xnat.user == "shared-user"


def test_extends_child_overrides_parent(tmp_path: Path) -> None:
    common = _write(
        tmp_path,
        "common.yaml",
        {"xnat": {"server": "https://parent", "user": "u", "password": "p"}},
    )
    path = _write(
        tmp_path,
        "spec.yaml",
        {
            "extends": common.name,
            "xnat": {"server": "https://child"},
            "stages": [
                {"name": "u", "command": "upload", "args": {"input_dir": "/staged"}}
            ],
        },
    )
    spec = load_spec(path)
    assert spec.xnat is not None
    assert spec.xnat.server == "https://child"
    assert spec.xnat.user == "u"  # inherited from parent, not overridden


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
