from pathlib import Path

from fileformats.core import FileSet
from fileformats.image.raster import Jpeg, Png

from xnat_ingest.helpers.arg_types import ClashSpec, IDSpec
from xnat_ingest.workflow.stages import STAGES, StageContext


def _ctx(input_path=None, output_path=None, xnat=None) -> StageContext:  # type: ignore[no-untyped-def]
    return StageContext(input_path=input_path, output_path=output_path, xnat=xnat)


def test_group_kwargs_input_from_context() -> None:
    kwargs = STAGES["group"].build_kwargs(
        {}, _ctx(input_path=Path("/in"), output_path=Path("/out"))
    )
    assert kwargs["input_paths"] == ["/in"]
    assert kwargs["output_dir"] == Path("/out")


def test_group_kwargs_input_from_args_overrides_context() -> None:
    kwargs = STAGES["group"].build_kwargs(
        {"input_paths": ["/a", "/b"]}, _ctx(output_path=Path("/out"))
    )
    assert kwargs["input_paths"] == ["/a", "/b"]


def test_group_kwargs_composite_fields() -> None:
    kwargs = STAGES["group"].build_kwargs(
        {
            "datatypes": ["image/png", "image/jpeg"],
            "session": "{subject_uid}",
            "scan": [{"specifier": "dermoscopy-{LesionID}", "datatype": "image/png"}],
            "on_resource_clash": [{"policy": "merge", "scope": "image/png|image/jpeg"}],
            "collate_resources": [["image/png", "adjacent"]],
            "convert": [["image/png", "image/jpeg"]],
        },
        _ctx(input_path=Path("/in"), output_path=Path("/out")),
    )
    assert kwargs["datatypes"] == [Png, Jpeg]
    assert isinstance(kwargs["session"], list) and isinstance(
        kwargs["session"][0], IDSpec
    )
    assert kwargs["scan"][0].specifier == "dermoscopy-{LesionID}"
    assert isinstance(kwargs["on_resource_clash"][0], ClashSpec)
    assert kwargs["collation_map"][Png].name == "adjacent"
    assert kwargs["conversion_map"] == {Png: Jpeg}


def test_group_kwargs_plain_scalars_pass_through() -> None:
    kwargs = STAGES["group"].build_kwargs(
        {"recursive": True, "wait_period": 30},
        _ctx(input_path=Path("/in"), output_path=Path("/out")),
    )
    assert kwargs["recursive"] is True
    assert kwargs["wait_period"] == 30


def test_assign_kwargs_field_renames() -> None:
    kwargs = STAGES["assign"].build_kwargs(
        {
            "project": "StudyComments",
            "subject": "SubjectID",
            "session": "{SubjectID}_{CaptureDate:%Y%m%d}",
            "scan": "SeriesDescription",
            "constant_project_id": "ACEMID",
        },
        _ctx(input_path=Path("/in"), output_path=Path("/out")),
    )
    assert kwargs["input_dir"] == Path("/in")
    assert kwargs["output_dir"] == Path("/out")
    assert kwargs["project_field"] == "StudyComments"
    assert kwargs["subject_field"] == "SubjectID"
    assert kwargs["session_field"] == "{SubjectID}_{CaptureDate:%Y%m%d}"
    assert kwargs["scan_field"] == "SeriesDescription"
    assert kwargs["project_id"] == "ACEMID"
    assert "project" not in kwargs and "subject" not in kwargs


def test_assign_kwargs_include() -> None:
    kwargs = STAGES["assign"].build_kwargs(
        {"include": ["image/png"]},
        _ctx(input_path=Path("/in"), output_path=Path("/out")),
    )
    assert kwargs["include"] == [Png]


def test_deidentify_kwargs_reid_dir_optional() -> None:
    kwargs = STAGES["deidentify"].build_kwargs(
        {"spec_dir": "/specs"}, _ctx(input_path=Path("/in"), output_path=Path("/out"))
    )
    assert kwargs["spec_dir"] == Path("/specs")
    assert "reid_dir" not in kwargs  # untouched -> deidentify()'s own default (None)


def test_deidentify_kwargs_reid_dir_explicit() -> None:
    kwargs = STAGES["deidentify"].build_kwargs(
        {"spec_dir": "/specs", "reid_dir": "/reid"},
        _ctx(input_path=Path("/in"), output_path=Path("/out")),
    )
    assert kwargs["reid_dir"] == Path("/reid")


def test_deidentify_kwargs_reid_encrypt_key_encoded() -> None:
    kwargs = STAGES["deidentify"].build_kwargs(
        {"reid_encrypt_key": "abc123"},
        _ctx(input_path=Path("/in"), output_path=Path("/out")),
    )
    assert kwargs["reid_encrypt_key"] == b"abc123"


def test_associate_kwargs_datatype() -> None:
    kwargs = STAGES["associate"].build_kwargs(
        {"datatype": "image/png", "glob": "*.png", "identity_pattern": r"(\w+)"},
        _ctx(input_path=Path("/in"), output_path=Path("/out")),
    )
    assert kwargs["datatype"] is Png
    assert kwargs["glob"] == "*.png"


def test_upload_kwargs_no_repo_built_here() -> None:
    kwargs = STAGES["upload"].build_kwargs(
        {"always_include": ["all"]}, _ctx(input_path=Path("/staged"))
    )
    assert kwargs["input_dir"] == "/staged"
    assert kwargs["always_include"] == ["all"]
    assert "xnat_repo" not in kwargs  # injected by run_stage(), not build_kwargs()


def test_copy_mode_string_conversion() -> None:
    kwargs = STAGES["group"].build_kwargs(
        {"copy_mode": "copy"}, _ctx(input_path=Path("/in"), output_path=Path("/out"))
    )
    assert kwargs["copy_mode"] == FileSet.CopyMode.copy
