from fileformats.core import FileSet
from fileformats.image.raster import Jpeg, Png

from xnat_ingest.helpers.arg_types import ClashSpec, IDSpec, PathMetadataRegex
from xnat_ingest.workflow import coerce


def test_construct_from_mapping() -> None:
    spec = coerce.construct(IDSpec, {"specifier": "SeriesNumber", "datatype": "all"})
    assert isinstance(spec, IDSpec)
    assert spec.specifier == "SeriesNumber"


def test_construct_from_list() -> None:
    spec = coerce.construct(ClashSpec, ["merge", "image/png"])
    assert spec.policy == "merge"
    assert spec.scope is Png


def test_construct_from_scalar() -> None:
    spec = coerce.construct(IDSpec, "SeriesNumber")
    assert spec.specifier == "SeriesNumber"
    assert spec.datatype is FileSet  # IDSpec's default datatype


def test_construct_passes_through_existing_instance() -> None:
    original = IDSpec("SeriesNumber")
    assert coerce.construct(IDSpec, original) is original


def test_datatype() -> None:
    assert coerce.datatype("image/png") is Png
    assert coerce.datatype("all").__name__ == "FileSet"


def test_datatypes_empty() -> None:
    assert coerce.datatypes(None) == []
    assert coerce.datatypes([]) == []


def test_datatypes_list() -> None:
    assert coerce.datatypes(["image/png", "image/jpeg"]) == [Png, Jpeg]


def test_id_specs_bare_string() -> None:
    specs = coerce.id_specs("SeriesNumber")
    assert len(specs) == 1
    assert specs[0].specifier == "SeriesNumber"


def test_id_specs_bare_mapping() -> None:
    specs = coerce.id_specs({"specifier": "SeriesNumber", "datatype": "all"})
    assert len(specs) == 1


def test_id_specs_list_of_mappings() -> None:
    specs = coerce.id_specs(
        [
            {"specifier": "dermoscopy-{LesionID}", "datatype": "image/png|image/jpeg"},
            {"specifier": "dexi-{CaptureTime}", "datatype": "image/png"},
        ]
    )
    assert len(specs) == 2
    assert specs[1].datatype is Png


def test_id_specs_empty() -> None:
    assert coerce.id_specs(None) == []
    assert coerce.id_specs([]) == []


def test_path_metadata_regexes() -> None:
    regexes = coerce.path_metadata_regexes(
        [{"regex": r".*/(?P<subject_uid>[\w-]+)", "datatype": "image/png"}]
    )
    assert len(regexes) == 1
    assert isinstance(regexes[0], PathMetadataRegex)
    assert regexes[0].datatype is Png


def test_on_resource_clash_default() -> None:
    assert coerce.on_resource_clash(None) == "error"


def test_on_resource_clash_bare_string() -> None:
    assert coerce.on_resource_clash("avoid") == "avoid"


def test_on_resource_clash_scoped_list() -> None:
    specs = coerce.on_resource_clash(
        [{"policy": "merge", "scope": "image/png|image/jpeg"}]
    )
    assert isinstance(specs, list)
    assert isinstance(specs[0], ClashSpec)
    assert specs[0].policy == "merge"


def test_metadata_tables_joins_mapping_alias(tmp_path) -> None:  # type: ignore[no-untyped-def]
    csv_path = tmp_path / "table.csv"
    csv_path.write_text("subject_uid,ImagePath\nsubj-01,foo.png\n")
    tables = coerce.metadata_tables(
        [
            {
                "path": str(csv_path),
                "row_frequency": "fileset[image/png]",
                "joins": {"ImagePath": '=HYPERLINK("{subject_uid}")'},
            }
        ]
    )
    assert len(tables) == 1
    assert tables[0].join_exprs[0].column_name == "ImagePath"


def test_collation_map(tmp_path) -> None:  # type: ignore[no-untyped-def]
    result = coerce.collation_map([{"mime": "image/png", "collation": "adjacent"}])
    assert result is not None
    assert list(result)[0] is Png


def test_collation_map_empty() -> None:
    assert coerce.collation_map(None) is None
    assert coerce.collation_map([]) is None


def test_conversion_map() -> None:
    result = coerce.conversion_map([["image/png", "image/jpeg"]])
    assert result == {Png: Jpeg}


def test_conversion_map_empty() -> None:
    assert coerce.conversion_map(None) is None


def test_copy_mode_string() -> None:
    from fileformats.core import FileSet

    assert coerce.copy_mode("copy") == FileSet.CopyMode.copy


def test_store_credentials_none() -> None:
    assert coerce.store_credentials(None) is None


def test_store_credentials_mapping() -> None:
    creds = coerce.store_credentials({"access_key": "AKIA", "access_secret": "secret"})
    assert creds is not None
    assert creds.access_key == "AKIA"
