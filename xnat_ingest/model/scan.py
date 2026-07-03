import logging
import re
import typing as ty
from pathlib import Path

import attrs
from fileformats.core import FileSet
from typing_extensions import Self

from ..helpers.arg_types import AssociatedFiles
from ..helpers.metadata import Metadata
from .resource import ImagingResource

logger = logging.getLogger("xnat-ingest")


if ty.TYPE_CHECKING:
    from xnat_ingest.model.session import ImagingSession


def scan_type_converter(scan_type: str | None) -> str | None:
    "Ensure there aren't any special characters that aren't valid file/dir paths"
    if scan_type is None:
        return None
    return re.sub(r"[\"\*\/\:\<\>\?\\\|\+\,\.\;\=\[\]]+", "", scan_type)


def scan_resources_converter(
    resources: dict[str, ImagingResource | FileSet],
) -> ty.Dict[str, ImagingResource]:
    return {
        scan_type_converter(k): (
            v if isinstance(v, ImagingResource) else ImagingResource(k, v)
        )
        for k, v in resources.items()
    }


@attrs.define
class ImagingScan:
    """Representation of a scan to be uploaded to XNAT

    Parameters
    ----------
    id: str
        the ID of the scan on XNAT
    type: str | None
        the scan type/description
    resources : dict[str, ImagingResource]
        the list of the resources associated with the scan
    associated: AssociatedFiles, optional
        Secondary files that are associated with the given imaging scan
        (but may not have the necessary metadata to identify the scan itself)
    """

    id: str
    type: str = attrs.field(converter=scan_type_converter, default=None)
    resources: ty.Dict[str, ImagingResource] = attrs.field(
        factory=dict, converter=scan_resources_converter
    )
    metadata: Metadata = attrs.field(eq=False, repr=False, init=False)
    associated: AssociatedFiles | None = None
    # Back-ref to parent session
    session: "ImagingSession" = attrs.field(default=None, eq=False, repr=False)

    def __contains__(self, resource_name: str) -> bool:
        return resource_name in self.resources

    def __getitem__(self, resource_name: str) -> ImagingResource:
        return self.resources[resource_name]

    def __attrs_post_init__(self) -> None:
        for resource in self.resources.values():
            resource.scan = self

    def new_empty(self) -> Self:
        return type(self)(self.id, self.type)

    @metadata.default
    def _metadata_default(self) -> Metadata:
        return Metadata({}, self)

    def load_metadata(self):
        return Metadata.collate(r.metadata for r in self.resources.values())

    def save(
        self,
        dest_dir: Path,
        copy_mode: FileSet.CopyMode = FileSet.CopyMode.hardlink_or_copy,
        collation_map: dict[ty.Type[FileSet], FileSet.CopyCollation] | None = None,
    ) -> Self:
        # Ensure scan type is a valid directory name. A scan with no description set
        # (e.g. not yet resolved by 'assign') is saved as '<scan_id>.' (trailing dot,
        # no description) to still distinguish it from session-level resource dirs
        saved = self.new_empty()
        scan_dir = dest_dir / f"{self.id}.{self.type if self.type is not None else ''}"
        scan_dir.mkdir(parents=True, exist_ok=True)
        for resource in self.resources.values():
            saved_resource = resource.save(
                scan_dir, copy_mode=copy_mode, collation_map=collation_map
            )
            saved_resource.scan = saved
            saved.resources[saved_resource.name] = saved_resource
        self.metadata.save(scan_dir)
        return saved

    @classmethod
    def load(
        cls, scan_dir: Path, require_manifest: bool = True, check_checksums: bool = True
    ) -> Self:
        scan_id, scan_type = scan_dir.name.split(".", 1)
        scan = cls(scan_id, scan_type or None)
        for resource_dir in scan_dir.iterdir():
            if resource_dir.is_dir():
                resource = ImagingResource.load(
                    resource_dir,
                    require_manifest=require_manifest,
                    check_checksums=check_checksums,
                )
                resource.scan = scan
                scan.resources[resource.name] = resource
        if (scan_dir / Metadata.FNAME).exists():
            scan.metadata = Metadata.load(scan_dir, scan)
        return scan

    @property
    def path(self) -> str:
        return self.session.path + ":" + self.id + "-" + self.type
