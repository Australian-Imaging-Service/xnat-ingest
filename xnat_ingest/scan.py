import typing as ty
import re
import logging
import attrs
from fileformats.core import FileSet
from .resource import ImagingResource
from .utils import AssociatedFiles

logger = logging.getLogger("xnat-ingest")


def scan_type_converter(scan_type: str) -> str:
    "Ensure there aren't any special characters that aren't valid file/dir paths"
    return re.sub(r"[\"\*\/\:\<\>\?\\\|\+\,\.\;\=\[\]]+", "", scan_type)


def scan_resources_converter(
    resources: dict[str, ImagingResource | FileSet]
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
    type: str
        the scan type/description
    """

    id: str
    type: str = attrs.field(converter=scan_type_converter)
    resources: ty.Dict[str, ImagingResource] = attrs.field(
        factory=dict, converter=scan_resources_converter
    )
    associated: AssociatedFiles | None = None

    def __contains__(self, resource_name: str) -> bool:
        return resource_name in self.resources

    def __getitem__(self, resource_name: str) -> ImagingResource:
        return self.resources[resource_name]
