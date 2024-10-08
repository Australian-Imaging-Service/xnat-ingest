import typing as ty
import logging
import hashlib
from pathlib import Path
from typing_extensions import Self
import shutil
import attrs
from fileformats.application import Json
from fileformats.core import FileSet
from .exceptions import (
    IncompleteCheckumsException,
    DifferingCheckumsException,
)
import xnat_ingest.scan

logger = logging.getLogger("xnat-ingest")


@attrs.define
class ImagingResource:
    name: str
    fileset: FileSet
    checksums: dict[str, str] = attrs.field(eq=False, repr=False)
    scan: "xnat_ingest.scan.ImagingScan" = attrs.field(
        default=None, eq=False, repr=False
    )

    @checksums.default
    def calculate_checksums(self) -> dict[str, str]:
        return self.fileset.hash_files(
            crypto=hashlib.md5, relative_to=self.fileset.parent
        )

    @property
    def datatype(self) -> ty.Type[FileSet]:
        return type(self.fileset)

    @property
    def metadata(self) -> ty.Mapping[str, ty.Any]:
        return self.fileset.metadata  # type: ignore[no-any-return]

    @property
    def mime_like(self) -> str:
        return self.fileset.mime_like

    def __lt__(self, other: Self) -> bool:
        try:
            scan_id = int(self.scan.id)
        except ValueError:
            scan_id = self.scan.id  # type: ignore[assignment]
        try:
            other_scan_id = int(other.scan.id)
        except ValueError:
            other_scan_id = other.scan.id  # type: ignore[assignment]
        return (scan_id, self.name) < (other_scan_id, other.name)

    def newer_than_or_equal(self, other: Self) -> bool:
        return all(s >= m for s, m in zip(self.fileset.mtimes, other.fileset.mtimes))

    def save(
        self,
        dest_dir: Path,
        copy_mode: FileSet.CopyMode = FileSet.CopyMode.copy,
        calculate_checksums: bool = True,
        overwrite: bool | None = None,
    ) -> Self:
        """Save the resource to a directory

        Parameters
        ----------
        dest_dir: Path
            The directory to save the resource
        copy_mode: FileSet.CopyMode
            The method to copy the files
        calculate_checksums: bool
            Whether to calculate the checksums of the files
        overwrite: bool
            Whether to overwrite the resource if it already exists, if None then the files
            are overwritten if they are newer than the ones saved, otherwise a warning is
            issued, if False an exception will be raised, if True then the resource is
            saved regardless of the files being newer

        Returns
        -------
        ImagingResource
            The saved resource

        Raises
        ------
        FileExistsError
            If the resource already exists and overwrite is False or None and the files
            are not newer
        """
        resource_dir = dest_dir / self.name
        checksums = (
            self.calculate_checksums() if calculate_checksums else self.checksums
        )
        if resource_dir.exists():
            try:
                loaded = self.load(resource_dir, require_manifest=False)
                if loaded.checksums == checksums:
                    return loaded
                elif overwrite is None and not self.newer_than_or_equal(loaded):
                    logger.warning(
                        f"Resource '{self.name}' already exists in '{dest_dir}' but "
                        "the files are not older than the ones to be be saved"
                    )
                elif overwrite:
                    logger.warning(
                        f"Resource '{self.name}' already exists in '{dest_dir}', overwriting"
                    )
                    shutil.rmtree(resource_dir)
                else:
                    if overwrite is None:
                        msg = "and the files are not older than the ones to be be saved"
                    else:
                        msg = ""
                    raise FileExistsError(
                        f"Resource '{self.name}' already exists in '{dest_dir}'{msg}, set "
                        "'overwrite' to True to overwrite regardless of file times"
                    )
            except DifferingCheckumsException:
                logger.warning(
                    f"Resource '{self.name}' already exists in '{dest_dir}', but it is "
                    "incomplete, overwriting"
                )
                shutil.rmtree(resource_dir)
        saved_fileset = self.fileset.copy(resource_dir, mode=copy_mode, trim=True)
        manifest = {"datatype": self.fileset.mime_like, "checksums": checksums}
        Json.new(resource_dir / self.MANIFEST_FNAME, manifest)
        return type(self)(name=self.name, fileset=saved_fileset, checksums=checksums)

    @classmethod
    def load(
        cls,
        resource_dir: Path,
        require_manifest: bool = True,
        check_checksums: bool = True,
    ) -> Self:
        """Load a resource from a directory, reading the manifest file if it exists.
        If the manifest file doesn't exist and 'require_manifest' is True then an
        exception is raised, if it is False, then a generic FileSet object is loaded
        from the files that were found
        """
        manifest_file = resource_dir / cls.MANIFEST_FNAME
        if manifest_file.exists():
            manifest = Json(manifest_file).load()
            checksums = manifest["checksums"]
            datatype: ty.Type[FileSet] = FileSet.from_mime(manifest["datatype"])  # type: ignore[assignment]
        elif require_manifest:
            raise FileNotFoundError(
                f"Manifest file not found in '{resource_dir}' resource, set "
                "'require_manifest' to False to ignore and load as a generic FileSet object"
            )
        else:
            checksums = None
            datatype = FileSet
        fileset = datatype(
            p for p in resource_dir.iterdir() if p.name != cls.MANIFEST_FNAME
        )
        resource = cls(name=resource_dir.name, fileset=fileset, checksums=checksums)
        if checksums is not None and check_checksums:
            resource.check_checksums()
        return resource

    def check_checksums(self) -> None:
        calc_checksums = self.calculate_checksums()
        if calc_checksums != self.checksums:
            if all(v == self.checksums[k] for k, v in calc_checksums.items()):
                missing = list(set(self.checksums) - set(calc_checksums))
                raise IncompleteCheckumsException(
                    f"Files saved with '{self.name}' resource are incomplete "
                    f"according to saved checksums, missing {missing}"
                )

            differing = [
                k for k in self.checksums if calc_checksums[k] != self.checksums[k]
            ]
            raise DifferingCheckumsException(
                f"Checksums don't match those saved with '{self.name}' "
                f"resource: {differing}"
            )

    def unlink(self) -> None:
        """Remove all files in the file-set, the object will be unusable after this"""
        for fspath in self.fileset.fspaths:
            if fspath.is_file():
                fspath.unlink()
            else:
                shutil.rmtree(fspath)

    @property
    def path(self) -> str:
        return self.scan.path + ":" + self.name

    MANIFEST_FNAME = "MANIFEST.json"
