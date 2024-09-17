import typing as ty
import subprocess as sp

# import re
import pydicom

# from fileformats.core import FileSet
# from fileformats.application import Dicom
# from fileformats.extras.application.medical import dicom_read_metadata

dcmedit_path: ty.Optional[str]
try:
    dcmedit_path = sp.check_output("which dcmedit", shell=True).decode("utf-8").strip()
except sp.CalledProcessError:
    dcmedit_path = None

dcminfo_path: ty.Optional[str]
try:
    dcminfo_path = sp.check_output("which dcminfo", shell=True).decode("utf-8").strip()
except sp.CalledProcessError:
    dcminfo_path = None


def tag2keyword(tag: ty.Tuple[str, str]) -> str:
    return pydicom.datadict.dictionary_keyword((int(tag[0]), int(tag[1])))


def keyword2tag(keyword: str) -> ty.Tuple[str, str]:
    tag = pydicom.datadict.tag_for_keyword(keyword)
    if not tag:
        raise ValueError(f"Could not find tag for keyword '{keyword}'")
    tag_str = hex(tag)[2:]
    return (f"{tag_str[:-4].zfill(4)}", tag_str[-4:])


class DicomField:
    name = "dicom_field"

    def __init__(self, keyword_or_tag):
        # Get the tag associated with the keyword
        try:
            self.tag = keyword2tag(keyword_or_tag)
        except ValueError:
            try:
                self.keyword = tag2keyword(keyword_or_tag)
            except ValueError:
                raise ValueError(
                    f'Could not parse "{keyword_or_tag}" as a DICOM keyword or tag'
                )
            else:
                self.tag = keyword_or_tag
        else:
            self.keyword = keyword_or_tag

    def __str__(self):
        return f"'{self.keyword}' field ({','.join(self.tag)})"
