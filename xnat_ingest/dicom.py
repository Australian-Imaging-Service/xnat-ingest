import typing as ty
import subprocess as sp
# import re
import pydicom
# from fileformats.core import FileSet
# from fileformats.application import Dicom
# from fileformats.extras.application.medical import dicom_read_metadata


try:
    dcmedit_path = sp.check_output("which dcmedit", shell=True).decode("utf-8").strip()
except sp.CalledProcessError:
    dcmedit_path = None


try:
    dcminfo_path = sp.check_output("which dcminfo", shell=True).decode("utf-8").strip()
except sp.CalledProcessError:
    dcminfo_path = None


def tag2keyword(tag: ty.Tuple[str, str]) -> str:
    return pydicom.datadict.dictionary_keyword(tag)


def keyword2tag(keyword: str) -> ty.Tuple[str, str]:
    tag_str = hex(pydicom.datadict.tag_for_keyword(keyword))[2:]
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


# @FileSet.read_metadata.register
# def mrtrix_dicom_read_metadata(
#     dcm: Dicom, selected_keys: ty.Optional[ty.Sequence[str]] = None
# ) -> ty.Mapping[str, ty.Any]:
#     if dcminfo_path is None or selected_keys is None:
#         return dicom_read_metadata(dcm, selected_keys)

#     tags = [keyword2tag(k) for k in selected_keys]
#     tag_str = " ".join(f"-t {t[0]} {t[1]}" for t in tags)
#     cmd = f"dcminfo {tag_str} {dcm.fspath}"
#     line_re = re.compile(r"\[([0-9A-F]{4}),([0-9A-F]{4})] (.*)")
#     dcminfo_output = sp.check_output(cmd, shell=True).decode("utf-8")
#     metadata = {}
#     for line in dcminfo_output.splitlines():
#         match = line_re.match(line)
#         if not match:
#             continue
#         t1, t2, val = match.groups()
#         key = tag2keyword((t1, t2))
#         val = val.strip()
#         metadata[key] = val
#     return metadata
