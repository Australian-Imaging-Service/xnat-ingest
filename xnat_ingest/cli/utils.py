import pydicom
from fileformats.core import from_mime


class LoggerEmail:
    def __init__(self, address, loglevel, subject):
        self.address = address
        self.loglevel = loglevel
        self.subject = subject

    @classmethod
    def split_envvar_value(cls, envvar):
        return [cls(*entry.split(",")) for entry in envvar.split(";")]

    def __str__(self):
        return self.address


class MailServer:
    def __init__(self, host, sender_email, user, password):
        self.host = host
        self.sender_email = sender_email
        self.user = user
        self.password = password


class NonDicomType(str):
    def __init__(self, mime):
        self.type = from_mime(mime)

    @classmethod
    def split_envvar_value(cls, envvar):
        return [cls(entry) for entry in envvar.split(";")]


class DicomField:
    def __init__(self, keyword_or_tag):
        # Get the tag associated with the keyword
        try:
            self.tag = pydicom.datadict.tag_for_keyword(keyword_or_tag)
        except ValueError:
            try:
                self.keyword = pydicom.datadict.dictionary_description(keyword_or_tag)
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