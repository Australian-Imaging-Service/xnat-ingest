class XnatIngestError(Exception):
    def __init__(self, msg: str):
        self.msg = msg


class UnsupportedModalityError(XnatIngestError): ...


class StagingError(XnatIngestError): ...


class OrthancStudyChangedError(StagingError):
    """The Orthanc study changed while it was being staged. A later pass stages it again."""


class ImagingSessionParseError(StagingError): ...


class UploadError(XnatIngestError): ...


class DifferingCheckumsException(XnatIngestError): ...


class UpdatedFilesException(DifferingCheckumsException): ...


class IncompleteCheckumsException(DifferingCheckumsException): ...
