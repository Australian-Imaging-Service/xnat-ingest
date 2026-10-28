class XnatIngestError(Exception):
    def __init__(self, msg: str):
        self.msg = msg


class UnsupportedModalityError(XnatIngestError): ...


class StagingError(XnatIngestError): ...


class ImagingSessionParseError(StagingError): ...


class UploadError(XnatIngestError): ...


class DifferingCheckumsException(XnatIngestError): ...


class UpdatedFilesException(DifferingCheckumsException): ...


class IncompleteCheckumsException(DifferingCheckumsException): ...


class SessionIdentityRefused(ValueError):
    """Fresh evidence cannot establish the intended XNAT subject and session."""


class UnknownStudyIdentity(SessionIdentityRefused):
    """The remote experiment has no ingest grouping identity yet."""
