class AccessionFlowError(Exception):
    def __init__(self, msg: str):
        self.msg = msg


class UnsupportedModalityError(AccessionFlowError): ...


class StagingError(AccessionFlowError): ...


class OrthancStudyChangedError(StagingError):
    """The Orthanc study changed while it was being staged. A later pass stages it again."""


class AccessionParseError(StagingError): ...


class UploadError(AccessionFlowError): ...


class DifferingCheckumsException(AccessionFlowError): ...


class UpdatedFilesException(DifferingCheckumsException): ...


class IncompleteCheckumsException(DifferingCheckumsException): ...


class SessionIdentityRefused(ValueError):
    """Fresh evidence cannot establish the intended XNAT subject and session."""


class UnknownStudyIdentity(SessionIdentityRefused):
    """The remote experiment has no ingest grouping identity yet."""
