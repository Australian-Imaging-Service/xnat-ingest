class UnsupportedModalityError(Exception):
    def __init__(self, msg: str):
        self.msg = msg


class StagingError(Exception):
    def __init__(self, msg: str):
        self.msg = msg


class ImagingSessionParseError(StagingError):
    def __init__(self, msg: str):
        self.msg = msg


class UploadError(Exception):
    def __init__(self, msg: str):
        self.msg = msg
