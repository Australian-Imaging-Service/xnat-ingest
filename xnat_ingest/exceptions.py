

class UnsupportedModalityError(Exception):
    def __init__(self, msg):
        self.msg = msg


class StagingError(Exception):
    def __init__(self, msg):
        self.msg = msg


class DicomParseError(StagingError):
    def __init__(self, msg):
        self.msg = msg


class UploadError(Exception):
    def __init__(self, msg):
        self.msg = msg
