

class DicomParseError(Exception):
    def __init__(self, msg):
        self.msg = msg


class UnsupportedModalityError(Exception):
    def __init__(self, msg):
        self.msg = msg
