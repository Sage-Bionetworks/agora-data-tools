"""Custom error classes for agoradatatools."""


class ADTError(Exception):
    """Base class for all custom exceptions in agoradatatools."""


class ADTDataProcessingError(ADTError):
    """Error to be raised when Data Processing runs fail."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message
