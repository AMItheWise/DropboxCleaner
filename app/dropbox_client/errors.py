from __future__ import annotations


class DropboxClientError(Exception):
    """Base error for Dropbox adapter failures."""


class TemporaryDropboxError(DropboxClientError):
    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class PermanentDropboxError(DropboxClientError):
    """Non-retryable Dropbox error."""


class AuthenticationFailureError(PermanentDropboxError):
    """Authentication or authorization failed."""


class MissingScopeError(PermanentDropboxError):
    def __init__(self, message: str, required_scope: str | None = None) -> None:
        super().__init__(message)
        self.required_scope = required_scope


class PathNotFoundError(PermanentDropboxError):
    """Dropbox path does not exist."""


class DestinationConflictError(PermanentDropboxError):
    """Destination already exists or conflicts with the intended action."""


class CursorResetError(PermanentDropboxError):
    """List-folder cursor expired or needs to be reset."""


class ConflictPolicyAbortError(PermanentDropboxError):
    """Raised when the configured conflict policy requires stopping the run."""


class BlockedPreconditionError(PermanentDropboxError):
    """A required team/admin precondition could not be satisfied safely."""
