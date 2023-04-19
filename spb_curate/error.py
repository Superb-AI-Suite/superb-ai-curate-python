from typing import Optional, Union

# -- API ERRORS --


class SuperbAIError(Exception):
    def __init__(
        self,
        message: Union[str, dict] = "",
        http_status=None,
        code: Optional[int] = None,
        error_body=None,
    ):
        super(SuperbAIError, self).__init__(message)

        self._message = message
        self.http_status = http_status
        self.code = code
        self.raw_error_body = error_body
        self.error = self.build_error_object()

    def __str__(self):
        msg = self._message or "<empty message>"
        return msg

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(message={self._message}, "
            f"http_status={self.http_status})"
        )

    def build_error_object(self):
        if self.raw_error_body is None:
            return None

        if isinstance(self.raw_error_body, str) or isinstance(
            self.raw_error_body, dict
        ):
            return self.raw_error_body

        return None


class APIConnectionError(SuperbAIError):
    def __init__(
        self,
        message: Union[str, dict] = "",
        http_status=None,
        code: Optional[int] = None,
        error_body=None,
        should_retry=False,
    ):
        super().__init__(
            message=message, http_status=http_status, code=code, error_body=error_body
        )

        self.should_retry = should_retry


# generic case
class APIError(SuperbAIError):
    pass


class AuthenticationError(SuperbAIError):
    """HTTP CODE: 401"""

    pass


class BadRequestError(SuperbAIError):
    """HTTP CODE: 400"""

    pass


class ConflictError(SuperbAIError):
    """HTTP CODE: 409"""

    pass


class QuerySyntaxError(BadRequestError):
    """HTTP CODE: 400"""

    pass


class SystemError(SuperbAIError):
    """HTTP CODE: 500"""

    pass


class MaxFieldCountReachedError(SuperbAIError):
    """HTTP CODE: 403"""

    pass


class NotFoundError(SuperbAIError):
    """HTTP CODE: 404"""

    pass


class TooManyRequestsError(SuperbAIError):
    """HTTP CODE: 429"""

    pass


# -- SDK ERRORS --


class ValidationError(Exception):
    pass
