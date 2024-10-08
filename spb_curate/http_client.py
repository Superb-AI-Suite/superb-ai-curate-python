import textwrap
import threading

import requests
from requests.adapters import HTTPAdapter, Retry

from spb_curate import error

BACKOFF_FACTOR = 1
MAX_RETRY_COUNT = 2
STATUS_FORCE_LIST = (500, 502, 503, 504)
STATUS_FORCE_SET = {x for x in STATUS_FORCE_LIST}


def calculate_backoff(attempt: int, factor: int = BACKOFF_FACTOR) -> int:
    return factor * (2 ** (attempt - 1))


class RequestsClient(object):
    name = "requests"

    def __init__(self, timeout: int = 80, session=None, **kwargs):
        super(RequestsClient, self).__init__(**kwargs)
        self._thread_local = threading.local()
        self._session = session
        self._timeout = timeout

    def request(self, *, method: str, url: str, headers: dict, post_data=None):
        return self._request_internal(
            method=method,
            url=url,
            headers=headers,
            post_data=post_data,
        )

    def _request_internal(self, *, method: str, url: str, headers: dict, post_data):
        kwargs = {}

        if getattr(self._thread_local, "session", None) is None:
            if self._session is None:
                self._session = requests.Session()
                allowed_methods = list(Retry.DEFAULT_ALLOWED_METHODS) + [
                    "POST",
                    "PATCH",
                ]
                retries = Retry(
                    total=MAX_RETRY_COUNT,
                    read=MAX_RETRY_COUNT,
                    connect=MAX_RETRY_COUNT,
                    backoff_factor=BACKOFF_FACTOR,
                    status_forcelist=STATUS_FORCE_LIST,
                    allowed_methods=allowed_methods,
                )
                adapter = HTTPAdapter(max_retries=retries)
                self._session.mount("http://", adapter)
                self._session.mount("https://", adapter)

            self._thread_local.session = self._session

        try:
            try:
                result = self._thread_local.session.request(
                    method,
                    url,
                    headers=headers,
                    data=post_data,
                    timeout=self._timeout,
                    **kwargs,
                )
            except TypeError as e:
                raise TypeError(
                    "Warning: It looks like your installed version of the "
                    '"requests" library is not compatible. Most likely '
                    'the "requests" library is out of date. You can fix '
                    'that by running "pip install -U requests".) The '
                    "request error was: %s" % (e,)
                )

            content = result.content

            status_code = result.status_code
        except Exception as e:
            # Would catch just requests.exceptions.RequestException, but can
            # also raise ValueError, RuntimeError, etc.
            self._handle_request_error(e)
        return content, status_code, result.headers

    def _handle_request_error(self, e):
        # Catch SSL error first as it belongs to ConnectionError,
        # but we don't want to retry
        if isinstance(e, requests.exceptions.SSLError):
            msg = (
                "Could not verify Superb AI's SSL certificate. Please make "
                "sure that your network is not intercepting certificates."
            )
            err = "%s: %s" % (type(e).__name__, str(e))
            should_retry = False
        # Retry only timeout and connect errors; similar to urllib3 Retry
        elif isinstance(
            e,
            (requests.exceptions.Timeout, requests.exceptions.ConnectionError),
        ):
            msg = "Unexpected error communicating with Superb AI."
            err = "%s: %s" % (type(e).__name__, str(e))
            should_retry = True
        # Reached maximum retry attempts
        elif isinstance(e, requests.exceptions.RetryError):
            msg = "Unexpected error communicating with Superb AI."
            err = "%s: %s" % (type(e).__name__, str(e))
            should_retry = False
        # Catch remaining request exceptions
        elif isinstance(e, requests.exceptions.RequestException):
            msg = "Unexpected error communicating with Superb AI."
            err = "%s: %s" % (type(e).__name__, str(e))
            should_retry = False
        else:
            msg = (
                "Unexpected error communicating with Superb AI. "
                "It looks like there's probably a configuration "
                "issue locally."
            )
            err = "A %s was raised" % (type(e).__name__,)
            if str(e):
                err += " with error message %s" % (str(e),)
            else:
                err += " with no error message"
            should_retry = False

        msg = textwrap.fill(msg) + "\n\n(Network error: %s)" % (err,)
        raise error.APIConnectionError(msg, should_retry=should_retry)

    def close(self):
        if getattr(self._thread_local, "session", None) is not None:
            self._thread_local.session.close()
