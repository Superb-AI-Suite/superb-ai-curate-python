import base64
import calendar
import datetime
import json
import platform
import time
import uuid
from typing import Optional, Union
from urllib.parse import urlencode, urlsplit, urlunsplit

from spb_curate import error, http_client, util
from spb_curate.superb_ai_response import SuperbAIResponse
from spb_curate.version import VERSION


def _encode_datetime(dttime) -> int:
    if dttime.tzinfo and dttime.tzinfo.utcoffset(dttime) is not None:
        utc_timestamp = calendar.timegm(dttime.utctimetuple())
    else:
        utc_timestamp = time.mktime(dttime.timetuple())

    return int(utc_timestamp)


def _api_encode(data: dict):
    for key, value in iter(data.items()):
        if value is None:
            continue
        if isinstance(value, bool):
            yield (key, str(value).lower())
        elif isinstance(value, datetime.datetime):
            yield (key, _encode_datetime(value))
        elif isinstance(value, list):
            for sub_value in value:
                yield (f"{key}[]", sub_value)
        else:
            yield (key, value)


def _build_api_url(*, url: str, query: str):
    scheme, netloc, path, base_query, fragment = urlsplit(url)

    if base_query:
        query = "%s&%s" % (base_query, query)

    return urlunsplit((scheme, netloc, path, query, fragment))


class APIRequestor(object):
    def __init__(
        self,
        *,
        access_key: Optional[str] = None,
        api_base: Optional[str] = None,
        team_name: Optional[str] = None,
    ) -> None:
        import spb_curate

        self.api_base = api_base or spb_curate.api_base
        self.access_key = access_key or spb_curate.access_key
        self.team_name = team_name or spb_curate.team_name

        if spb_curate.default_http_client:
            self._client: http_client.RequestsClient = spb_curate.default_http_client
        else:
            spb_curate.default_http_client = http_client.RequestsClient()
            self._client: http_client.RequestsClient = spb_curate.default_http_client

    def handle_error_response(self, rbody, rcode, resp, rheaders):
        error_type = ""
        try:
            if isinstance(resp, dict):
                error_data = resp.get("detail", "")
                error_type = resp.get("type", "")
            else:
                error_data = str(resp)
        except (KeyError, TypeError):
            raise error.APIError(
                message="Invalid response object from API: %r (HTTP response code "
                "was %d)" % (rbody, rcode),
                http_status=rbody,
                code=rcode,
                error_body=resp,
            )

        raise self.specific_api_error(
            rbody, rcode, resp, rheaders, error_data, error_type
        )

    def specific_api_error(
        self,
        rbody,
        rcode: int,
        resp,
        rheaders,
        error_data: Optional[Union[str, dict]] = None,
        error_type: Optional[str] = None,
    ):
        message = ""

        if error_data:
            message = (
                error_data.get("message")
                if isinstance(error_data, dict)
                else str(error_data)
            )

        util.log_info(
            "Superb AI API error response",
            error_code=rcode,
            error_type=error_type,
            error_message=message,
        )

        if rcode == 400:
            if error_type == "QUERY_SYNTAX":
                return error.QuerySyntaxError(
                    message=message,
                    code=rcode,
                )
            return error.BadRequestError(
                message=message,
                code=rcode,
            )
        elif rcode == 401:
            return error.AuthenticationError(
                message=message,
                code=rcode,
            )
        elif rcode == 404:
            return error.NotFoundError(
                message=message,
                code=rcode,
            )
        elif rcode == 409:
            return error.ConflictError(
                message=message,
                code=rcode,
            )
        elif rcode == 429:
            return error.TooManyRequestsError(
                message=message,
                code=rcode,
            )
        elif rcode == 500:
            return error.SystemError(
                message=message,
                code=rcode,
            )

        return error.APIError(message=message, code=rcode)

    def request(
        self,
        *,
        method: str,
        url: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
    ):
        rbody, rcode, rheaders, my_access_key = self.request_raw(
            method=method.lower(), url=url, params=params, supplied_headers=headers
        )
        resp = self.interpret_response(rbody=rbody, rcode=rcode, rheaders=rheaders)
        return resp, my_access_key

    def request_headers(self, *, access_key: str, team_name: str, method: str) -> dict:
        user_agent = "SPB/v0 PythonBindings/%s" % (VERSION,)

        ua = {"bindings_version": VERSION, "lang": "python", "publisher": "superb-ai"}

        for attr, func in [
            ["lang_version", platform.python_version],
            ["platform", platform.platform],
            ["uname", lambda: " ".join(platform.uname())],
        ]:
            try:
                val = func()
            except Exception:
                val = "(disabled)"
            ua[attr] = val

        headers = {
            "X-SPB-Client-User-Agent": json.dumps(ua),
            "User-Agent": user_agent,
            "X-Api-Key": access_key,
            "X-Tenant-Id": team_name,
            "Authorization": f"Basic {base64.b64encode(f'{team_name}:{access_key}'.encode('utf-8')).decode('utf-8')}",
        }

        if method == "post" or method == "put" or method == "patch":
            headers["Content-Type"] = "application/json"
            headers.setdefault("Idempotency-Key", str(uuid.uuid4()))

        return headers

    def request_raw(
        self,
        *,
        method: str,
        url: str,
        params: Optional[dict] = None,
        supplied_headers: Optional[dict] = None,
    ):
        """
        Mechanism for issuing an API call
        """

        if self.access_key:
            my_access_key = self.access_key
        else:
            from spb_curate import access_key

            my_access_key = access_key

        if my_access_key is None:
            raise error.AuthenticationError("No access key was provided.")

        if self.team_name:
            my_team_name = self.team_name
        else:
            from spb_curate import team_name

            my_team_name = team_name

        if my_team_name is None:
            raise error.AuthenticationError("No team name was provided.")

        abs_url = f"{self.api_base}{url}"

        # prepare headers
        headers = self.request_headers(
            access_key=my_access_key, team_name=my_team_name, method=method
        )
        if supplied_headers is not None:
            for key, value in iter(supplied_headers.items()):
                headers[key] = value

        def encode_params() -> str:
            return urlencode(list(_api_encode(params or {})))

        if method == "get" or method == "delete":
            if params:
                abs_url = _build_api_url(url=abs_url, query=encode_params())
            post_data = None
        elif method == "post" or method == "put" or method == "patch":
            if headers.get("Content-Type", "") == "application/json":
                post_data = json.dumps(params)
            elif headers.get("Content-Type", "") == "application/x-www-form-urlencoded":
                post_data = encode_params()
            else:
                raise error.APIConnectionError(
                    f"Unrecognized header Content-Type {headers.get('Content-Type', '')}."
                )
        else:
            raise error.APIConnectionError(f"Unrecognized HTTP method {method}.")

        util.log_info("Superb AI API Request", method=method, path=abs_url)
        util.log_debug(
            "POST data",
            post_data=post_data,
        )

        rcontent, rcode, rheaders = self._client.request(
            method=method, url=abs_url, headers=headers, post_data=post_data
        )

        util.log_info("Superb AI API response", path=abs_url, response_code=rcode)
        util.log_debug("API response body", body=rcontent)

        return rcontent, rcode, rheaders, my_access_key

    def _should_handle_code_as_error(self, rcode):
        return not 200 <= rcode < 300

    def interpret_response(self, *, rbody, rcode, rheaders) -> SuperbAIResponse:
        try:
            if hasattr(rbody, "decode"):
                rbody = rbody.decode("utf-8")
            resp = SuperbAIResponse(rbody, rcode, rheaders)
        except Exception:
            raise error.APIError(
                "Invalid response body from API: %s "
                "(HTTP response code was %d)" % (rbody, rcode),
                rbody,
                rcode,
                rheaders,
            )
        if self._should_handle_code_as_error(rcode):
            self.handle_error_response(
                rbody=rbody, rcode=rcode, resp=resp.data, rheaders=rheaders
            )
        return resp
