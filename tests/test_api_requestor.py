import base64
import json
import uuid
from collections import OrderedDict
from importlib import reload

import pytest

import spb_curate
from spb_curate.api_requestor import APIRequestor
from spb_curate.error import (
    APIError,
    AuthenticationError,
    BadRequestError,
    ConflictError,
    NotFoundError,
    QuerySyntaxError,
    SystemError,
    TooManyRequestsError,
)
from spb_curate.superb_ai_response import SuperbAIResponse


@pytest.fixture(scope="function", autouse=True)
def credentials():
    team_name = str(uuid.uuid4())
    access_key = str(uuid.uuid4())

    return {
        "team_name": team_name,
        "access_key": access_key,
        "api_base": f"https://{uuid.uuid4()}.superb-ai.com",
        "headers": {
            "Authorization": f"Basic {base64.b64encode(f'{team_name}:{access_key}'.encode('utf-8')).decode('utf-8')}"
        },
    }


@pytest.fixture(scope="function", autouse=True)
def bad_response():
    return {"rbody": {"decode": None}, "rcode": 200, "rheaders": {}}


@pytest.fixture(scope="function", autouse=True)
def ok_empty_response():
    return {
        "rbody": "",
        "rcode": 200,
        "rheaders": {},
    }


@pytest.fixture(scope="function", autouse=True)
def ok_json_response():
    return {
        "rbody": json.dumps({str(uuid.uuid4()): str(uuid.uuid4())}),
        "rcode": 200,
        "rheaders": {},
    }


@pytest.fixture(scope="function", autouse=True)
def error_json_response():
    return {
        "rbody": json.dumps(
            {
                str(uuid.uuid4()): str(uuid.uuid4()),
                "detail": "Sample error message.",
                "type": "Sample_Type",
            }
        ),
        "rcode": 400,
        "rheaders": {},
    }


@pytest.fixture(scope="function", autouse=True)
def error_body_dict():
    return {
        str(uuid.uuid4()): str(uuid.uuid4()),
        "detail": "Sample error message.",
        "type": "Sample_Type",
    }


class TestApiRequestor(object):
    def test_credentials_package_level(self, credentials):
        reload(spb_curate)

        spb_curate.api_base = credentials["api_base"]
        spb_curate.access_key = credentials["access_key"]
        spb_curate.team_name = credentials["team_name"]

        requestor = APIRequestor()

        assert requestor.api_base == credentials["api_base"]
        assert requestor.access_key == credentials["access_key"]
        assert requestor.team_name == credentials["team_name"]

    def test_credentials_params(self, credentials):
        reload(spb_curate)

        requestor = APIRequestor(
            api_base=credentials["api_base"],
            access_key=credentials["access_key"],
            team_name=credentials["team_name"],
        )

        assert requestor.api_base == credentials["api_base"]
        assert requestor.access_key == credentials["access_key"]
        assert requestor.team_name == credentials["team_name"]

    def test_headers(self, credentials):
        reload(spb_curate)

        requestor = APIRequestor(
            api_base=credentials["api_base"],
            access_key=credentials["access_key"],
            team_name=credentials["team_name"],
        )

        headers = requestor.request_headers(
            access_key=credentials["access_key"],
            team_name=credentials["team_name"],
            method="get",
        )

        assert isinstance(headers, dict)
        assert headers.get("X-Api-Key") is credentials["access_key"]
        assert headers.get("X-Tenant-Id") is credentials["team_name"]
        assert headers.get("Authorization") == credentials["headers"]["Authorization"]

    def test_interpret_response(
        self,
        credentials,
        bad_response,
        ok_empty_response,
        ok_json_response,
        error_json_response,
        error_body_dict,
    ):
        reload(spb_curate)

        requestor = APIRequestor(
            api_base=credentials["api_base"],
            access_key=credentials["access_key"],
            team_name=credentials["team_name"],
        )

        try:
            requestor.interpret_response(
                rbody=bad_response["rbody"],
                rcode=bad_response["rcode"],
                rheaders=bad_response["rheaders"],
            )
        except APIError:
            pass
        else:
            raise Exception("The bad response did not raise an exception")

        ok_response = requestor.interpret_response(
            rbody=ok_empty_response["rbody"],
            rcode=ok_empty_response["rcode"],
            rheaders=ok_empty_response["rheaders"],
        )

        assert isinstance(ok_response, SuperbAIResponse)
        assert ok_response.code == ok_empty_response["rcode"]
        assert ok_response.headers == ok_empty_response["rheaders"]
        assert ok_response.body == ok_empty_response["rbody"]
        assert ok_response.data is None

        ok_json_response = requestor.interpret_response(
            rbody=ok_json_response["rbody"],
            rcode=ok_json_response["rcode"],
            rheaders=ok_json_response["rheaders"],
        )
        assert ok_json_response.data == json.loads(
            ok_json_response.body, object_pairs_hook=OrderedDict
        )

        # 400 error
        error_body_dict["type"] = "QUERY_SYNTAX"
        with pytest.raises(QuerySyntaxError):
            ok_json_response = requestor.interpret_response(
                rbody=json.dumps(error_body_dict),
                rcode=400,
                rheaders=error_json_response["rheaders"],
            )

        with pytest.raises(BadRequestError):
            ok_json_response = requestor.interpret_response(
                rbody=error_json_response["rbody"],
                rcode=400,
                rheaders=error_json_response["rheaders"],
            )

        # 401 error
        with pytest.raises(AuthenticationError):
            ok_json_response = requestor.interpret_response(
                rbody=error_json_response["rbody"],
                rcode=401,
                rheaders=error_json_response["rheaders"],
            )

        # 404 error
        with pytest.raises(NotFoundError):
            ok_json_response = requestor.interpret_response(
                rbody=error_json_response["rbody"],
                rcode=404,
                rheaders=error_json_response["rheaders"],
            )

        # 409 error
        with pytest.raises(ConflictError):
            ok_json_response = requestor.interpret_response(
                rbody=error_json_response["rbody"],
                rcode=409,
                rheaders=error_json_response["rheaders"],
            )

        # 429 error
        with pytest.raises(TooManyRequestsError):
            ok_json_response = requestor.interpret_response(
                rbody=error_json_response["rbody"],
                rcode=429,
                rheaders=error_json_response["rheaders"],
            )

        # 500 error
        with pytest.raises(SystemError):
            ok_json_response = requestor.interpret_response(
                rbody=error_json_response["rbody"],
                rcode=500,
                rheaders=error_json_response["rheaders"],
            )

        # Generic error
        with pytest.raises(APIError):
            ok_json_response = requestor.interpret_response(
                rbody=error_json_response["rbody"],
                rcode=999999,
                rheaders=error_json_response["rheaders"],
            )
