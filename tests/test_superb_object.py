import pytest

from spb_curate import error
from spb_curate.abstract.superb_ai_object import SuperbAIObject


class TestSuperbAIObject(object):
    def test_get_endpoint(self):
        SuperbAIObject._endpoints = {"sample": "/sample/endpoint/{id}/"}
        url = SuperbAIObject.get_endpoint(name="sample", params={"id": "sample-id"})
        assert url == "/sample/endpoint/sample-id/"

        with pytest.raises(error.ValidationError):
            SuperbAIObject.get_endpoint(name="sample", params={"id": ""})

        with pytest.raises(error.ValidationError):
            SuperbAIObject.get_endpoint(name="sample", params={"id": None})

    def test_get_endpoint_method(self):
        SuperbAIObject._endpoints_method = {"sample-1": "post"}
        method = SuperbAIObject.get_endpoint_method(name="sample-1", default="get")
        assert method == "post"

        method = SuperbAIObject.get_endpoint_method(name="sample-2", default="get")
        assert method == "get"

        with pytest.raises(error.ValidationError):
            SuperbAIObject.get_endpoint_method(name="sample-2")
