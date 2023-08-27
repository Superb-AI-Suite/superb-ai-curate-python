from __future__ import annotations

from typing import Dict, Iterator, List, Optional, Union

from spb_curate import api_requestor, error, util
from spb_curate.abstract.superb_ai_object import SuperbAIObject


class APIResource(SuperbAIObject):
    @classmethod
    def _static_request(
        cls,
        *,
        method_: str,
        url_: str,
        access_key: Optional[str] = None,
        api_base: Optional[str] = None,
        team_name: Optional[str] = None,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
    ) -> SuperbAIObject:
        requestor = api_requestor.APIRequestor(
            access_key=access_key, api_base=api_base, team_name=team_name
        )
        response, access_key = requestor.request(
            method=method_, url=url_, params=params, headers=headers
        )

        return util.convert_to_superb_ai_object(
            data=response,
            access_key=access_key,
            team_name=team_name,
            cls=cls,
        )

    @classmethod
    def fetch(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        endpoint_params: Optional[dict] = None,
        headers: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> SuperbAIObject:
        """
        Raises
        ------
        ValidationError
            When `endpoint_params` contains `None` or empty strings.
        """
        url = cls.get_endpoint(name="fetch", params=endpoint_params)
        method = cls.get_endpoint_method(name="fetch", default="get")

        return cls._static_request(
            method_=method,
            url_=url,
            access_key=access_key,
            team_name=team_name,
            params=params,
            headers=headers,
        )

    def refresh(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        endpoint_params: Optional[dict] = None,
        headers: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> None:
        url = self.get_endpoint(name="fetch", params=endpoint_params)
        method = self.get_endpoint_method(name="fetch", default="get")

        refreshed_object = self._static_request(
            method_=method,
            url_=url,
            access_key=access_key,
            team_name=team_name,
            params=params,
            headers=headers,
        )

        self.load_from_dict(
            data=refreshed_object,
            access_key=access_key,
            team_name=team_name,
        )


class CreateResource(APIResource):
    @classmethod
    def create(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        endpoint_params: Optional[dict] = None,
        headers: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> SuperbAIObject:
        """
        Raises
        ------
        ValidationError
            When `endpoint_params` contains `None` or empty strings.
        """
        url = cls.get_endpoint(name="create", params=endpoint_params)
        method = cls.get_endpoint_method(name="create", default="post")

        return cls._static_request(
            method_=method,
            url_=url,
            access_key=access_key,
            team_name=team_name,
            params=params,
            headers=headers,
        )


class DeleteResource(APIResource):
    @classmethod
    def _cls_delete(
        cls,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        endpoint_params: Optional[dict] = None,
        headers: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> None:
        """
        Raises
        ------
        ValidationError
            When `endpoint_params` contains `None` or empty strings.
        """
        url = cls.get_endpoint(name="delete", params=endpoint_params)
        method = cls.get_endpoint_method(name="delete", default="delete")

        return cls._static_request(
            method_=method,
            url_=url,
            access_key=access_key,
            team_name=team_name,
            headers=headers,
            params=params,
        )

    def delete(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        endpoint_params: Optional[dict] = None,
        headers: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> None:
        """
        Raises
        ------
        ValidationError
            When `endpoint_params` contains `None` or empty strings.
        """
        self._cls_delete(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            headers=headers,
            params=params,
        )
        self.id = None


class PaginateResource(APIResource):
    @classmethod
    def fetch_all(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        endpoint_params: Optional[dict] = None,
        headers: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> List[SuperbAIObject]:
        """
        Raises
        ------
        ValidationError
            When `endpoint_params` contains `None` or empty strings.
        """
        url = cls.get_endpoint(name="paginate", params=endpoint_params)
        method = cls.get_endpoint_method(name="paginate", default="get")
        get_params = params.copy() if params else {}

        requestor = api_requestor.APIRequestor(
            access_key=access_key, team_name=team_name
        )

        response, access_key = requestor.request(
            method=method, url=url, params=get_params, headers=headers
        )

        response_data = response.data
        response_data_results = response_data.pop("results", [])

        result_objects: List[SuperbAIObject] = util.convert_to_superb_ai_object(
            data=response_data_results,
            access_key=access_key,
            team_name=team_name,
            cls=cls,
        )

        data = util.convert_to_superb_ai_object(
            data=response_data,
            access_key=access_key,
            team_name=team_name,
        )

        data.update({"results": result_objects})

        return data

    @classmethod
    def fetch_all_iter(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        endpoint_params: Optional[dict] = None,
        headers: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> Iterator[SuperbAIObject]:
        raise NotImplementedError

    @classmethod
    def fetch_page(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        endpoint_params: Optional[dict] = None,
        headers: Optional[dict] = None,
        params: dict,
    ) -> Dict[str, Union[int, List[SuperbAIObject], str]]:
        """
        Raises
        ------
        ValidationError
            When `endpoint_params` contains `None` or empty strings.
        """
        url = cls.get_endpoint(name="paginate", params=endpoint_params)
        method = cls.get_endpoint_method(name="paginate", default="get")
        requestor = api_requestor.APIRequestor(
            access_key=access_key, team_name=team_name
        )

        response, access_key = requestor.request(
            method=method, url=url, params=params, headers=headers
        )

        response_data = response.data
        response_data_results = response_data.pop("results", [])

        result_objects: List[SuperbAIObject] = util.convert_to_superb_ai_object(
            data=response_data_results,
            access_key=access_key,
            team_name=team_name,
            cls=cls,
        )

        data = util.convert_to_superb_ai_object(
            data=response_data,
            access_key=access_key,
            team_name=team_name,
        )

        data.update({"results": result_objects})

        return data

    @classmethod
    def fetch_page_iter(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        endpoint_params: Optional[dict] = None,
        headers: Optional[dict] = None,
        params: dict,
    ) -> Iterator[Dict[str, Union[int, List[SuperbAIObject], str]]]:
        raise NotImplementedError


class ModifyResource(APIResource):
    @classmethod
    def _cls_modify(
        cls,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        endpoint_params: Optional[dict] = None,
        headers: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> SuperbAIObject:
        """
        Raises
        ------
        ValidationError
            When `endpoint_params` contains `None` or empty strings.
        """
        url = cls.get_endpoint(name="modify", params=endpoint_params)
        method = cls.get_endpoint_method(name="modify", default="put")

        return cls._static_request(
            method_=method,
            url_=url,
            access_key=access_key,
            team_name=team_name,
            headers=headers,
            params=params,
        )

    def modify(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        endpoint_params: Optional[dict] = None,
        headers: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> SuperbAIObject:
        """
        Raises
        ------
        ValidationError
            When `endpoint_params` contains `None` or empty strings.
        """
        modified_object = self._cls_modify(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            headers=headers,
            params=params,
        )

        self.load_from_dict(
            data=modified_object,
            access_key=access_key,
            team_name=team_name,
        )
