from __future__ import annotations

import json
import time
from typing import Dict, Iterator, List, Optional, Union

import requests

from spb_curate import api_requestor, error, util
from spb_curate.abstract.api.resource import PaginateResource
from spb_curate.curate.api import settings
from spb_curate.curate.api.enums import JobType


class Job(PaginateResource):
    _endpoints = {
        "bulk_create": "/curate/batch/jobs/",
        "bulk_create_upload": "/curate/batch/params/",
        "fetch": "/curate/batch/jobs/{id}/",
        "paginate": "/curate/batch/jobs/",
    }
    _object_type = "job"

    @classmethod
    def create(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        job_type: JobType,
        param: dict,
    ) -> Job:
        """
        Creates a job.

        Parameters
        ----------
        job_type
            The type of the job to create.
        param
            The parameters for the job.
            Differs by each ``JobType``.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The created job.
        """

        if not isinstance(job_type, JobType):
            raise error.ValidationError(f"Invalid job type {job_type}.")

        # Submit the bulk job
        bulk_params = {"job_type": job_type.value, "param": param}
        url = cls.get_endpoint(name="bulk_create", params=None)

        return Job._static_request(
            method_="post",
            url_=url,
            access_key=access_key,
            team_name=team_name,
            params=bulk_params,
            headers=None,
        )

    @classmethod
    def fetch(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        id: str,
    ) -> Job:
        """
        Fetches a job.

        Parameters
        ----------
        id
            The ID of the job to fetch.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The fetched job.
        """
        endpoint_params = {"id": id}

        return super(Job, cls).fetch(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            headers=None,
            params=None,
        )

    @classmethod
    def fetch_all(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        from_date: Optional[str] = None,
    ) -> List[Job]:
        """
        Fetches jobs that match the date filter.
        If not provided, fetches all jobs.

        Parameters
        ----------
        from_date
            ISO 8601 formatted UTC date string to filter jobs created on or after the specified date.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            Matching jobs.
        """
        all_jobs = []
        for page in cls.fetch_page_iter(
            access_key=access_key,
            team_name=team_name,
            from_date=from_date,
        ):
            all_jobs.extend(page.get("results", []))
        return all_jobs

    @classmethod
    def fetch_all_iter(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        from_date: Optional[str] = None,
    ) -> Iterator[Job]:
        """
        Iterates through jobs that match the date filter.
        If not provided, iterates through all jobs.

        Parameters
        ----------
        from_date
            ISO 8601 formatted UTC date string to filter jobs created on or after the specified date.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The matching job iterator.

        Yields
        -------
            The next matching job.
        """
        for fetch_result in cls.fetch_page_iter(
            access_key=access_key,
            team_name=team_name,
            from_date=from_date,
        ):
            for job in fetch_result.get("results", []):
                yield job

    @classmethod
    def fetch_page(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        from_date: Optional[str] = None,
        cursor: Optional[str] = None,
        limit: int = 10,
    ) -> Dict[str, Union[int, List[Job]]]:
        """
        Fetches a page of jobs that match the date filter.
        If not provided, paginates all jobs.

        Parameters
        ----------
        cursor
            A cursor for pagination.
            Pass in `next_cursor` from the previous page to fetch the next page.
        limit
            The maximum number of jobs in a page.
        from_date
            ISO 8601 formatted UTC date string to filter jobs created on or after the specified date.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            A page of matching jobs.
        """
        params = {"limit": limit}

        if from_date:
            params.update({"from_date": from_date})

        if cursor:
            params["cursor"] = cursor

        return super(Job, cls).fetch_page(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=None,
            headers=None,
            params=params,
        )

    @classmethod
    def fetch_page_iter(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        from_date: Optional[str] = None,
    ) -> Iterator[Dict[str, Union[int, List[Job]]]]:
        """
        Iterates through pages of jobs that match the date filter.
        If not provided, paginates all jobs.

        Parameters
        ----------
        from_date
            ISO 8601 formatted UTC date string to filter jobs created on or after the specified date.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The matching job page iterator.

        Yields
        -------
            The next page of matching jobs.
        """

        cursor = None
        page_result = {}
        limit = settings.FETCH_PAGE_LIMIT

        def fetch_result(cursor: Optional[str] = None):
            page_result = cls.fetch_page(
                access_key=access_key,
                team_name=team_name,
                from_date=from_date,
                cursor=cursor,
                limit=limit,
            )
            return page_result

        page_result = fetch_result(cursor=cursor)
        yield page_result

        while (
            len(page_result.get("results", [])) == limit
            and page_result.get("next_cursor", None) is not None
        ):
            page_result = fetch_result(cursor=page_result.get("next_cursor"))
            yield page_result

    @classmethod
    def _upload_params(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        data: Union[dict, list],
    ) -> dict:
        # Upload params to S3 bucket
        params_data = json.dumps(data, cls=cls.ReprJSONEncoder).encode("UTF-8")
        url = cls.get_endpoint(name="bulk_create_upload", params=None)

        requestor = api_requestor.APIRequestor(
            access_key=access_key, team_name=team_name
        )
        response, access_key = requestor.request(
            method="post",
            url=url,
            params={"file_size": len(params_data)},
            headers=None,
        )

        upload_params_dict = util.convert_to_superb_ai_object(
            data=response, access_key=access_key, team_name=team_name
        )

        put_params_response = requests.put(
            upload_params_dict["upload_url"], data=params_data
        )

        if put_params_response.status_code != 200:
            raise error.SuperbAIError(
                "There was an error in uploading the job parameters."
            )

        return upload_params_dict

    def refresh(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
    ) -> None:
        """
        Refreshes the job.

        Parameters
        ----------
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.
        """
        endpoint_params = {"id": self.id}

        super(Job, self).refresh(
            access_key=access_key, team_name=team_name, endpoint_params=endpoint_params
        )

    def wait_until_complete(self, *, timeout: Optional[int] = None) -> None:
        """
        Waits until the job has either completed or failed.

        Parameters
        ----------
        timeout
            The maximum time in seconds to wait for.
            If not provided, ``spb_curate.timeout`` will be used.
        """
        from spb_curate import timeout as default_timeout

        if timeout is None:
            timeout = default_timeout

        frequency = 2  # seconds

        while True:
            if self.status in ["FAILED", "COMPLETE"]:
                break

            wait_seconds = min(timeout, frequency)

            if wait_seconds <= 0:
                break

            timeout -= frequency
            time.sleep(wait_seconds)

            self.refresh()
