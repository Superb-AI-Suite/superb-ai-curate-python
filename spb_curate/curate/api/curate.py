from __future__ import annotations

import asyncio
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

import aiohttp

from spb_curate import api_requestor, error, http_client, util
from spb_curate.abstract.api.resource import (
    CreateResource,
    DeleteResource,
    ModifyResource,
    PaginateResource,
)
from spb_curate.curate.api import settings
from spb_curate.curate.api.abstract import BaseImageSource
from spb_curate.curate.api.enums import (
    JobType,
    SearchFieldMappingType,
    SupportedImageFormat,
)
from spb_curate.curate.api.job import Job
from spb_curate.curate.model.annotation_types import (
    AnnotationType,
    BoundingBox,
    Category,
    Cuboid2D,
    Keypoints,
    Polygon,
    Polyline,
    RotatedBox,
)


class Annotation(CreateResource, DeleteResource, ModifyResource, PaginateResource):
    _discriminator_map = {
        "annotation_value": (
            "annotation_type",
            {
                "box": BoundingBox,
                "category": Category,
                "cuboid2d": Cuboid2D,
                "keypoint": Keypoints,
                "polygon": Polygon,
                "polyline": Polyline,
                "rbox": RotatedBox,
            },
        )
    }
    _endpoints = {
        "create": "/curate/dataset-core/datasets/{dataset_id}/images/{image_id}/annotations/",
        "delete": "/curate/dataset-core/datasets/{dataset_id}/annotations/{id}",
        "fetch": "/curate/dataset-query/datasets/{dataset_id}/images/{image_id}/annotations/{id}",
        "modify": "/curate/dataset-core/datasets/{dataset_id}/annotations/{id}/metadata",
        "paginate": "/curate/dataset-query/datasets/{dataset_id}/annotations/_search",
    }
    _endpoints_method = {
        "paginate": "post",
    }
    _field_initializers = {"annotation_value": "_init_annotation_value"}
    _object_type = "annotation"

    def __init__(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        image_id: Optional[str] = None,
        image_key: Optional[str] = None,
        annotation_class: str,
        annotation_type: Optional[str] = None,
        annotation_value: Union[
            BoundingBox,
            Category,
            Cuboid2D,
            Keypoints,
            Polygon,
            Polyline,
            RotatedBox,
            dict,
            list,
        ],
        metadata: dict,
        **params,
    ):
        """
        Initializes an annotation.
        A newly initialized annotation is incomplete and must be added to a dataset.

        Parameters
        ----------
        image_id
            The ID of the image to add the annotation to.
            Must provide at least one of ``image_id`` or ``image_key``.
        image_key
            The key of the image to add the annotation to.
            Must provide at least one of ``image_id`` or ``image_key``.
        annotation_class
            The classification of the annotation (e.g. "person", "vehicle").
        annotation_type
            The type of the annotation (e.g. "box", "polygon").
            Will be inferred if ``annotation_value`` is an instance of ``AnnotationType``.
        annotation_value
            The value of the annotation.
        metadata
            The metadata associated with the annotation.
            Must be flat (one level deep).
        """
        super(Annotation, self).__init__(
            access_key=access_key,
            team_name=team_name,
            image_id=image_id,
            image_key=image_key,
            annotation_class=annotation_class,
            annotation_type=(
                annotation_value._object_type
                if isinstance(annotation_value, AnnotationType)
                else annotation_type
            ),
            annotation_value=annotation_value,
            metadata=metadata,
            **params,
        )

    def _init_volatile_fields(
        self,
        annotation_class: str,
        annotation_type: str,
        annotation_value: Union[
            BoundingBox,
            Category,
            Cuboid2D,
            Keypoints,
            Polygon,
            Polyline,
            RotatedBox,
            dict,
            list,
        ],
        metadata: dict,
        image_id: Optional[str] = None,
        image_key: Optional[str] = None,
        **params,
    ) -> None:
        super(Annotation, self)._init_volatile_fields(
            image_id=image_id,
            image_key=image_key,
            annotation_class=annotation_class,
            annotation_type=annotation_type,
            annotation_value=annotation_value,
            metadata=metadata,
            **params,
        )

        for k, v, is_required in iter(
            [
                (["image_id", "image_key"], [image_id, image_key], True),
                ("annotation_class", annotation_class, True),
                ("annotation_type", annotation_type, True),
                ("annotation_value", annotation_value, True),
                ("metadata", metadata, True),
            ]
        ):
            if isinstance(k, list):
                util.validate_argument_list(keys=k, values=v, is_required=is_required)
                for paired_i in range(len(k)):
                    self[k[paired_i]] = util.convert_to_superb_ai_object(
                        data=v[paired_i]
                    )
            else:
                util.validate_argument_value(key=k, value=v, is_required=is_required)
                self[k] = util.convert_to_superb_ai_object(data=v)

        if not annotation_type:
            raise error.ValidationError("annotation_type is required")

        if isinstance(annotation_value, (dict, list)) and not isinstance(
            annotation_value, AnnotationType
        ):
            self._init_annotation_value(annotation_type, annotation_value)

    def _init_annotation_value(
        self,
        annotation_type: str,
        annotation_value: Union[
            BoundingBox,
            Category,
            Cuboid2D,
            Keypoints,
            Polygon,
            Polyline,
            RotatedBox,
        ],
        **params,
    ):
        annotation_type_cls = self.get_cls_by_discriminator(
            field="annotation_value", data={"annotation_type": annotation_type}
        )
        self.annotation_value = annotation_type_cls(raw_data=annotation_value)

    @classmethod
    def create(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        image_id: Optional[str] = None,
        image_key: Optional[str] = None,
        annotation_class: str,
        annotation_type: str,
        annotation_value: Union[
            BoundingBox,
            Cuboid2D,
            Category,
            Keypoints,
            Polygon,
            Polyline,
            RotatedBox,
        ],
        metadata: dict,
    ) -> Annotation:
        """
        Not implemented.
        Annotations can be created by ``Annotation.create_bulk()`` or ``Dataset.add_annotations()``.
        """
        raise NotImplementedError

    @classmethod
    def create_bulk(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        annotations: List[Annotation],
        asynchronous: bool = True,
    ) -> Job:
        """
        Creates a job that adds newly initialized annotations to a dataset.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to add the annotations to.
        annotations
            Newly initialized annotations to add.
        asynchronous
            Whether to immediately return the job after creating it.
            If set to ``False``, the function waits for the job to finish before returning.
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

        raw_annotations = []

        # Ensure the annotation_values are converted to "raw" form
        for annotation in annotations:
            raw_annotations.append(annotation.to_dict_deep())

        uploaded_param = Job._upload_params(
            access_key=access_key,
            team_name=team_name,
            data=raw_annotations,
        )

        job = Job.create(
            access_key=access_key,
            team_name=team_name,
            job_type=JobType.ANNOTATION_IMPORT,
            param={
                "dataset_id": dataset_id,
                "annotations": {"param_id": uploaded_param["id"]},
            },
        )

        if not asynchronous:
            job.wait_until_complete()

        return job

    def delete(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
    ) -> None:
        """
        Deletes the annotation.

        Parameters
        ----------
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.
        """
        endpoint_params = {
            "dataset_id": self.dataset_id,
            "id": self.id,
        }

        super(Annotation, self).delete(
            access_key=access_key, team_name=team_name, endpoint_params=endpoint_params
        )

    @classmethod
    def fetch(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        id: str,
        dataset_id: str,
        image_id: str,
    ) -> Annotation:
        """
        Fetches an annotation.

        Parameters
        ----------
        id
            The ID of the annotation to fetch.
        dataset_id
            The ID of the dataset with the image to fetch the annotation from.
        image_id
            The ID of the image to fetch the annotation from.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The fetched annotation.
        """
        endpoint_params = {"id": id, "dataset_id": dataset_id, "image_id": image_id}

        return super(Annotation, cls).fetch(
            access_key=access_key, team_name=team_name, endpoint_params=endpoint_params
        )

    @classmethod
    def fetch_all(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        query: Optional[str] = None,
        slice: Optional[str] = None,
        annotation_classes: Optional[List[str]] = None,
        annotation_types: Optional[List[str]] = None,
    ) -> List[Annotation]:
        """
        Fetches annotations in a dataset that match the provided filters.
        If filters are not provided, fetches all annotations.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch annotations from.
        query
            A query string to filter the annotations to fetch.
        slice
            The name of a slice to fetch annotations from.
        annotation_classes
            A list of annotation classes to filter the annotations to fetch.
        annotation_types
            A list of annotation types to filter the annotations to fetch.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            Matching annotations.

        Raises
        ------
        NotFoundError
            When a dataset with the provided ``dataset_id`` does not exist.
        QuerySyntaxError
            When the provided ``query`` is syntactically incorrect.
        """
        all_annotations = []
        for page in cls.fetch_page_iter(
            access_key=access_key,
            team_name=team_name,
            dataset_id=dataset_id,
            query=query,
            slice=slice,
            annotation_classes=annotation_classes,
            annotation_types=annotation_types,
        ):
            all_annotations.extend(page.get("results", []))
        return all_annotations

    @classmethod
    def fetch_all_iter(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        query: Optional[str] = None,
        slice: Optional[str] = None,
        annotation_classes: Optional[List[str]] = None,
        annotation_types: Optional[List[str]] = None,
    ) -> Iterator[Annotation]:
        """
        Iterates through annotations in a dataset that match the provided filters.
        If filters are not provided, iterates through all annotations.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch annotations from.
        query
            A query string to filter the annotations to fetch.
        slice
            The name of a slice to fetch annotations from.
        annotation_classes
            A list of annotation classes to filter the annotations to fetch.
        annotation_types
            A list of annotation types to filter the annotations to fetch.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The matching annotation iterator.

        Yields
        -------
            The next matching annotation.

        Raises
        ------
        NotFoundError
            When a dataset with the provided ``dataset_id`` does not exist.
        QuerySyntaxError
            When the provided ``query`` is syntactically incorrect.
        """
        for fetch_result in Annotation.fetch_page_iter(
            access_key=access_key,
            team_name=team_name,
            dataset_id=dataset_id,
            query=query,
            slice=slice,
            annotation_classes=annotation_classes,
            annotation_types=annotation_types,
        ):
            for annotation in fetch_result.get("results", []):
                yield annotation

    @classmethod
    def fetch_page(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        query: Optional[str] = None,
        slice: Optional[str] = None,
        annotation_classes: Optional[List[str]] = None,
        annotation_types: Optional[List[str]] = None,
        search_after: Optional[str] = None,
        limit: int = 10,
    ) -> Dict[str, Union[int, List[str], List[Annotation]]]:
        """
        Fetches a page of annotations that match the provided filters.
        If filters are not provided, paginates all annotations.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch annotations from.
        query
            A query string to filter the annotations to fetch.
        slice
            The name of a slice to fetch annotations from.
        annotation_classes
            A list of annotation classes to filter the annotations to fetch.
        annotation_types
            A list of annotation types to filter the annotations to fetch.
        search_after
            The ID of an annotation to start the search from.
            If not provided, fetches the first page.
        limit
            The maximum number of annotations in a page.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            A page of matching annotations.

        Raises
        ------
        NotFoundError
            When a dataset with the provided ``dataset_id`` does not exist.
        QuerySyntaxError
            When the provided ``query`` is syntactically incorrect.
        """
        endpoint_params = {"dataset_id": dataset_id}
        params = {"size": limit}

        if query:
            params["query"] = query

        if slice:
            params["slice"] = slice

        if annotation_classes or annotation_types:
            annotation_filters = {}

            if annotation_classes:
                annotation_filters["annotation_class_in"] = annotation_classes

            if annotation_types:
                annotation_filters["annotation_type_in"] = annotation_types

            params["annotation_filters"] = annotation_filters

        if search_after:
            params["search_after"] = [search_after]

        return super(Annotation, cls).fetch_page(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            params=params,
        )

    @classmethod
    def fetch_page_iter(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        query: Optional[str] = None,
        slice: Optional[str] = None,
        annotation_classes: Optional[List[str]] = None,
        annotation_types: Optional[List[str]] = None,
    ) -> Iterator[Dict[str, Union[int, List[str], List[Annotation]]]]:
        """
        Iterates through pages of annotations from a dataset that match the provided
        filters. If filters are not provided, paginates all annotations.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch annotations from.
        query
            A query string to filter the annotations to fetch.
        slice
            The name of a slice to fetch annotations from.
        annotation_classes
            A list of annotation classes to filter the annotations to fetch.
        annotation_types
            A list of annotation types to filter the annotations to fetch.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The matching annotation page iterator.

        Yields
        -------
            The next page of matching annotations.

        Raises
        ------
        NotFoundError
            When a dataset with the provided ``dataset_id`` does not exist.
        QuerySyntaxError
            When the provided ``query`` is syntactically incorrect.
        """

        search_after = None
        page_result = {}
        limit = settings.FETCH_PAGE_LIMIT

        def fetch_result(search_after: Optional[str] = None):
            page_result = cls.fetch_page(
                access_key=access_key,
                team_name=team_name,
                dataset_id=dataset_id,
                query=query,
                slice=slice,
                annotation_classes=annotation_classes,
                annotation_types=annotation_types,
                search_after=search_after,
                limit=limit,
            )
            return page_result

        page_result = fetch_result(search_after=search_after)
        yield page_result

        while len(page_result.get("results", [])) == limit and page_result.get(
            "last", []
        ):
            page_result = fetch_result(search_after=page_result.get("last")[0])
            yield page_result

    def refresh(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
    ) -> None:
        """
        Refreshes the annotation.

        Parameters
        ----------
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.
        """
        endpoint_params = {
            "dataset_id": self.dataset_id,
            "image_id": self.image_id,
            "id": self.id,
        }

        super(Annotation, self).refresh(
            access_key=access_key, team_name=team_name, endpoint_params=endpoint_params
        )

    def modify(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        """
        Modifies the annotation.

        Parameters
        ----------
        metadata
            The metadata associated with the annotation.
            Must be flat (one level deep).
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.
        """
        endpoint_params = {
            "dataset_id": self.dataset_id,
            "id": self.id,
        }
        params = {}

        if metadata:
            params.update({"metadata": metadata})

        super(Annotation, self).modify(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            params=params,
        )


class Dataset(CreateResource, DeleteResource, PaginateResource, ModifyResource):
    _endpoints = {
        "create": "/curate/dataset-core/datasets/",
        "delete": "/curate/dataset-core/datasets/{id}/",
        "fetch": "/curate/dataset-query/datasets/{id}/",
        "paginate": "/curate/dataset-query/datasets/",
        "modify": "/curate/dataset-core/datasets/{id}",
    }
    _endpoints_method = {
        "modify": "patch",
    }
    _object_type = "dataset"
    _supported_image_extensions = SupportedImageFormat.__members__

    @classmethod
    def fetch(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        id: Optional[str] = None,
        name: Optional[str] = None,
        include_image_count: bool = False,
        include_slice_count: bool = False,
    ) -> Dataset:
        """
        Fetches a dataset.

        Parameters
        ----------
        id
            The id of the dataset to fetch.
            Must provide at least one of ``id`` or ``name``.
        name
            The name of the dataset to fetch.
            Must provide at least one of ``id`` or ``name``.
        include_image_count
            Whether to include the count of images in the fetched dataset.
        include_slice_count
            Whether to include the count of slices in the fetched dataset.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The fetched dataset.
        """
        if id is None and name is None:
            raise error.ValidationError("Must provide at least one of id or name.")
        elif id is not None and name is not None:
            raise error.ValidationError("Must provide only one of id or name.")

        if name:
            try:
                return cls.fetch_all(
                    access_key=access_key,
                    team_name=team_name,
                    exact={"name": name},
                    include_image_count=include_image_count,
                    include_slice_count=include_slice_count,
                )[0]
            except IndexError:
                # TODO: Fix error message
                raise error.NotFoundError("Could not find the dataset.") from None

        endpoint_params = {"id": id}
        params = {}

        if include_image_count:
            params.update({"expand": params.get("expand", []) + ["image_count"]})

        if include_slice_count:
            params.update({"expand": params.get("expand", []) + ["slice_count"]})

        return super(Dataset, cls).fetch(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            params=params,
        )

    @classmethod
    def fetch_all(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        exact: Dict[str, Any] = None,
        contains: Dict[str, Any] = None,
        include_image_count: bool = False,
        include_slice_count: bool = False,
    ) -> List[Dataset]:
        """
        Fetches datasets that match the provided filters.
        If filters are not provided, fetches all datasets.

        Parameters
        ----------
        exact
            A dictionary for exact, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        contains
            A dictionary for partial, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        include_image_count
            Whether to include the count of images in the fetched datasets.
        include_slice_count
            Whether to include the count of slices in the fetched datasets.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            Matching datasets.
        """
        all_datasets = []
        for page in cls.fetch_page_iter(
            access_key=access_key,
            team_name=team_name,
            exact=exact,
            contains=contains,
            include_image_count=include_image_count,
            include_slice_count=include_slice_count,
        ):
            all_datasets.extend(page.get("results", []))
        return all_datasets

    @classmethod
    def fetch_all_iter(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        exact: Dict[str, Any] = None,
        contains: Dict[str, Any] = None,
        include_image_count: bool = False,
        include_slice_count: bool = False,
    ) -> Iterator[Dataset]:
        """
        Iterates through datasets that match the provided filters.
        If filters are not provided, iterates through all datasets.

        Parameters
        ----------
        exact
            A dictionary for exact, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        contains
            A dictionary for partial, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        include_image_count
            Whether to include the count of images in the fetched datasets.
        include_slice_count
            Whether to include the count of slices in the fetched datasets.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The matching dataset iterator.

        Yields
        -------
            The next matching dataset.
        """
        for fetch_result in Dataset.fetch_page_iter(
            access_key=access_key,
            team_name=team_name,
            exact=exact,
            contains=contains,
            include_image_count=include_image_count,
            include_slice_count=include_slice_count,
        ):
            for dataset in fetch_result.get("results", []):
                yield dataset

    @classmethod
    def fetch_page(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        exact: Dict[str, Any] = None,
        contains: Dict[str, Any] = None,
        include_image_count: bool = False,
        include_slice_count: bool = False,
        page: int = 1,
        limit: int = 10,
    ) -> Dict[str, Union[int, List[Dataset]]]:
        """
        Fetches a page of datasets that match the provided filters.
        If filters are not provided, paginates all datasets.

        Parameters
        ----------
        exact
            A dictionary for exact, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        contains
            A dictionary for partial, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        include_image_count
            Whether to include the count of images in the fetched datasets.
        include_slice_count
            Whether to include the count of slices in the fetched datasets.
        page
            The page number.
        limit
            The maximum number of datasets in a page.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            A page of matching datasets.
        """
        params = {"size": limit}

        if exact:
            for field, filter in [
                ("name", "name"),
            ]:
                params.update({filter: exact.get(field)})

        if contains:
            for field, filter in [
                ("name", "name_contains"),
            ]:
                params.update({filter: contains.get(field)})

        if include_image_count:
            params.update({"expand": params.get("expand", []) + ["image_count"]})

        if include_slice_count:
            params.update({"expand": params.get("expand", []) + ["slice_count"]})

        if page:
            params["page"] = page

        return super(Dataset, cls).fetch_page(
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
        exact: Dict[str, Any] = None,
        contains: Dict[str, Any] = None,
        include_image_count: bool = False,
        include_slice_count: bool = False,
    ) -> Iterator[Dict[str, Union[int, List[Dataset]]]]:
        """
        Iterates through pages of datasets that match the provided filters.
        If filters are not provided, paginates all datasets.

        Parameters
        ----------
        exact
            A dictionary for exact, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        contains
            A dictionary for partial, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        include_image_count
            Whether to include the count of images in the fetched datasets.
        include_slice_count
            Whether to include the count of slices in the fetched datasets.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The matching dataset page iterator.

        Yields
        -------
            The next page of matching datasets.
        """

        page = 1
        page_result = {}
        limit = settings.FETCH_PAGE_LIMIT

        def fetch_result(page: int):
            page_result = cls.fetch_page(
                access_key=access_key,
                team_name=team_name,
                exact=exact,
                contains=contains,
                include_image_count=include_image_count,
                include_slice_count=include_slice_count,
                page=page,
                limit=limit,
            )
            return page_result

        page_result = fetch_result(page=page)
        yield page_result

        while page * limit < page_result["count"]:
            page += 1
            page_result = fetch_result(page=page)
            yield page_result

    @classmethod
    def create(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        name: str,
        description: str,
    ) -> Dataset:
        """
        Creates a dataset.

        Parameters
        ----------
        name
            The name of the dataset to create.
        description
            The description of the dataset to create.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The created dataset.

        Raises
        ------
        ConflictError
            When a dataset with the provided name already exists.
        """
        params = {
            "name": name,
            "description": description,
        }

        return super(Dataset, cls).create(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=None,
            headers=None,
            params=params,
        )

    def delete(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        force: bool = False,
    ) -> None:
        """
        Deletes the dataset.

        Parameters
        ----------
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Raises
        ------
        ConflictError
            When there are images in the dataset.
            Delete the images before deleting the dataset.
        """
        endpoint_params = {"id": self.id}
        params = {"force": force}

        super(Dataset, self).delete(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            params=params,
        )

    def modify(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> None:
        """
        Modifies the dataset.

        Parameters
        ----------
        name
            The new name for the dataset.
        description
            The new description for the dataset.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.
        """
        endpoint_params = {"id": self.id}
        params = {}

        if name:
            params.update({"name": name})

        if description:
            params.update({"description": description})

        super(Dataset, self).modify(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            params=params,
        )

    def add_images(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        images: List[Image],
        asynchronous: bool = True,
    ) -> Job:
        """
        Creates a job that adds newly initialized images to the dataset.

        Parameters
        ----------
        images
            Newly initialized images to add.
        asynchronous
            Whether to immediately return the job after creating it.
            If set to ``False``, the function waits for the job to finish before returning.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The created job
        """
        return Image.create_bulk(
            access_key=access_key,
            team_name=team_name,
            dataset_id=self.id,
            images=images,
            asynchronous=asynchronous,
        )

    def fetch_images(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        key: Optional[str] = None,
        query: Optional[str] = None,
        slice: Optional[str] = None,
        include_annotations: Optional[bool] = False,
        include_image_url: Optional[bool] = False,
    ) -> List[Image]:
        """
        Fetches images from the dataset that match the provided filters.
        If filters are not provided, fetches all images.

        Parameters
        ----------
        key
            The key of a specific image to fetch.
            If provided, only returns at most 1 matching image.
        query
            A query string to filter the images to fetch.
        slice
            The name of a slice to fetch images from.
        include_annotations
            Whether to include annotations in the fetched images.
        include_image_url
            Whether to include the image URL in the fetched images.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            Matching images.

        Raises
        ------
        NotFoundError
            When the dataset does not exist.
            This could occur if the dataset has been deleted.
        QuerySyntaxError
            When the provided ``query`` is syntactically incorrect.
        """

        return Image.fetch_all(
            access_key=access_key,
            team_name=team_name,
            dataset_id=self.id,
            key=key,
            query=query,
            slice=slice,
            include_annotations=include_annotations,
            include_image_url=include_image_url,
        )

    def fetch_images_iter(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        key: Optional[str] = None,
        query: Optional[str] = None,
        slice: Optional[str] = None,
        include_annotations: Optional[bool] = False,
        include_image_url: Optional[bool] = False,
    ) -> Iterator[Image]:
        """
        Iterates through images in the dataset that match the provided filters.
        If filters are not provided, iterates through all images.

        Parameters
        ----------
        key
            The key of a specific image to fetch.
            If provided, only returns at most 1 matching image.
        query
            A query string to filter the images to fetch.
        slice
            The name of a slice to fetch images from.
        include_annotations
            Whether to include annotations in the fetched images.
        include_image_url
            Whether to include the image URL in the fetched images.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The matching image iterator.

        Yields
        -------
            The next matching image.

        Raises
        ------
        NotFoundError
            When the dataset does not exist.
            This could occur if the dataset has been deleted.
        QuerySyntaxError
            When the provided ``query`` is syntactically incorrect.
        """
        return Image.fetch_all_iter(
            access_key=access_key,
            team_name=team_name,
            dataset_id=self.id,
            key=key,
            query=query,
            slice=slice,
            include_annotations=include_annotations,
            include_image_url=include_image_url,
        )

    def create_slice(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        name: str,
        description: str,
    ) -> Slice:
        """
        Creates a slice in the dataset.

        Parameters
        ----------
        name
            The name of the slice to create.
        description
            The description of the slice to create.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The created slice
        """
        return Slice.create(
            access_key=access_key,
            team_name=team_name,
            dataset_id=self.id,
            name=name,
            description=description,
        )

    def fetch_slice(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        id: Optional[str] = None,
        name: Optional[str] = None,
        include_image_count: bool = False,
    ) -> Slice:
        """
        Fetches a slice from the dataset.

        Parameters
        ----------
        id
            The ID of the slice to fetch.
            Must provide at least one of ``id`` or ``name``.
        name
            The name of the slice to fetch.
            Must provide at least one of ``id`` or ``name``.
        include_image_count
            Whether to include the count of images in the fetched slice.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The fetched slice.
        """
        return Slice.fetch(
            access_key=access_key,
            team_name=team_name,
            id=id,
            name=name,
            dataset_id=self.id,
            include_image_count=include_image_count,
        )

    # TODO: list reference issue, needs a fix.
    def delete_images(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        images: List[Image] = [],
        image_ids: List[str] = [],
        image_keys: List[str] = [],
        asynchronous: bool = True,
    ) -> Job:
        """
        Creates a job that deletes images from the dataset.

        Parameters
        ----------
        images
            Images to delete.
        image_ids
            IDs of images to delete.
        image_keys
            Keys of images to delete.
        asynchronous
            Whether to immediately return the job after creating it.
            If set to ``False``, the function waits for the job to finish before returning.
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
        return Image.delete_bulk(
            access_key=access_key,
            team_name=team_name,
            dataset_id=self.id,
            images=images,
            image_ids=image_ids,
            image_keys=image_keys,
            asynchronous=asynchronous,
        )

    def add_annotations(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        annotations: List[Annotation],
        asynchronous: bool = True,
    ) -> Job:
        """
        Creates a job that adds newly initialized annotations to the dataset.

        Parameters
        ----------
        annotations
            Newly initialized annotations to add.
        asynchronous
            Whether to immediately return the job after creating it.
            If set to ``False``, the function waits for the job to finish before returning.
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
        return Annotation.create_bulk(
            access_key=access_key,
            team_name=team_name,
            dataset_id=self.id,
            annotations=annotations,
            asynchronous=asynchronous,
        )

    def refresh(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
    ) -> None:
        """
        Refreshes the dataset.

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
        params = {}

        if "image_count" in self:
            params.update({"expand": params.get("expand", []) + ["image_count"]})

        if "slice_count" in self:
            params.update({"expand": params.get("expand", []) + ["slice_count"]})

        super(Dataset, self).refresh(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            params=params,
        )

    def upload_images_from_directory(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        directory_path: Union[str, Path],
        recursive: bool = True,
        asynchronous: bool = True,
    ) -> Job:
        """
        Creates a job that uploads image files in the given directory.

        Parameters
        ----------
        directory_path
            The path of the directory to search for image files to upload.
            Supports a string path or a ``Path`` object that points to the directory.
        recursive
            Whether to recursively search through the given directory.
            If set to ``True``, the function searches through nested directories.
        asynchronous
            Whether to immediately return the job after creating it.
            If set to ``False``, the function waits for the job to finish before returning.
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
        if isinstance(directory_path, str):
            directory_path = Path(directory_path)

        if not directory_path.is_dir():
            raise error.ValidationError("The provided path is not a valid directory.")

        if recursive:
            file_iterator = directory_path.rglob("*")
        else:
            file_iterator = directory_path.glob("*")

        images: List[Image] = []

        for file_path in file_iterator:
            if file_path.is_file():
                file_extension = file_path.suffix.upper().lstrip(".")

                if file_extension in self._supported_image_extensions:
                    image_key = str(os.path.relpath(file_path, directory_path))

                    if len(image_key) > settings.MAX_IMAGE_KEY_LENGTH:
                        raise error.ValidationError(
                            f"Automatically generated image key {image_key} exceeds"
                            f" the {settings.MAX_IMAGE_KEY_LENGTH} characters limit."
                        )

                    images.append(
                        Image(
                            key=image_key,
                            source=ImageSourceLocal(asset=file_path),
                            metadata={},
                        )
                    )

        if images:
            return self.add_images(
                access_key=access_key,
                team_name=team_name,
                images=images,
                asynchronous=asynchronous,
            )

        raise error.ValidationError("There are no image files in the given directory.")


class Image(DeleteResource, PaginateResource, ModifyResource):
    _endpoints = {
        "bulk_asset_upload": "/curate/batch/assets/bulk/",
        "delete": "/curate/dataset-core/datasets/{dataset_id}/images/{id}/",
        "fetch": "/curate/dataset-query/datasets/{dataset_id}/images/{id}/",
        "paginate": "/curate/dataset-query/datasets/{dataset_id}/images/_search",
        "modify": "/curate/dataset-core/datasets/{dataset_id}/images/{id}/metadata",
    }
    _endpoints_method = {
        "paginate": "post",
    }
    _object_type = "image"

    def __init__(
        self,
        *,
        key: Optional[str] = None,
        source: Optional[Union[ImageSourceLocal, ImageSourceUrl]] = None,
        metadata: Optional[dict] = None,
        **params,
    ):
        """
        Initializes an image.
        A newly initialized image is incomplete must be added to a dataset.

        Parameters
        ----------
        key
            The unique key of the image to create.
        source
            The source of the image to.
        metadata
            The metadata associated with the image.
            Must be flat (one level deep).
        """
        super(Image, self).__init__(key=key, source=source, metadata=metadata, **params)

    def _init_volatile_fields(
        self,
        key: Optional[str] = None,
        source: Optional[Union[ImageSourceLocal, ImageSourceUrl]] = None,
        metadata: Optional[dict] = None,
        **params,
    ) -> None:
        super(Image, self)._init_volatile_fields(
            key=key, source=source, metadata=metadata, **params
        )

        for k, v, is_required in iter(
            (
                ("key", key, True),
                ("source", source, True),
                ("metadata", metadata, True),
            )
        ):
            if is_required and v is None:
                raise error.ValidationError(f"{k} is required.")
            self[k] = util.convert_to_superb_ai_object(data=v)

    @classmethod
    async def __upload_asset(cls, *, key: str, source: ImageSourceLocal, url: str):
        attempt = 0

        while attempt < http_client.MAX_RETRY_COUNT:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.put(
                        url, data=await source.get_asset()
                    ) as response:
                        if response.status == 200:
                            return
                        elif response.status in http_client.STATUS_FORCE_SET:
                            attempt += 1
                            if attempt < http_client.MAX_RETRY_COUNT:
                                await asyncio.sleep(
                                    http_client.calculate_backoff(attempt=attempt)
                                )
                            else:
                                raise error.RetryableError(
                                    f"Failed to upload after "
                                    f"{http_client.MAX_RETRY_COUNT} attempts. "
                                    f"Status code: {response.status}"
                                )

                        else:
                            raise error.SuperbAIError(
                                "There was an error in uploading the local file of the "
                                f"image with the key '{key}'."
                                f"Status code: {response.status}"
                            )
            except aiohttp.ClientError as e:
                attempt += 1
                if attempt < http_client.MAX_RETRY_COUNT:
                    await asyncio.sleep(http_client.calculate_backoff(attempt=attempt))
                else:
                    raise error.RetryableError(
                        f"Failed to upload after {http_client.MAX_RETRY_COUNT} "
                        f"attempts due to a network error: {str(e)}"
                    )

    @classmethod
    async def __upload_local_images(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        images: List[Image],
    ) -> List[Image]:
        import spb_curate

        bulk_upload_bytes_max = spb_curate.bulk_upload_bytes_max
        bulk_upload_object_max = spb_curate.bulk_upload_object_max

        url = cls.get_endpoint(name="bulk_asset_upload", params=None)
        requestor = api_requestor.APIRequestor(
            access_key=access_key, team_name=team_name
        )

        N = len(images)
        last_i = 0  # slice position for the source list
        i = 0
        total_transfer_size = 0
        file_sizes = []

        while i < N:
            asset_size = images[i].source.get_asset_size()

            if asset_size > settings.UPLOAD_IMAGE_FILE_BYTES_MAX:
                raise error.ValidationError(
                    f"The image with the key '{images[i].key}' has exceeded the file size "
                    f"limit of 20MB."
                )

            total_transfer_size += asset_size
            file_sizes.append(asset_size)

            if (
                total_transfer_size >= bulk_upload_bytes_max
                or len(file_sizes) == bulk_upload_object_max
                or i == (N - 1)
            ):
                # First generate presigned urls
                response, access_key = requestor.request(
                    method="post",
                    url=url,
                    params={"file_sizes": file_sizes},
                    headers=None,
                )

                assets = util.convert_to_superb_ai_object(
                    data=response, access_key=access_key, team_name=team_name
                )

                tasks = []

                # Upload asset data to S3 and save the asset_id
                for asset_i, asset in enumerate(assets.get("results", [])):
                    image: Image = images[last_i + asset_i]
                    tasks.append(
                        cls.__upload_asset(
                            key=image.key,
                            source=image.source,
                            url=asset.get("upload_url"),
                        )
                    )
                    images[last_i + asset_i].source.update(
                        {"asset_id": asset.get("id")}
                    )

                # Run all tasks concurrently
                await asyncio.gather(*tasks)

                util.log_info(f"Uploading local images: {i + 1} of {N}")

                total_transfer_size = 0
                file_sizes = []
                last_i = i + 1

            i += 1

        return images

    @classmethod
    def __run_upload_local_images(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        images: List[Image],
    ) -> List[Image]:
        async_fn = cls.__upload_local_images(
            access_key=access_key, team_name=team_name, images=images
        )

        if util.is_running_in_notebook():
            # In Jupyter Notebook, we check if the event loop is running
            try:
                import nest_asyncio
            except ImportError:
                raise error.DependencyError(
                    "When running in Jupyter Notebook, `nest_asyncio` is required."
                )

            nest_asyncio.apply()
        return asyncio.run(async_fn)

    @classmethod
    def create_bulk(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        images: List[Union[Image, dict]],
        asynchronous: bool = True,
    ) -> Job:
        """
        Creates a job that adds newly initialized images to a dataset.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to add the images to.
        images
            Newly initialized images to add.
        asynchronous
            Whether to immediately return the job after creating it.
            If set to ``False``, the function waits for the job to finish before returning.
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
        param_images: List[Image] = []
        local_images: List[Image] = []

        for _, image in enumerate(images):
            if isinstance(image.get("source"), ImageSourceLocal):
                local_images.append(image)
            else:
                param_images.append(image)

        # Upload all local images to s3 storage and retrieve the asset_ids
        if local_images:
            param_images += cls.__run_upload_local_images(
                access_key=access_key, team_name=team_name, images=local_images
            )

        uploaded_param = Job._upload_params(
            access_key=access_key, team_name=team_name, data=param_images
        )

        job = Job.create(
            access_key=access_key,
            team_name=team_name,
            job_type=JobType.IMAGE_IMPORT,
            param={
                "dataset_id": dataset_id,
                "images": {"param_id": uploaded_param["id"]},
            },
        )

        if not asynchronous:
            job.wait_until_complete()

        return job

    def delete(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
    ) -> None:
        """
        Deletes the image.

        Parameters
        ----------
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.
        """
        endpoint_params = {"dataset_id": self.dataset_id, "id": self.id}

        super(Image, self).delete(
            access_key=access_key, team_name=team_name, endpoint_params=endpoint_params
        )

    @classmethod
    def fetch(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        id: str,
        include_annotations: Optional[bool] = False,
    ) -> Image:
        """
        Fetches an image.

        Parameters
        ----------
        dataset_id
            The ID of the dataset with the image to fetch.
        id
            The ID of the image to fetch.
        include_annotations
            Whether to include annotations in the fetched images.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The fetched image.
        """
        endpoint_params = {"dataset_id": dataset_id, "id": id}
        params = {}

        if include_annotations:
            params.update({"expand": ["annotations"]})

        return super(Image, cls).fetch(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            params=params,
        )

    @classmethod
    def fetch_all(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        key: Optional[str] = None,
        query: Optional[str] = None,
        slice: Optional[str] = None,
        include_annotations: Optional[bool] = False,
        include_image_url: Optional[bool] = False,
    ) -> List[Image]:
        """
        Fetches images in a dataset that match the provided filters.
        If filters are not provided, fetches all images.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch images from.
        key
            The key of a specific image to fetch.
            If provided, only returns at most 1 matching image.
        query
            A query string to filter the images to fetch.
        slice
            The name of a slice to fetch images from.
        include_annotations
            Whether to include annotations in the fetched images.
        include_image_url
            Whether to include the image URL in the fetched images.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            Matching images.

        Raises
        ------
        NotFoundError
            When a dataset with the provided ``dataset_id`` does not exist.
        QuerySyntaxError
            When the provided ``query`` is syntactically incorrect.
        """
        all_images = []
        for page in cls.fetch_page_iter(
            access_key=access_key,
            team_name=team_name,
            dataset_id=dataset_id,
            key=key,
            query=query,
            slice=slice,
            include_annotations=include_annotations,
            include_image_url=include_image_url,
        ):
            all_images.extend(page.get("results", []))
        return all_images

    @classmethod
    def fetch_all_iter(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        key: Optional[str] = None,
        query: Optional[str] = None,
        slice: Optional[str] = None,
        include_annotations: Optional[bool] = False,
        include_image_url: Optional[bool] = False,
    ) -> Iterator[Image]:
        """
        Iterates through images in a dataset that match the provided filters.
        If filters are not provided, iterates through all images.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch images from.
        key
            The key of a specific image to fetch.
            If provided, only returns at most 1 matching image.
        query
            A query string to filter the images to fetch.
        slice
            The name of a slice to fetch images from.
        include_annotations
            Whether to include annotations in the fetched images.
        include_image_url
            Whether to include the image URL in the fetched images.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The matching image iterator.

        Yields
        -------
            The next matching image.

        Raises
        ------
        NotFoundError
            When a dataset with the provided ``dataset_id`` does not exist.
        QuerySyntaxError
            When the provided ``query`` is syntactically incorrect.
        """
        for fetch_result in Image.fetch_page_iter(
            access_key=access_key,
            team_name=team_name,
            dataset_id=dataset_id,
            key=key,
            query=query,
            slice=slice,
            include_annotations=include_annotations,
            include_image_url=include_image_url,
        ):
            for image in fetch_result.get("results", []):
                yield image

    @classmethod
    def fetch_page(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        key: Optional[str] = None,
        query: Optional[str] = None,
        slice: Optional[str] = None,
        include_annotations: Optional[bool] = False,
        include_image_url: Optional[bool] = False,
        search_after: Optional[str] = None,
        limit: int = 10,
    ) -> Dict[str, Union[int, List[str], List[Image]]]:
        """
        Fetches a page of images that match the provided filters.
        If filters are not provided, paginates all images.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch images from.
        key
            The key of a specific image to fetch.
            If provided, only returns at most 1 matching image.
        query
            A query string to filter the images to fetch.
        slice
            The name of a slice to fetch images from.
        include_annotations
            Whether to include annotations in the fetched images.
        search_after
            The ID of an image to start the search from.
            If not provided, fetches the first page.
        limit
            The maximum number of images in a page.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            A page of matching images.

        Raises
        ------
        NotFoundError
            When a dataset with the provided ``dataset_id`` does not exist.
        QuerySyntaxError
            When the provided ``query`` is syntactically incorrect.
        """
        endpoint_params = {"dataset_id": dataset_id}
        params = {"size": limit}

        if key:
            params["key"] = key

        if query:
            params["query"] = query

        if slice:
            params["slice"] = slice

        expand = []

        # TODO: if other expand parameters are added, fix this
        if include_annotations:
            expand.append("annotations")

        if include_image_url:
            expand.append("image_url")

        if expand:
            params["expand"] = expand

        if search_after:
            params["search_after"] = [search_after]

        return super(Image, cls).fetch_page(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            params=params,
        )

    @classmethod
    def fetch_page_iter(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        key: Optional[str] = None,
        query: Optional[str] = None,
        slice: Optional[str] = None,
        include_annotations: Optional[bool] = False,
        include_image_url: Optional[bool] = False,
    ) -> Iterator[Dict[str, Union[int, List[str], List[Image]]]]:
        """
        Iterates through pages of images from a dataset that match the provided filters.
        If filters are not provided, paginates all images.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch images from.
        key
            The key of a specific image to fetch.
            If provided, only returns at most 1 matching image.
        query
            A query string to filter the images to fetch.
        slice
            The name of a slice to fetch images from.
        include_annotations
            Whether to include annotations in the fetched images.
        include_image_url
            Whether to include the image URL in the fetched images.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The matching image page iterator.

        Yields
        -------
            The next page of matching images.

        Raises
        ------
        NotFoundError
            When a dataset with the provided ``dataset_id`` does not exist.
        QuerySyntaxError
            When the provided ``query`` is syntactically incorrect.
        """

        search_after = None
        page_result = {}
        limit = settings.FETCH_PAGE_LIMIT

        def fetch_result(search_after: Optional[str] = None):
            page_result = cls.fetch_page(
                access_key=access_key,
                team_name=team_name,
                dataset_id=dataset_id,
                key=key,
                query=query,
                slice=slice,
                include_annotations=include_annotations,
                include_image_url=include_image_url,
                search_after=search_after,
                limit=limit,
            )
            return page_result

        page_result = fetch_result(search_after=search_after)
        yield page_result

        while len(page_result.get("results", [])) == limit and page_result.get(
            "last", []
        ):
            page_result = fetch_result(search_after=page_result.get("last")[0])
            yield page_result

    @classmethod
    def delete_bulk(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        images: Optional[List[Image]] = None,
        image_ids: Optional[List[str]] = None,
        image_keys: Optional[List[str]] = None,
        asynchronous: bool = True,
    ) -> Job:
        """
        Creates a job that deletes images from the dataset.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to delete images from.
        images
            Images to delete.
        image_ids
            IDs of images to delete.
        image_keys
            Keys of images to delete.
        asynchronous
            Whether to immediately return the job after creating it.
            If set to ``False``, the function waits for the job to finish before returning.
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
        images = [] if not images else images
        image_ids = [] if not image_ids else image_ids
        image_keys = [] if not image_keys else image_keys

        image_ids_and_keys = []

        for image in images:
            image_ids_and_keys.append({"id": image.id})

        for image_id in image_ids:
            image_ids_and_keys.append({"id": image_id})

        for image_key in image_keys:
            image_ids_and_keys.append({"key": image_key})

        uploaded_param = Job._upload_params(
            access_key=access_key,
            team_name=team_name,
            data=image_ids_and_keys,
        )

        job = Job.create(
            access_key=access_key,
            team_name=team_name,
            job_type=JobType.DELETE_IMAGES,
            param={
                "dataset_id": dataset_id,
                "images": {
                    "param_id": uploaded_param["id"],
                },
            },
        )

        if not asynchronous:
            job.wait_until_complete()

        return job

    def refresh(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
    ) -> None:
        """
        Refreshes the image.

        Parameters
        ----------
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.
        """
        endpoint_params = {"dataset_id": self.dataset_id, "id": self.id}
        params = {}

        if "annotations" in self:
            params.update({"expand": ["annotations"]})

        super(Image, self).refresh(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            params=params,
        )

    def modify(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        """
        Modifies the image.

        Parameters
        ----------
        metadata
            The metadata associated with the image.
            Must be flat (one level deep).
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.
        """
        endpoint_params = {"dataset_id": self.dataset_id, "id": self.id}
        params = {}

        if metadata:
            params.update({"metadata": metadata})

        super(Image, self).modify(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            params=params,
        )


class ImageSourceLocal(BaseImageSource):
    _asset: Optional[bytes] = None
    _asset_path: Optional[str] = None
    _asset_size: Optional[int] = None

    def __init__(
        self,
        *,
        asset: Union[bytes, str, Path],
        asset_id: Optional[str] = None,
        **params,
    ):
        """
        Initializes a local image source.

        Parameters
        ----------
        asset
            The data of the image.
            Supports byte array, string path, ``Path``object of the image.
        asset_id
            The ID of the asset.
            Should not be explicitly provided.
        """
        super(ImageSourceLocal, self).__init__(type="LOCAL", params=params)

        if asset:
            if isinstance(asset, str) or isinstance(asset, Path):
                self._asset_path = asset
            else:
                self.__set_asset(asset=asset)

            self._upload_url = None
        else:
            self.asset_id = asset_id

    def __set_asset(self, *, asset: bytes):
        self._asset = asset

    async def get_asset(self) -> bytes:
        return await self.load_asset()

    async def read_file(self) -> bytes:
        if self._asset is not None:
            return self._asset

        if self._asset_path is not None:
            loop = asyncio.get_event_loop()
            with open(self._asset_path, "rb") as fp:
                return await loop.run_in_executor(None, fp.read)

        raise error.ValidationError("Local image file path not supplied.")

    async def load_asset(self) -> bytes:
        if self._asset is None:
            return await self.read_file()

        return self._asset

    def unload_asset(self, *, force: bool = True):
        if force:
            self._asset = None

    def get_asset_size(self) -> int:
        if self._asset_size is None:
            if self._asset is not None:
                self._asset_size = len(self._asset)
            elif self._asset_path is not None:
                self._asset_size = os.path.getsize(self._asset_path)
            else:
                raise error.ValidationError(
                    "Local image file path or bytes not supplied."
                )

        return self._asset_size


class ImageSourceUrl(BaseImageSource):
    __validator_message = "The URL is invalid: {value}"
    __validator_regex = re.compile(
        r"^(?:http|ftp)s?://"  # http:// or https://
        r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|"  # domain...
        r"localhost|"  # localhost...
        r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"  # ...or ip
        r"(?::\d+)?"  # optional port
        r"(?:/?|[/?]\S+)$",
        re.IGNORECASE,
    )

    def __init__(self, *, url: str, **params):
        """
        Initializes a URL image source.

        Parameters
        ----------
        url
            The URL of the image.
        """
        super(ImageSourceUrl, self).__init__(type="URL", params=params)
        self["url"] = self.__validate(value=url)

    def __validate(self, value: str) -> str:
        if (
            not isinstance(value, str)
            or value == ""
            or not self.__validator_regex.search(value)
        ):
            raise error.ValidationError(self.__validator_message.format(value=value))

        return value


class SearchFieldMapping(PaginateResource):
    _endpoints = {
        "paginate": "/curate/dataset-query/datasets/{dataset_id}/search-field-mappings",
    }
    _object_type = "search_field_mapping"

    @classmethod
    def fetch(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
    ) -> SearchFieldMapping:
        """
        Not implemented.
        """
        raise NotImplementedError

    @classmethod
    def fetch_all(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        mapping_type: Optional[SearchFieldMappingType] = None,
    ) -> Dict[str, Union[int, List[SearchFieldMapping], str]]:
        """
        Fetches search field mappings that match the provided filters.
        If filters are not provided, fetches all search field mappings.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch the search field mappings from.
        mapping_type
            A type of search field mappings to filter by.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            Matching search field mappings.
        """
        all_search_field_mappings = []
        for page in cls.fetch_page_iter(
            access_key=access_key,
            team_name=team_name,
            dataset_id=dataset_id,
            mapping_type=mapping_type,
        ):
            all_search_field_mappings.extend(page.get("results", []))
        return all_search_field_mappings

    @classmethod
    def fetch_all_iter(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        mapping_type: Optional[SearchFieldMappingType] = None,
    ) -> Iterator[SearchFieldMapping]:
        """
        Iterates through search field mappings that match the provided filters.
        If filters are not provided, iterates through all search field mappings.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch the search field mappings from.
        mapping_type
            A type of search field mappings to filter by.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The matching search field mapping iterator.

        Yields
        -------
            The next matching search field mapping.
        """
        for page in SearchFieldMapping.fetch_page_iter(
            access_key=access_key,
            team_name=team_name,
            dataset_id=dataset_id,
            mapping_type=mapping_type,
        ):
            for search_field_mapping in page.get("results", []):
                yield search_field_mapping

    @classmethod
    def fetch_page(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        mapping_type: Optional[SearchFieldMappingType] = None,
    ) -> Dict[str, Union[int, List[SearchFieldMapping]]]:
        """
        Fetches a page of search field mappings that match the provided filters.
        If filters are not provided, paginates all search field mappings.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch the search field mappings from.
        mapping_type
            A type of search field mappings to filter by.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            A page of matching search field mappings.
        """
        endpoint_params = {"dataset_id": dataset_id}
        params = {}

        if mapping_type:
            params.update({"mapping_type": mapping_type.value})

        return super(SearchFieldMapping, cls).fetch_page(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            headers=None,
            params=params,
        )

    @classmethod
    def fetch_page_iter(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        mapping_type: Optional[SearchFieldMappingType] = None,
    ) -> Iterator[Dict[str, Union[int, List[SearchFieldMapping]]]]:
        """
        Iterates through pages of search field mappings that match the provided filters.
        If filters are not provided, paginates all search field mappings.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch the search field mappings from.
        mapping_type
            A type of search field mappings to filter by.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The matching search field mapping page iterator.

        Yields
        -------
            The next page of matching search field mappings.
        """
        page_result = {}

        def fetch_result():
            page_result = cls.fetch_page(
                access_key=access_key,
                team_name=team_name,
                dataset_id=dataset_id,
                mapping_type=mapping_type,
            )
            return page_result

        # There is only one page
        page_result = fetch_result()
        yield page_result

    def refresh(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
    ) -> None:
        """
        Not implemented.
        """
        raise NotImplementedError


class Slice(CreateResource, DeleteResource, PaginateResource, ModifyResource):
    _endpoints = {
        "create": "/curate/dataset-core/datasets/{dataset_id}/slices/",
        "delete": "/curate/dataset-core/datasets/{dataset_id}/slices/{id}/",
        "fetch": "/curate/dataset-query/datasets/{dataset_id}/slices/{id}/",
        "paginate": "/curate/dataset-query/datasets/{dataset_id}/slices/",
        "modify": "/curate/dataset-core/datasets/{dataset_id}/slices/{id}/",
    }
    _endpoints_method = {
        "modify": "patch",
    }
    _object_type = "slice"

    @classmethod
    def fetch(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        id: Optional[str] = None,
        name: Optional[str] = None,
        include_image_count: bool = False,
    ) -> Slice:
        """
        Fetches a slice.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch the slice from.
        id
            The ID of the slice to fetch.
            Must provide at least one of ``id`` or ``name``.
        name
            The name of the slice to fetch.
            Must provide at least one of ``id`` or ``name``.
        include_image_count
            Whether to include the count of images in the fetched slice.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The fetched slice.
        """
        if id is None and name is None:
            raise error.ValidationError("Must provide at least one of id or name.")
        elif id is not None and name is not None:
            raise error.ValidationError("Must provide only one of id or name.")

        if name:
            try:
                return cls.fetch_all(
                    access_key=access_key,
                    team_name=team_name,
                    dataset_id=dataset_id,
                    exact={"name": name},
                    include_image_count=include_image_count,
                )[0]
            except IndexError:
                # TODO: Fix error message
                raise error.NotFoundError("Could not find the slice.") from None

        endpoint_params = {"dataset_id": dataset_id, "id": id}
        params = {}

        if include_image_count:
            params.update({"expand": ["image_count"]})

        return super(Slice, cls).fetch(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            params=params,
        )

    @classmethod
    def fetch_all(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        exact: Dict[str, Any] = None,
        contains: Dict[str, Any] = None,
        include_image_count: bool = False,
    ) -> List[Slice]:
        """
        Fetches slices that match the provided filters.
        If filters are not provided, fetches all slices.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch the slices from.
        exact
            A dictionary for exact, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        contains
            A dictionary for partial, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        include_image_count
            Whether to include the count of images in the fetched slice.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            Matching slices.

        Raises
        ------
        NotFoundError
            When a dataset with the provided ``dataset_id`` does not exist.
        """
        all_slices = []
        for page in cls.fetch_page_iter(
            access_key=access_key,
            team_name=team_name,
            dataset_id=dataset_id,
            exact=exact,
            contains=contains,
            include_image_count=include_image_count,
        ):
            all_slices.extend(page.get("results", []))
        return all_slices

    @classmethod
    def fetch_all_iter(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        exact: Dict[str, Any] = None,
        contains: Dict[str, Any] = None,
        include_image_count: bool = False,
    ) -> Iterator[Slice]:
        """
        Iterates through slices that match the provided filters.
        If filters are not provided, iterates through all slices.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch the slices from.
        exact
            A dictionary for exact, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        contains
            A dictionary for partial, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        include_image_count
            Whether to include the count of images in the fetched slice.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The matching slice iterator.

        Yields
        -------
            The next matching slice.

        Raises
        ------
        NotFoundError
            When a dataset with the provided ``dataset_id`` does not exist.
        """
        for fetch_result in Slice.fetch_page_iter(
            access_key=access_key,
            team_name=team_name,
            dataset_id=dataset_id,
            exact=exact,
            contains=contains,
            include_image_count=include_image_count,
        ):
            for slice in fetch_result.get("results", []):
                yield slice

    @classmethod
    def fetch_page(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        exact: Dict[str, Any] = None,
        contains: Dict[str, Any] = None,
        include_image_count: bool = False,
        page: int = 1,
        limit: int = 10,
    ) -> Dict[str, Union[int, List[Slice]]]:
        """
        Fetches a page of slices that match the provided filters.
        If filters are not provided, paginates all slices.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch the slices from.
        exact
            A dictionary for exact, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        contains
            A dictionary for partial, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        include_image_count
            Whether to include the count of images in the fetched slices.
        page
            The page number.
        limit
            The maximum number of slices in a page.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            A page of matching slices.

        Raises
        ------
        NotFoundError
            When a dataset with the provided ``dataset_id`` does not exist.
        """
        endpoint_params = {"dataset_id": dataset_id}
        params = {"size": limit}

        if exact:
            for field, filter in [
                ("name", "name"),
            ]:
                params.update({filter: exact.get(field)})

        if contains:
            for field, filter in [
                ("name", "name_contains"),
            ]:
                params.update({filter: contains.get(field)})

        if include_image_count:
            params.update({"expand": params.get("expand", []) + ["image_count"]})

        if page:
            params["page"] = page

        return super(Slice, cls).fetch_page(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            headers=None,
            params=params,
        )

    @classmethod
    def fetch_page_iter(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        exact: Dict[str, Any] = None,
        contains: Dict[str, Any] = None,
        include_image_count: bool = False,
    ) -> Iterator[Dict[str, Union[int, List[Slice]]]]:
        """
        Iterates through pages of slices that match the provided filters.
        If filters are not provided, paginates all slices.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch the slices from.
        exact
            A dictionary for exact, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        contains
            A dictionary for partial, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        include_image_count
            Whether to include the count of images in the fetched slices.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The matching slice page iterator.

        Yields
        -------
            The next page of matching slices.

        Raises
        ------
        NotFoundError
            When a dataset with the provided ``dataset_id`` does not exist.
        """

        page = 1
        page_result = {}
        limit = settings.FETCH_PAGE_LIMIT

        def fetch_result(page: int):
            page_result = cls.fetch_page(
                access_key=access_key,
                team_name=team_name,
                dataset_id=dataset_id,
                exact=exact,
                contains=contains,
                include_image_count=include_image_count,
                page=page,
                limit=limit,
            )
            return page_result

        page_result = fetch_result(page=page)
        yield page_result

        while page * limit < page_result["count"]:
            page += 1
            page_result = fetch_result(page=page)
            yield page_result

    @classmethod
    def create(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        name: str,
        description: str,
    ) -> Slice:
        """
        Creates a slice.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to create the slice in.
        name
            The name of the slice to create.
        description
            The description of the slice to create.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The created slice.

        Raises
        ------
        ConflictError
            When a slice with the provided name already exists in the dataset.
        """
        endpoint_params = {
            "dataset_id": dataset_id,
        }

        params = {
            "name": name,
            "description": description,
        }

        return super(Slice, cls).create(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            headers=None,
            params=params,
        )

    def add_images(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        images: Optional[List[Image]] = None,
        image_ids: Optional[List[str]] = None,
        image_keys: Optional[List[str]] = None,
        query: Optional[str] = None,
        asynchronous: bool = True,
    ) -> Job:
        """
        Create a job that adds images to the slice.

        Parameters
        ----------
        images
            Images to add.
        image_ids
            IDs of images to add.
        image_keys
            Keys of images to add.
        query
            A query string to filter the images to add.
        asynchronous
            Whether to immediately return the job after creating it.
            If set to ``False``, the function waits for the job to finish before returning.
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
        if not (images or image_ids or image_keys) and query is None:
            raise error.ValidationError(
                "Must provide at least one of query or images|image_ids|image_keys."
            )
        elif (images or image_ids or image_keys) and query is not None:
            raise error.ValidationError(
                "Must provide only one of query or images|image_ids|image_keys."
            )

        if query is not None:
            job = Job.create(
                access_key=access_key,
                team_name=team_name,
                job_type=JobType.UPDATE_SLICE_BY_QUERY,
                param={
                    "dataset_id": self.dataset_id,
                    "image_filters": {
                        "slice": None,
                        "query": query,
                    },
                    "slice_id": self.id,
                    "remove": False,
                },
            )
        else:
            images = [] if not images else images
            image_ids = [] if not image_ids else image_ids
            image_keys = [] if not image_keys else image_keys

            image_ids_and_keys = []

            for image in images:
                image_ids_and_keys.append({"id": image.id})

            for image_id in image_ids:
                image_ids_and_keys.append({"id": image_id})

            for image_key in image_keys:
                image_ids_and_keys.append({"key": image_key})

            uploaded_param = Job._upload_params(
                access_key=access_key,
                team_name=team_name,
                data=image_ids_and_keys,
            )

            job = Job.create(
                access_key=access_key,
                team_name=team_name,
                job_type=JobType.UPDATE_SLICE,
                param={
                    "dataset_id": self.dataset_id,
                    "images": {
                        "param_id": uploaded_param["id"],
                    },
                    "slice_id": self.id,
                    "remove": False,
                },
            )

        if not asynchronous:
            job.wait_until_complete()

        return job

    def remove_images(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        images: Optional[List[Image]] = None,
        image_ids: Optional[List[str]] = None,
        image_keys: Optional[List[str]] = None,
        query: Optional[str] = None,
        asynchronous: bool = True,
    ) -> Job:
        """
        Create a job that removes images from the slice.

        Parameters
        ----------
        images
            Images to remove.
        image_ids
            IDs of images to remove.
        image_keys
            Keys of images to remove.
        query
            A query string to filter the images to remove.
        asynchronous
            Whether to immediately return the job after creating it.
            If set to ``False``, the function waits for the job to finish before returning.
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
        if not (images or image_ids or image_keys) and query is None:
            raise error.ValidationError(
                "Must provide at least one of query or images|image_ids|image_keys."
            )
        elif (images or image_ids or image_keys) and query is not None:
            raise error.ValidationError(
                "Must provide only one of query or images|image_ids|image_keys."
            )

        if query is not None:
            job = Job.create(
                access_key=access_key,
                team_name=team_name,
                job_type=JobType.UPDATE_SLICE_BY_QUERY,
                param={
                    "dataset_id": self.dataset_id,
                    "image_filters": {
                        "slice": None,
                        "query": query,
                    },
                    "slice_id": self.id,
                    "remove": True,
                },
            )
        else:
            images = [] if not images else images
            image_ids = [] if not image_ids else image_ids
            image_keys = [] if not image_keys else image_keys

            image_ids_and_keys = []

            for image in images:
                image_ids_and_keys.append({"id": image.id})

            for image_id in image_ids:
                image_ids_and_keys.append({"id": image_id})

            for image_key in image_keys:
                image_ids_and_keys.append({"key": image_key})

            uploaded_param = Job._upload_params(
                access_key=access_key,
                team_name=team_name,
                data=image_ids_and_keys,
            )

            job = Job.create(
                access_key=access_key,
                team_name=team_name,
                job_type=JobType.UPDATE_SLICE,
                param={
                    "dataset_id": self.dataset_id,
                    "images": {"param_id": uploaded_param["id"]},
                    "slice_id": self.id,
                    "remove": True,
                },
            )

        if not asynchronous:
            job.wait_until_complete()

        return job

    def delete(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
    ) -> None:
        """
        Deletes the slice.

        Parameters
        ----------
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.
        """
        endpoint_params = {"dataset_id": self.dataset_id, "id": self.id}

        super(Slice, self).delete(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            params=None,
        )

    def fetch_images(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        key: Optional[str] = None,
        query: Optional[str] = None,
        include_annotations: Optional[bool] = False,
        include_image_url: Optional[bool] = False,
    ) -> List[Image]:
        """
        Fetches images from the slice that match the provided filters.
        If filters are not provided, fetches all images.

        Parameters
        ----------
        key
            The key of a specific image to fetch.
            If provided, only returns at most 1 matching image.
        query
            A query string to filter the images to fetch.
        include_annotations
            Whether to include annotations in the fetched images.
        include_image_url
            Whether to include the image URL in the fetched images.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            Matching images.

        Raises
        ------
        NotFoundError
            When the slice or the dataset does not exist.
            This could occur if the slice or the dataset has been deleted.
        QuerySyntaxError
            When the provided ``query`` is syntactically incorrect.
        """

        return Image.fetch_all(
            access_key=access_key,
            team_name=team_name,
            dataset_id=self.dataset_id,
            key=key,
            query=query,
            slice=self.name,
            include_annotations=include_annotations,
            include_image_url=include_image_url,
        )

    def fetch_images_iter(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        key: Optional[str] = None,
        query: Optional[str] = None,
        include_annotations: Optional[bool] = False,
        include_image_url: Optional[bool] = False,
    ) -> Iterator[Image]:
        """
        Iterates through images in the slice that match the provided filters.
        If filters are not provided, iterates through all images.

        Parameters
        ----------
        key
            The key of a specific image to fetch.
            If provided, only returns at most 1 matching image.
        query
            A query string to filter the images to fetch.
        include_annotations
            Whether to include annotations in the fetched images.
        include_image_url
            Whether to include the image URL in the fetched images.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The matching image iterator.

        Yields
        -------
            The next matching image.

        Raises
        ------
        NotFoundError
            When the slice or the dataset does not exist.
            This could occur if the slice or the dataset has been deleted.
        QuerySyntaxError
            When the provided ``query`` is syntactically incorrect.
        """

        return Image.fetch_all_iter(
            access_key=access_key,
            team_name=team_name,
            dataset_id=self.dataset_id,
            key=key,
            query=query,
            slice=self.name,
            include_annotations=include_annotations,
            include_image_url=include_image_url,
        )

    def refresh(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
    ) -> None:
        """
        Refreshes the slice.

        Parameters
        ----------
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.
        """
        endpoint_params = {"dataset_id": self.dataset_id, "id": self.id}
        params = {}

        if "image_count" in self:
            params.update({"expand": ["image_count"]})

        super(Slice, self).refresh(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            params=params,
        )

    def modify(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> None:
        """
        Modifies the slice.

        Parameters
        ----------
        description
            The new description for the slice.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.
        """
        endpoint_params = {"dataset_id": self.dataset_id, "id": self.id}
        params = {}

        if description:
            params.update({"description": description})

        super(Slice, self).modify(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            params=params,
        )


def create_dataset(
    *,
    access_key: Optional[str] = None,
    team_name: Optional[str] = None,
    name: str,
    description: str,
) -> Dataset:
    """
    Creates a dataset.

    Parameters
    ----------
    name
        The name of the dataset to create.
    description
        The description of the dataset to create.
    access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

    Returns
    -------
        The created dataset.

    Raises
    ------
    ConflictError
        When a dataset with the provided name already exists.
    """
    return Dataset.create(
        access_key=access_key,
        team_name=team_name,
        name=name,
        description=description,
    )


def fetch_dataset(
    *,
    access_key: Optional[str] = None,
    team_name: Optional[str] = None,
    id: Optional[str] = None,
    name: Optional[str] = None,
    include_image_count: bool = False,
    include_slice_count: bool = False,
) -> Dataset:
    """
    Fetches a dataset.

    Parameters
    ----------
    id
        The id of the dataset to fetch.
        Must provide at least one of ``id`` or ``name``.
    name
        The name of the dataset to fetch.
        Must provide at least one of ``id`` or ``name``.
    include_image_count
        Whether to include the count of images in the fetched dataset.
    include_slice_count
        Whether to include the count of slices in the fetched dataset.
    access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

    Returns
    -------
        The fetched dataset.
    """
    return Dataset.fetch(
        access_key=access_key,
        team_name=team_name,
        id=id,
        name=name,
        include_image_count=include_image_count,
        include_slice_count=include_slice_count,
    )


def fetch_datasets(
    *,
    access_key: Optional[str] = None,
    team_name: Optional[str] = None,
    exact: Dict[str, Any] = None,
    contains: Dict[str, Any] = None,
    include_image_count: bool = False,
    include_slice_count: bool = False,
) -> List[Dataset]:
    """
    Fetches datasets that match the provided filters.
    If filters are not provided, fetches all datasets.

    Parameters
    ----------
    exact
        A dictionary for exact, case-sensitive filtering.
        Must provide field names as keys and their desired values.
        Supported fields: ``name``.
    contains
        A dictionary for partial, case-sensitive filtering.
        Must provide field names as keys and their desired values.
        Supported fields: ``name``.
    include_image_count
        Whether to include the count of images in the fetched datasets.
    include_slice_count
        Whether to include the count of slices in the fetched datasets.
    access_key
        An access key for request authentication.
        If provided, overrides the configuration.
    team_name
        A team name for request authentication.
        If provided, overrides the configuration.

    Returns
    -------
        Matching datasets.
    """

    return Dataset.fetch_all(
        access_key=access_key,
        team_name=team_name,
        exact=exact,
        contains=contains,
        include_image_count=include_image_count,
        include_slice_count=include_slice_count,
    )
