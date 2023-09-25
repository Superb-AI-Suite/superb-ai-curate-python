from __future__ import annotations

import decimal
from typing import Any, Dict, Iterator, List, Optional, Union

from spb_curate import error, util
from spb_curate.abstract.api.resource import (
    CreateResource,
    ModifyResource,
    PaginateResource,
)
from spb_curate.abstract.superb_ai_object import SuperbAIObject
from spb_curate.curate.api import settings
from spb_curate.curate.api.curate import Job
from spb_curate.curate.api.enums import JobType
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


class Diagnosis(CreateResource, PaginateResource):
    _endpoints = {
        "create": "/curate/model-diagnosis/datasets/{dataset_id}/diagnoses/",
        "fetch": "/curate/model-diagnosis/datasets/{dataset_id}/diagnoses/{id}/",
        "paginate": "/curate/model-diagnosis/datasets/{dataset_id}/diagnoses/_search/",
    }
    _object_type = "diagnosis"

    @classmethod
    def create(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        model_name: str,
    ) -> Diagnosis:
        """
        Creates a diagnosis.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to use for the diagnosis.
        model_name
            The name of the model to diagnose.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The created diagnosis.

        Raises
        ------
        ConflictError
            When a diagnosis related to the dataset and model already exists.
        """
        endpoint_params = {"dataset_id": dataset_id}
        params = {
            "model_name": model_name,
            "model_source": "external",
        }

        return super(Diagnosis, cls).create(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            headers=None,
            params=params,
        )

    @classmethod
    def fetch(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        id: Optional[str] = None,
        model_name: Optional[str] = None,
    ) -> Diagnosis:
        """
        Fetches a diagnosis.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch the diagnosis from.
        id
            The ID of the diagnosis to fetch.
            Must provide at least one of ``id`` or ``model_name``.
        model_name
            The name of the model associated with the dataset's diagnosis.
            Must provide at least one of ``id`` or ``model_name``.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The fetched diagnosis.
        """
        if id is None and model_name is None:
            raise error.ValidationError(
                "Must provide at least one of id or model_name."
            )
        elif id is not None and model_name is not None:
            raise error.ValidationError("Must provide only one of id or model_name.")

        if model_name:
            try:
                return cls.fetch_all(
                    access_key=access_key,
                    team_name=team_name,
                    dataset_id=dataset_id,
                    exact={"model_name": model_name},
                )[0]
            except IndexError:
                # TODO: Fix error message
                raise error.NotFoundError("Could not find the diagnosis.") from None

        endpoint_params = {"dataset_id": dataset_id, "id": id}

        return super(Diagnosis, cls).fetch(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
        )

    @classmethod
    def fetch_all(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        exact: Dict[str, Any] = None,
    ) -> List[Diagnosis]:
        """
        Fetches diagnoses that match the provided filters.
        If filters are not provided, fetches all diagnoses.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch the diagnosis from.
        exact
            A dictionary for exact, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``model_name``.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            Matching diagnoses.
        """
        all_diagnoses = []
        for page in cls.fetch_page_iter(
            access_key=access_key,
            team_name=team_name,
            dataset_id=dataset_id,
            exact=exact,
        ):
            all_diagnoses.extend(page.get("results", []))
        return all_diagnoses

    @classmethod
    def fetch_all_iter(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        exact: Dict[str, Any] = None,
    ) -> Iterator[Diagnosis]:
        """
        Iterates through diagnoses that match the provided filters.
        If filters are not provided, iterates through all diagnoses.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch the diagnosis from.
        exact
            A dictionary for exact, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``model_name``.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The matching diagnosis iterator.

        Yields
        -------
            The next matching diagnosis.
        """
        for fetch_result in Diagnosis.fetch_page_iter(
            access_key=access_key,
            team_name=team_name,
            dataset_id=dataset_id,
            exact=exact,
        ):
            for diagnosis in fetch_result.get("results", []):
                yield diagnosis

    @classmethod
    def fetch_page(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        exact: Dict[str, Any] = None,
        page: int = 1,
        limit: int = 10,
    ) -> Dict[str, Union[int, List[Diagnosis]]]:
        """
        Fetches a page of diagnoses that match the provided filters.
        If filters are not provided, paginates all diagnoses.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch the diagnosis from.
        exact
            A dictionary for exact, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``model_name``.
        page
            The page number.
        limit
            The maximum number of diagnoses in a page.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            A page of matching diagnoses.
        """
        endpoint_params = {"dataset_id": dataset_id}
        params = {"size": limit}

        if exact:
            for field, filter in [
                ("model_name", "model_name"),
            ]:
                params.update({filter: exact.get(field)})

        if page:
            params["page"] = page

        return super(Diagnosis, cls).fetch_page(
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
    ) -> Iterator[Dict[str, Union[int, List[Diagnosis]]]]:
        """
        Iterates through pages of diagnoses that match the provided filters.
        If filters are not provided, paginates all diagnoses.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch the diagnosis from.
        exact
            A dictionary for exact, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``model_name``.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The matching diagnosis page iterator.

        Yields
        -------
            The next page of matching diagnoses.
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

    def add_evaluations(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        evaluations: List[Evaluation],
        asynchronous: bool = True,
    ) -> Job:
        """
        Creates a job that adds newly initialized evaluations to the diagnosis.

        Parameters
        ----------
        evaluations
            Newly initialized evaluations to add.
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
        return Evaluation.create_bulk(
            access_key=access_key,
            team_name=team_name,
            dataset_id=self.dataset_id,
            diagnosis_id=self.id,
            evaluations=evaluations,
            asynchronous=asynchronous,
        )

    def refresh(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
    ) -> None:
        """
        Refreshes the diagnosis.

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

        super(Diagnosis, self).refresh(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
        )


class Evaluation(CreateResource):
    _object_type = "evaluation"

    def __init__(
        self,
        *,
        image_id: str,
        predictions: List[Prediction] = [],
        **params,
    ):
        """
        Initializes an evaluation.
        A newly initialized evaluation is incomplete and must be added to a diagnosis.

        Parameters
        ----------
        image_id
            The ID of the image to associate the evaluation to.
        predictions
            Newly initialized predictions to add.
        """

        super(Prediction, self).__init__(
            image_id=image_id,
            predictions=predictions,
            **params,
        )

    @classmethod
    def create(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
    ) -> Evaluation:
        """
        Not implemented.
        Evaluations can be created by ``Evaluation.create_bulk()`` or ``Diagnosis.add_evaluations()``.
        """
        raise NotImplementedError

    @classmethod
    def create_bulk(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        diagnosis_id: str,
        evaluations: List[Evaluation],
        asynchronous: bool = True,
    ) -> Job:
        """
        Creates a job that adds newly initialized evaluations to a diagnosis.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to add the evaluations to.
        diagnosis_id
            The ID of the diagnosis to add the evaluations to.
        evaluations
            Newly initialized evaluations to add.
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

        raw_evaluations = []

        # Ensure the evaluation_values are converted to "raw" form
        for evaluation in evaluations:
            raw_evaluations.append(evaluation.to_dict_deep())

        uploaded_param = Job._upload_params(
            access_key=access_key,
            team_name=team_name,
            data=raw_evaluations,
        )

        job = Job.create(
            access_key=access_key,
            team_name=team_name,
            job_type=JobType.EVALUATION_IMPORT,
            param={
                "dataset_id": dataset_id,
                "diagnosis_id": diagnosis_id,
                "evaluations": {"param_id": uploaded_param["id"]},
            },
        )

        if not asynchronous:
            job.wait_until_complete()

        return job


class _Model(PaginateResource, ModifyResource):
    _endpoints = {
        "modify": "/curate/model-diagnosis/models/{id}/",
        "paginate": "/curate/model-diagnosis/models/",
    }
    _endpoints_method = {
        "modify": "patch",
    }
    _object_type = "model"

    @classmethod
    def fetch(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        id: Optional[str] = None,
        name: Optional[str] = None,
    ) -> _Model:
        """
        Fetches a model.

        Parameters
        ----------
        id
            The ID of the model to fetch.
            Must provide at least one of ``id`` or ``name``.
        name
            The name of the model.
            Must provide at least one of ``id`` or ``name``.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The fetched model.
        """
        if id is None and name is None:
            raise error.ValidationError("Must provide at least one of id or name.")
        elif id is not None and name is not None:
            raise error.ValidationError("Must provide only one of id or name.")

        exact = {}

        if id:
            exact.update({"id": id})

        if name:
            exact.update({"name": name})

        try:
            return cls.fetch_all(
                access_key=access_key,
                team_name=team_name,
                exact=exact,
            )[0]
        except IndexError:
            # TODO: Fix error message
            raise error.NotFoundError("Could not find the model.") from None

    @classmethod
    def fetch_all(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        exact: Dict[str, Any] = None,
    ) -> List[_Model]:
        """
        Fetches models that match the provided filters.
        If filters are not provided, fetches all models.

        Parameters
        ----------
        exact
            A dictionary for exact, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            Matching models.
        """
        all_models = []
        for page in cls.fetch_page_iter(
            access_key=access_key,
            team_name=team_name,
            exact=exact,
        ):
            all_models.extend(page.get("results", []))
        return all_models

    @classmethod
    def fetch_all_iter(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        exact: Dict[str, Any] = None,
    ) -> Iterator[_Model]:
        """
        Iterates through models that match the provided filters.
        If filters are not provided, iterates through all models.

        Parameters
        ----------
        exact
            A dictionary for exact, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The matching model iterator.

        Yields
        -------
            The next matching models.
        """
        for fetch_result in _Model.fetch_page_iter(
            access_key=access_key,
            team_name=team_name,
            exact=exact,
        ):
            for model in fetch_result.get("results", []):
                yield model

    @classmethod
    def fetch_page(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        exact: Dict[str, Any] = None,
        page: int = 1,
        limit: int = 10,
    ) -> Dict[str, Union[int, List[_Model]]]:
        """
        Fetches a page of models that match the provided filters.
        If filters are not provided, paginates all models.

        Parameters
        ----------
        exact
            A dictionary for exact, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        page
            The page number.
        limit
            The maximum number of diagnoses in a page.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            A page of matching diagnoses.
        """
        params = {"size": limit}

        if exact:
            for field, filter in [
                ("name", "name"),
            ]:
                params.update({filter: exact.get(field)})

        if page:
            params["page"] = page

        return super(_Model, cls).fetch_page(
            access_key=access_key,
            team_name=team_name,
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
    ) -> Iterator[Dict[str, Union[int, List[_Model]]]]:
        """
        Iterates through pages of models that match the provided filters.
        If filters are not provided, paginates all models.

        Parameters
        ----------
        exact
            A dictionary for exact, case-sensitive filtering.
            Must provide field names as keys and their desired values.
            Supported fields: ``name``.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Returns
        -------
            The matching model page iterator.

        Yields
        -------
            The next page of matching models.
        """

        page = 1
        page_result = {}
        limit = settings.FETCH_PAGE_LIMIT

        def fetch_result(page: int):
            page_result = cls.fetch_page(
                access_key=access_key,
                team_name=team_name,
                exact=exact,
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

    def modify(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        name: Optional[str] = None,
    ) -> None:
        """
        Modifies the model.

        Parameters
        ----------
        name
            The new name for the model.
        access_key
            An access key for request authentication.
            If provided, overrides the configuration.
        team_name
            A team name for request authentication.
            If provided, overrides the configuration.

        Raises
        ------
        ConflictError
            When a model with the provided name already exists.
        """
        if name is None:
            return

        endpoint_params = {"id": self.id}
        params = {"name": name}

        super(_Model, self).modify(
            access_key=access_key,
            team_name=team_name,
            endpoint_params=endpoint_params,
            params=params,
        )


class Prediction(SuperbAIObject):
    _discriminator_map = {
        "prediction_value": (
            "prediction_type",
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
    _field_initializers = {"prediction_value": "_init_prediction_value"}

    def __init__(
        self,
        *,
        prediction_class: str,
        prediction_confidence: Union[int, float, decimal.Decimal],
        prediction_value: Union[
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
        prediction_type: Optional[str] = None,
        **params,
    ):
        """
        Initializes a prediction.
        A newly initialized prediction is incomplete and must be added to an evaluation.

        Parameters
        ----------
        prediction_class
            The classification of the prediction (e.g. "person", "vehicle").
        prediction_value
            The value of the prediction.
        prediction_type
            The type of the prediction (e.g. "box", "polygon").
            Will be inferred if ``prediction_value`` is an instance of ``AnnotationType``.
        """

        super(Prediction, self).__init__(
            prediction_class=prediction_class,
            prediction_confidence=prediction_confidence,
            prediction_value=prediction_value,
            prediction_type=prediction_value._object_type
            if isinstance(prediction_value, AnnotationType)
            else prediction_type,
            **params,
        )

    def _init_volatile_fields(
        self,
        prediction_class: str,
        prediction_confidence: Union[int, float, decimal.Decimal],
        prediction_value: Union[
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
        prediction_type: Optional[str] = None,
        **params,
    ) -> None:
        super(Prediction, self)._init_volatile_fields(
            prediction_class=prediction_class,
            prediction_confidence=prediction_confidence,
            prediction_value=prediction_value,
            prediction_type=prediction_type,
            **params,
        )

        for k, v, is_required in iter(
            [
                ("prediction_class", prediction_class, True),
                ("prediction_confidence", prediction_confidence, True),
                ("prediction_type", prediction_type, True),
                ("prediction_value", prediction_value, True),
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

        if not prediction_type:
            raise error.ValidationError("prediction_type is required")

        if isinstance(prediction_value, (dict, list)) and not isinstance(
            prediction_value, AnnotationType
        ):
            self._init_prediction_value(prediction_type, prediction_value)

    def _init_prediction_value(
        self,
        prediction_type: str,
        prediction_value: Union[
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
        prediction_type_cls = self.get_cls_by_discriminator(
            field="prediction_value", data={"prediction_type": prediction_type}
        )
        self.prediction_value = prediction_type_cls(raw_data=prediction_value)


def fetch_available_models(
    *,
    access_key: Optional[str] = None,
    team_name: Optional[str] = None,
) -> Dict[str, str]:
    return [
        model.to_dict_deep()
        for model in _Model.fetch_all(access_key=access_key, team_name=team_name)
    ]


def modify_model_name(
    *,
    access_key: Optional[str] = None,
    team_name: Optional[str] = None,
    id: str,
    name: str,
):
    """
    Modifies the model.

    Parameters
    ----------
    id
        The id of the model to modify.
    name
        The new name for the model.
    access_key
        An access key for request authentication.
        If provided, overrides the configuration.
    team_name
        A team name for request authentication.
        If provided, overrides the configuration.

    Raises
    ------
    ConflictError
        When a model with the provided name already exists.
    """
    _Model(id=id).modify(
        access_key=access_key,
        team_name=team_name,
        name=name,
    )
