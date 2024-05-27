from __future__ import annotations

import decimal
import json
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
from spb_curate.curate.api.enums import IouType, JobType, Split
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
        "paginate": "/curate/model-diagnosis/datasets/{dataset_id}/diagnoses/",
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
        class_list: List[str],
        iou_type: IouType,
        metadata: Optional[Dict[str, Union[int, float, decimal.Decimal]]] = None,
    ) -> Diagnosis:
        """
        Creates a diagnosis.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to use for the diagnosis.
        model_name
            The name of the model to diagnose.
        class_list
            The list of class names to diagnose.
        iou_type
            The IoU type of the diagnosis.
        metadata
            The metadata associated with the diagnosis.
            Supported fields: ``beta``, ``target_iou``.
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
        param_metadata = {
            "beta": 1.0,
            "class_list": class_list,
            "target_iou": 0.5,
        }

        if metadata:
            param_metadata["beta"] = metadata.get("beta", 1.0)
            param_metadata["target_iou"] = metadata.get("target_iou", 0.5)

        params = {
            "metadata": json.dumps(param_metadata),
            "iou_type": iou_type,
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
        id: str,
    ) -> Diagnosis:
        """
        Fetches a diagnosis.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch the diagnosis from.
        id
            The ID of the diagnosis to fetch.
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
    ) -> List[Diagnosis]:
        """
        Fetches diagnoses that match the provided filters.
        If filters are not provided, fetches all diagnoses.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch the diagnosis from.
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
    ) -> Iterator[Diagnosis]:
        """
        Iterates through diagnoses that match the provided filters.
        If filters are not provided, iterates through all diagnoses.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch the diagnosis from.
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
    ) -> Iterator[Dict[str, Union[int, List[Diagnosis]]]]:
        """
        Iterates through pages of diagnoses that match the provided filters.
        If filters are not provided, paginates all diagnoses.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to fetch the diagnosis from.
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

    def add_predictions(
        self,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        predictions: List[Prediction],
        split: Split = Split.VAL,
        asynchronous: bool = True,
    ) -> Job:
        """
        Creates a job that adds newly initialized predictions to the diagnosis.

        Parameters
        ----------
        predictions
            Newly initialized predictions to add.
        split
            The subset data type used for training the model.
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
        return Prediction.create_bulk(
            access_key=access_key,
            team_name=team_name,
            dataset_id=self.dataset_id,
            diagnosis_id=self.id,
            predictions=predictions,
            split=split,
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


class Evaluation(SuperbAIObject):
    _object_type = "evaluation"


class _Model(PaginateResource, ModifyResource):
    _endpoints = {
        "modify": "/curate/model-diagnosis/external-models/{id}/",
        "paginate": "/curate/model-diagnosis/external-models/",
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
    ) -> _Model:
        raise NotImplementedError

    @classmethod
    def fetch_all(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
    ) -> List[_Model]:
        """
        Fetches models that match the provided filters.
        If filters are not provided, fetches all models.

        Parameters
        ----------
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
        ):
            all_models.extend(page.get("results", []))
        return all_models

    @classmethod
    def fetch_all_iter(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
    ) -> Iterator[_Model]:
        """
        Iterates through models that match the provided filters.
        If filters are not provided, iterates through all models.

        Parameters
        ----------
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
        ):
            for model in fetch_result.get("results", []):
                yield model

    @classmethod
    def fetch_page(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        page: int = 1,
        limit: int = 10,
    ) -> Dict[str, Union[int, List[_Model]]]:
        """
        Fetches a page of models that match the provided filters.
        If filters are not provided, paginates all models.

        Parameters
        ----------
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

        if page:
            params["page"] = page

        return super(_Model, cls).fetch_page(
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
    ) -> Iterator[Dict[str, Union[int, List[_Model]]]]:
        """
        Iterates through pages of models that match the provided filters.
        If filters are not provided, paginates all models.

        Parameters
        ----------
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
        confidence: Union[int, float, decimal.Decimal],
        image_id: str,
        prediction_class: str,
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
        A newly initialized prediction is incomplete and must be added to an prediction.

        Parameters
        ----------
        confidence
            The score of the prediction.
        image_id
            The ID of the image to add the prediction to.
        prediction_class
            The classification of the prediction (e.g. "person", "vehicle").
        prediction_value
            The value of the prediction.
        prediction_type
            The type of the prediction (e.g. "box", "polygon").
            Will be inferred if ``prediction_value`` is an instance of ``AnnotationType``.
        """

        super(Prediction, self).__init__(
            confidence=confidence,
            image_id=image_id,
            prediction_class=prediction_class,
            prediction_value=prediction_value,
            prediction_type=(
                prediction_value._object_type
                if isinstance(prediction_value, AnnotationType)
                else prediction_type
            ),
            **params,
        )

    def _init_volatile_fields(
        self,
        confidence: Union[int, float, decimal.Decimal],
        image_id: str,
        prediction_class: str,
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
            confidence=confidence,
            image_id=image_id,
            prediction_class=prediction_class,
            prediction_value=prediction_value,
            prediction_type=prediction_type,
            **params,
        )

        for k, v, is_required in iter(
            [
                ("confidence", confidence, True),
                ("image_id", image_id, True),
                ("prediction_class", prediction_class, True),
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

    @classmethod
    def create_bulk(
        cls,
        *,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        dataset_id: str,
        diagnosis_id: str,
        predictions: List[Prediction],
        split: Split = Split.VAL,
        asynchronous: bool = True,
    ) -> Job:
        """
        Creates a job that adds newly initialized predictions to a diagnosis.

        Parameters
        ----------
        dataset_id
            The ID of the dataset to add the predictions to.
        diagnosis_id
            The ID of the diagnosis to add the predictions to.
        predictions
            Newly initialized predictions to add.
        split
            The subset data type used for training the model.
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

        raw_predictions = []

        # Ensure the prediction_values are converted to "raw" form
        for prediction in predictions:
            raw_predictions.append(prediction.to_dict_deep())

        uploaded_param = Job._upload_params(
            access_key=access_key,
            team_name=team_name,
            data=raw_predictions,
        )

        job = Job.create(
            access_key=access_key,
            team_name=team_name,
            job_type=JobType.IMPORT_PREDICTIONS,
            param={
                "dataset_id": dataset_id,
                "diagnosis_id": diagnosis_id,
                "predictions": {"param_id": uploaded_param["id"]},
                "split": split,
            },
        )

        if not asynchronous:
            job.wait_until_complete()

        return job


def create_diagnosis(
    *,
    access_key: Optional[str] = None,
    team_name: Optional[str] = None,
    dataset_id: str,
    model_name: str,
    class_list: List[str],
    iou_type: IouType,
    metadata: Optional[Dict[str, Union[int, float, decimal.Decimal]]] = None,
) -> Diagnosis:
    """
    Creates a diagnosis.

    Parameters
    ----------
    dataset_id
        The ID of the dataset to use for the diagnosis.
    model_name
        The name of the model to diagnose.
    class_list
        The list of class names to diagnose.
    iou_type
        The iou type of the diagnosis.
    metadata
        The metadata associated with the diagnosis.
        Supported fields: ``beta``, ``target_iou``.
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
        When a diagnosis for the (dataset, model_name) pair already exists.
    """
    return Diagnosis.create(
        access_key=access_key,
        team_name=team_name,
        dataset_id=dataset_id,
        model_name=model_name,
        class_list=class_list,
        iou_type=iou_type,
        metadata=metadata,
    )


def fetch_available_models(
    *,
    access_key: Optional[str] = None,
    team_name: Optional[str] = None,
) -> Dict[str, Any]:
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
) -> Dict[str, Any]:
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
    obj = _Model(id=id)
    obj.modify(
        access_key=access_key,
        team_name=team_name,
        name=name,
    )

    return obj.to_dict_deep()
