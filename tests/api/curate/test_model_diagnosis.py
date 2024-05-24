import decimal

import pytest

from spb_curate import error
from spb_curate.curate.api.model_diagnosis import Prediction
from spb_curate.curate.model.annotation_types import BoundingBox, Polygon


class TestDiagnosis(object):
    pass


class TestEvaluation(object):
    pass


class TestModel(object):
    pass


class TestPrediction(object):
    def test_init(self, prediction):
        # Test when required fields are missing
        init_keys = list(prediction.keys())

        for k in iter(init_keys):
            if k.startswith("_"):
                continue

            init_partial_data = prediction.copy()
            del init_partial_data[k]

            try:
                Prediction(**init_partial_data)
            except (TypeError, error.ValidationError):
                pass
            else:
                raise Exception(
                    f"Prediction initialization did not fail when missing the field '{k}'"
                )

        init_data = prediction.copy()
        init_data.update(
            {"prediction_value": BoundingBox(**prediction["prediction_value"])}
        )

        prediction_instance = Prediction(**init_data)

        assert isinstance(prediction_instance, Prediction)
        assert isinstance(prediction_instance.confidence, (int, float, decimal.Decimal))
        assert isinstance(prediction_instance.image_id, str)
        assert isinstance(prediction_instance.prediction_class, str)
        assert isinstance(prediction_instance.prediction_type, str)
        assert isinstance(
            prediction_instance.prediction_value,
            (
                BoundingBox,
                Polygon,
            ),
        )

    def test_init_prediction_value(
        self,
        prediction,
        bounding_box,
        polygon_raw,
    ):
        prediction_types = [
            ("box", bounding_box),
            ("polygon", polygon_raw),
        ]

        for prediction_type, prediction_value in prediction_types:
            prediction["prediction_type"] = prediction_type
            prediction["prediction_value"] = prediction_value
            prediction_instance = Prediction.construct_from_dict(data=prediction)

            assert isinstance(prediction_instance, Prediction)
            assert isinstance(
                prediction_instance.prediction_value,
                Prediction._discriminator_map["prediction_value"][1][prediction_type],
            )
            assert prediction_instance.prediction_value.raw == prediction_value

        prediction_types = [
            ("box", BoundingBox(raw_data=bounding_box)),
            ("polygon", Polygon(raw_data=polygon_raw)),
        ]

        for prediction_type, prediction_value in prediction_types:
            # Check using an AnnotationType object auto fills the `prediction_type` field
            prediction["prediction_type"] = ""
            prediction["prediction_value"] = prediction_value
            prediction_instance = Prediction(**prediction)
            assert prediction_instance.prediction_type == prediction_type

    def test_init_raw_prediction_value(
        self,
        prediction,
        bounding_box,
        polygon_raw,
    ):
        prediction_types_raw = [
            ("box", bounding_box, BoundingBox),
            ("polygon", polygon_raw, Polygon),
        ]

        for prediction_type, prediction_value, prediction_cls in prediction_types_raw:
            prediction["prediction_type"] = prediction_type
            prediction["prediction_value"] = prediction_value

            assert isinstance(prediction_value, (dict, list))

            prediction_instance = Prediction(**prediction)

            assert isinstance(prediction_instance, Prediction)
            assert prediction_instance.prediction_type == prediction_type
            assert isinstance(prediction_instance.prediction_value, prediction_cls)
            assert prediction_instance.prediction_value.raw == prediction_value

            # Test the requirement of an `prediction_type` when the `prediction_value` is "raw input"
            prediction["prediction_type"] = ""
            prediction["prediction_value"] = prediction_value

            with pytest.raises(error.ValidationError):
                prediction_instance = Prediction(**prediction)

    def test_fail_init_raw_prediction_value(
        self,
        prediction,
        bounding_box,
        polygon_raw,
    ):
        prediction_types = [
            ("", bounding_box),
            ("", polygon_raw),
        ]

        for prediction_type, prediction_value in prediction_types:
            prediction["prediction_type"] = prediction_type
            prediction["prediction_value"] = prediction_value

            with pytest.raises(error.ValidationError):
                Prediction(**prediction)
