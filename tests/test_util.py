import json
import random
import uuid

import pytest

from spb_curate import Annotation, Dataset, Image, Point, util
from spb_curate.abstract.superb_ai_object import SuperbAIObject
from spb_curate.superb_ai_response import SuperbAIResponse


@pytest.fixture(scope="function", autouse=True)
def sample_dataset():
    return {
        "_object_type": "dataset",
        "id": f"ds_{uuid.uuid4()}",
        "name": f"some_name_{uuid.uuid4()}",
        "description": f"some_description {uuid.uuid4()}",
    }


@pytest.fixture(scope="function", autouse=True)
def sample_image():
    dataset_id = f"ds_{uuid.uuid4()}"
    image_id = f"im_{uuid.uuid4()}"

    return {
        "_object_type": "image",
        "annotations": [
            {
                "_object_type": "annotation",
                "annotation_class": "1",
                "annotation_type": "box",
                "annotation_value": {
                    "height": 46.36,
                    "width": 76.69,
                    "x": 38.75,
                    "y": 0,
                },
                "created_at": "2022-12-12T08:47:04.407Z",
                "created_by": "some-email@superb-ai.com",
                "dataset_id": dataset_id,
                "id": f"an_{uuid.uuid4()}",
                "image_id": image_id,
                "metadata": {"iscrowd": 0},
                "updated_at": "2022-12-12T08:47:04.407Z",
                "updated_by": "some-email@superb-ai.com",
            }
        ],
        "attributes": {
            "file_size": 297595,
            "height": 640,
            "mime_type": "image/jpeg",
            "width": 481,
        },
        "created_at": "2022-12-12T08:46:57.541Z",
        "created_by": "some-email@superb-ai.com",
        "dataset_id": dataset_id,
        "id": image_id,
        "key": "11111",
        "metadata": {"license": 1},
        "source": f"as_{uuid.uuid4()}",
        "updated_at": "2022-12-12T08:46:57.541Z",
        "updated_by": "some-email@superb-ai.com",
    }


@pytest.fixture(scope="function", autouse=True)
def sample_point():
    return {
        "x": 1,
        "y": 2,
        "width": 3,
        "height": 4,
    }


@pytest.fixture(scope="function", autouse=True)
def sample_conversion_data(sample_dataset, sample_image, sample_point):
    return {
        "test_str": str(uuid.uuid4()),
        "test_cls_point": sample_point,
        "test_dict_image": sample_image,
        "test_list_int": [random.randint(0, 50) for _ in range(5)],
        "test_list_cls_dataset": [sample_dataset],
        "test_superb_object_dataset": Dataset.construct_from_dict(data=sample_dataset),
        "test_suberb_response_dataset": SuperbAIResponse(
            body=json.dumps(sample_dataset), code=200, headers={}
        ),
    }


def test_convert_to_superb_ai_object(sample_conversion_data):
    # Test list conversion of str
    result_obj = util.convert_to_superb_ai_object(
        data=sample_conversion_data["test_list_int"]
    )
    assert isinstance(result_obj, list)
    assert len(result_obj) is len(sample_conversion_data["test_list_int"])
    assert result_obj[0] is sample_conversion_data["test_list_int"][0]

    # Test list conversion of dictionary to SuperbAIObject
    cls = Dataset
    result_obj = util.convert_to_superb_ai_object(
        data=sample_conversion_data["test_list_cls_dataset"], cls=cls
    )
    assert isinstance(result_obj[0], cls)

    # Test dictionary conversion to SuperbAIObject without _object_type
    result_obj = util.convert_to_superb_ai_object(
        data=sample_conversion_data["test_cls_point"]
    )
    assert isinstance(result_obj, SuperbAIObject)
    assert not isinstance(result_obj, Point)
    assert result_obj.x == sample_conversion_data["test_cls_point"]["x"]

    # Test dictionary conversion to specified class without _object_type
    result_obj = util.convert_to_superb_ai_object(
        data=sample_conversion_data["test_cls_point"], cls=Point
    )
    assert isinstance(result_obj, Point)
    assert result_obj.x == sample_conversion_data["test_cls_point"]["x"]

    # Test dictionary conversion to specified class using _object_type
    # This also will convert a nested object list to appropriate _object_type
    result_obj = util.convert_to_superb_ai_object(
        data=sample_conversion_data["test_dict_image"]
    )
    assert isinstance(result_obj, Image)
    assert result_obj.id == sample_conversion_data["test_dict_image"]["id"]
    assert isinstance(result_obj.annotations[0], Annotation)
    assert (
        result_obj.annotations[0].id
        == sample_conversion_data["test_dict_image"]["annotations"][0]["id"]
    )

    # Test SuperbAIResponse's data conversion to cls
    cls = Dataset
    result_obj = util.convert_to_superb_ai_object(
        data=sample_conversion_data["test_suberb_response_dataset"], cls=cls
    )
    assert isinstance(result_obj, cls)

    # Test conversion of value that isn't a list, dict, SuperbAIResponse, or SuperbAIObject
    result_obj = util.convert_to_superb_ai_object(
        data=sample_conversion_data["test_str"]
    )
    assert isinstance(result_obj, str)
    assert result_obj == sample_conversion_data["test_str"]

    # Test that setting a class doesn't affect the value of non SuperbAIObject's values
    cls = Dataset
    result_obj = util.convert_to_superb_ai_object(
        data=sample_conversion_data["test_str"], cls=cls
    )
    assert isinstance(result_obj, str)
    assert result_obj == sample_conversion_data["test_str"]

    # Test SuperbAIObject doesn't get converted
    result_obj = util.convert_to_superb_ai_object(
        data=sample_conversion_data["test_superb_object_dataset"]
    )
    assert result_obj is sample_conversion_data["test_superb_object_dataset"]
