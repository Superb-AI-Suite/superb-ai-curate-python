import pathlib
import random
import uuid
from typing import List

import pytest


@pytest.fixture(scope="module")
def image_id() -> str:
    return f"im_{str(uuid.uuid4())}"


@pytest.fixture(scope="module")
def image_key() -> str:
    return f"some-image-key-{str(uuid.uuid4())}"


@pytest.fixture(scope="module")
def prediction_confidence() -> float:
    return random.random()


@pytest.fixture(scope="function", autouse=True)
def annotation(image_id: str, image_key: str, bounding_box: dict) -> dict:
    return {
        "_object_type": "annotation",
        "image_id": image_id,
        "image_key": image_key,
        "annotation_class": "annotation_class",
        "annotation_type": "box",
        "annotation_value": bounding_box,
        "metadata": {},
    }


@pytest.fixture(scope="function", autouse=True)
def prediction(image_id: str, prediction_confidence: float, bounding_box: dict) -> dict:
    return {
        "_object_type": "predprediction_confidence,iction",
        "confidence": prediction_confidence,
        "image_id": image_id,
        "prediction_class": "prediction_class",
        "prediction_type": "box",
        "prediction_value": bounding_box,
    }


@pytest.fixture(scope="function", autouse=True)
def image_source_url(image_key: str) -> dict:
    return {
        "_object_type": "image",
        "key": image_key,
        "metadata": {"some-key": "some value"},
        "source": {
            "type": "URL",
            "url": "http://images.cocodataset.org/train2017/000000391895.jpg",
        },
    }


@pytest.fixture(scope="function", autouse=True)
def image_source_urls_invalid() -> List[str]:
    return [
        0,
        "",
        "http:www.example.com",
        "https:www.example.com",
        "ftp:www.example.com",
        "ftps:www.example.com",
        "example",
        "example@com",
        "http://<example>",
        "://com/",
        "1http://com",
        "1http:////image.jpg",
    ]


@pytest.fixture(scope="function", autouse=True)
def image_source_urls_valid() -> List[str]:
    return [
        "http://localhost/",
        "http://127.0.0.1/",
        "https://www.example.com:8080/image.jpg",
        "http://www.example.com:8080/image.jpg",
        "http://example.co.kr/%20",
        "http://example.co.kr/path%20with%20spaces.jpg",
        "https://www.example.com/?a=0",
        "http://www.example.com/media/source/image.jpg",
        "https://www.example.com/media/source/image.jpg",
        "http://www.example.com/?a=1",
        "ftp://ftp.example.co.kr/media/image.jpg",
        "ftp://ftp.example.co.kr/../../../media/image.jpg",
        "http://www.example.com/media/image.jpg",
        "https://www.example.com/",
        "https://www.example.com",
        "http://www.example.com/",
        "http://www.example.com",
    ]


@pytest.fixture(scope="function", autouse=True)
def image_source_local(image_key: str) -> dict:
    return {
        "_object_type": "image",
        "key": image_key,
        "metadata": {"some-key": "some value"},
        "source": {
            "type": "LOCAL",
            "path": pathlib.Path(__file__).parent.resolve()
            / "../../resources/images/image1.png",
            "file_size": 10304,
        },
    }
