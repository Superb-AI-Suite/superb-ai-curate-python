import random
import uuid

import pytest


@pytest.fixture(scope="function", autouse=True)
def point() -> dict:
    return {"x": random.randint(0, 200), "y": random.randint(0, 200)}


point_raw = point


@pytest.fixture(scope="function", autouse=True)
def point_list(point) -> dict:
    return [point for _ in range(random.randint(3, 5))]


point_raw_list = point_list


@pytest.fixture(scope="function", autouse=True)
def contour(point_list) -> dict:
    return {"points": point_list}


@pytest.fixture(scope="function", autouse=True)
def contour_raw(point_raw_list) -> list:
    return point_raw_list


@pytest.fixture(scope="function", autouse=True)
def face(contour) -> dict:
    return {"contours": [contour]}


@pytest.fixture(scope="function", autouse=True)
def face_raw(contour_raw) -> list:
    return [contour_raw]


@pytest.fixture(scope="function", autouse=True)
def edge() -> dict:
    return {"x": random.randint(0, 200), "y": random.randint(0, 200)}


@pytest.fixture(scope="function", autouse=True)
def edge_list(edge) -> list:
    return [edge for _ in range(random.randint(3, 5))]


@pytest.fixture(scope="function", autouse=True)
def edge_raw_list(edge_list) -> list:
    return [{"x": edge["x"], "y": edge["y"]} for edge in edge_list]


@pytest.fixture(scope="function", autouse=True)
def keypoint_state() -> dict:
    return {
        "visible": True,
        "valid": True,
    }


@pytest.fixture(scope="function", autouse=True)
def keypoint(keypoint_state) -> dict:
    return {
        "name": f"some-name-{str(uuid.uuid4())}",
        "x": random.randint(0, 200),
        "y": random.randint(0, 200),
        "state": keypoint_state,
    }


@pytest.fixture(scope="function", autouse=True)
def keypoint_list(keypoint) -> list:
    return [keypoint for _ in range(random.randint(3, 5))]


keypoint_raw_list = keypoint_list


@pytest.fixture(scope="function", autouse=True)
def keypoints(edge_list, keypoint_list) -> dict:
    return {
        "edges": edge_list,
        "points": keypoint_list,
    }


@pytest.fixture(scope="function", autouse=True)
def keypoints_raw(edge_raw_list, keypoint_list) -> dict:
    return {
        "edges": edge_raw_list,
        "points": keypoint_list,
    }


@pytest.fixture(scope="function", autouse=True)
def polygon(face) -> dict:
    return {"faces": [face]}


@pytest.fixture(scope="function", autouse=True)
def polygon_raw(face_raw) -> dict:
    return {"points": [face_raw]}


path = contour
path_raw = contour_raw


@pytest.fixture(scope="function", autouse=True)
def polyline(path) -> dict:
    return {"paths": [path]}


@pytest.fixture(scope="function", autouse=True)
def polyline_raw(path_raw) -> dict:
    return {"points": [path_raw]}


@pytest.fixture(scope="function", autouse=True)
def bounding_box() -> dict:
    return {
        "x": random.randint(0, 200),
        "y": random.randint(0, 200),
        "width": random.randint(30, 50) / random.randint(2, 8),
        "height": random.random() * random.randint(5, 50),
    }


bounding_box_alt = bounding_box


@pytest.fixture(scope="function", autouse=True)
def rotated_box() -> dict:
    return {
        "cx": random.randint(0, 200),
        "cy": random.randint(0, 200),
        "width": random.randint(30, 50) / random.randint(2, 8),
        "height": random.random() * random.randint(5, 50),
        "angle": random.random() * 360,
    }


@pytest.fixture(scope="function", autouse=True)
def cuboid_2d(bounding_box, bounding_box_alt):
    return {"near": bounding_box, "far": bounding_box_alt}


@pytest.fixture(scope="function", autouse=True)
def category():
    return {
        "value": str(uuid.uuid4()),
    }


@pytest.fixture(scope="function", autouse=True)
def category_value_list():
    return {
        "value": [str(uuid.uuid4()) for _ in range(random.randint(3, 5))],
    }


@pytest.fixture(scope="function", autouse=True)
def category_value_none():
    return {
        "value": None,
    }


category_value_none_raw = category_value_none
