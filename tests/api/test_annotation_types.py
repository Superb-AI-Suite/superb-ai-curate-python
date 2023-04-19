from decimal import Decimal

from spb_curate import error
from spb_curate.curate import (
    BoundingBox,
    Category,
    Contour,
    Cuboid2D,
    Edge,
    Face,
    Keypoint,
    Keypoints,
    KeypointState,
    Path,
    Point,
    Polygon,
    Polyline,
    RotatedBox,
)
from spb_curate.curate.model.annotation_types import AnnotationType


class TestBoundingBox(object):
    def test_init(self, bounding_box):
        init_keys = list(bounding_box.keys())

        for k in iter(init_keys):
            init_partial_data = bounding_box.copy()
            del init_partial_data[k]

            try:
                BoundingBox(**init_partial_data)
            except (TypeError, error.ValidationError):
                pass
            else:
                raise Exception(
                    f"BoundingBox initialization did not fail when missing the field '{k}'"
                )

        bounding_box_instance = BoundingBox(
            x=int(bounding_box["x"]),
            y=bounding_box["y"],
            width=float(bounding_box["width"]),
            height=Decimal(bounding_box["height"]),
        )

        assert isinstance(bounding_box_instance, AnnotationType)
        assert isinstance(bounding_box_instance, BoundingBox)
        assert isinstance(bounding_box_instance.x, int)
        assert isinstance(bounding_box_instance.width, float)
        assert isinstance(bounding_box_instance.height, Decimal)

        bounding_box_dict_keys = list(dict(bounding_box_instance).keys())
        bounding_box_dict_keys.sort()

        assert bounding_box_dict_keys == ["height", "width", "x", "y"]

    def test_init_raw(self, bounding_box):
        bounding_box_instance = BoundingBox(
            x=int(bounding_box["x"]),
            y=bounding_box["y"],
            width=float(bounding_box["width"]),
            height=Decimal(bounding_box["height"]),
        )
        bounding_box_raw_instance = BoundingBox(raw_data=bounding_box)

        assert bounding_box_raw_instance == bounding_box_instance
        assert bounding_box_instance.raw == bounding_box

    def test_raw(self, bounding_box):
        bounding_box_instance = BoundingBox(
            x=bounding_box["x"],
            y=bounding_box["y"],
            width=bounding_box["width"],
            height=bounding_box["height"],
        )

        assert isinstance(bounding_box_instance.raw, dict)
        assert bounding_box_instance.raw == bounding_box


class TestCuboid2D(object):
    def test_init(self, cuboid_2d):
        init_keys = list(cuboid_2d.keys())

        for k in iter(init_keys):
            init_partial_data = cuboid_2d.copy()
            del init_partial_data[k]

            try:
                Cuboid2D(**init_partial_data)
            except (TypeError, error.ValidationError):
                pass
            else:
                raise Exception(
                    f"Cuboid2D initialization did not fail when missing the field '{k}'"
                )

        cuboid_2d_instance = Cuboid2D(near=cuboid_2d["near"], far=cuboid_2d["far"])

        assert isinstance(cuboid_2d_instance, AnnotationType)
        assert dict(cuboid_2d_instance) == cuboid_2d
        assert isinstance(cuboid_2d_instance, Cuboid2D)
        assert isinstance(cuboid_2d_instance.near, BoundingBox)
        assert isinstance(cuboid_2d_instance.far, BoundingBox)
        assert dict(cuboid_2d_instance.near) == cuboid_2d["near"]
        assert dict(cuboid_2d_instance.far) == cuboid_2d["far"]

    def test_init_raw(self, cuboid_2d):
        cuboid_2d_instance = Cuboid2D(near=cuboid_2d["near"], far=cuboid_2d["far"])
        cuboid_2d_raw_instance = Cuboid2D(raw_data=cuboid_2d)

        assert cuboid_2d_raw_instance == cuboid_2d_instance
        assert cuboid_2d_instance.raw == cuboid_2d

    def test_raw(self, cuboid_2d):
        cuboid_2d_instance = Cuboid2D(raw_data=cuboid_2d)

        assert isinstance(cuboid_2d_instance.raw, dict)
        assert cuboid_2d_instance.raw == cuboid_2d


class TestCategory(object):
    def test_init(self, category, category_value_list, category_value_none):
        category_instance = Category(value=category["value"])

        assert isinstance(category_instance, AnnotationType)
        assert dict(category_instance) == category
        assert isinstance(category_instance, Category)
        assert isinstance(category_instance.value, str)

        category_instance = Category(value=category_value_list["value"])

        assert isinstance(category_instance, AnnotationType)
        assert dict(category_instance) == category_value_list
        assert isinstance(category_instance, Category)
        assert isinstance(category_instance.value, list)

        category_instance = Category(value=category_value_none["value"])

        assert isinstance(category_instance, AnnotationType)
        assert dict(category_instance) == category_value_none
        assert isinstance(category_instance, Category)
        assert category_instance.value is None

    def test_init_raw(self, category):
        category_instance = Category(value=category["value"])
        category_raw_instance = Category(raw_data=category)

        assert category_raw_instance == category_instance
        assert category_instance.raw == category

    def test_raw(self, category, category_value_none_raw):
        category_instance = Category(raw_data=category)

        assert isinstance(category_instance.raw, dict)
        assert category_instance.raw == category

        category_instance = Category(raw_data=category_value_none_raw)

        assert category_instance.raw == category_value_none_raw


class TestKeypoints(object):
    def test_init(self, keypoints):
        init_keys = list(keypoints.keys())

        for k in iter(init_keys):
            init_partial_data = keypoints.copy()
            del init_partial_data[k]

            try:
                Keypoints(**init_partial_data)
            except (TypeError, error.ValidationError):
                pass
            else:
                raise Exception(
                    f"Keypoints initialization did not fail when missing the field '{k}'"
                )

        keypoints_instance = Keypoints(
            points=[
                Keypoint(
                    name=point["name"],
                    x=point["x"],
                    y=point["y"],
                    state=KeypointState(
                        visible=point["state"]["visible"],
                        valid=point["state"]["valid"],
                    ),
                )
                for point in keypoints["points"]
            ],
            edges=[Edge(x=edge["x"], y=edge["y"]) for edge in keypoints["edges"]],
        )

        assert isinstance(keypoints_instance, AnnotationType)
        assert dict(keypoints_instance) == keypoints
        assert isinstance(keypoints_instance, Keypoints)
        assert isinstance(keypoints_instance.points, list)
        assert isinstance(keypoints_instance.edges, list)
        assert len(keypoints_instance.points) == len(keypoints["points"])
        assert len(keypoints_instance.edges) == len(keypoints["edges"])

        for keypoint in keypoints_instance.points:
            assert isinstance(keypoint, Keypoint)

        for edge in keypoints_instance.edges:
            assert isinstance(edge, Edge)

    def test_init_raw(self, keypoints_raw):
        keypoints_instance = Keypoints(
            points=keypoints_raw["points"],
            edges=[
                Edge(x=edge_raw["x"], y=edge_raw["y"])
                for edge_raw in keypoints_raw["edges"]
            ],
        )
        keypoints_raw_instance = Keypoints(raw_data=keypoints_raw)

        assert keypoints_raw_instance == keypoints_instance
        assert keypoints_instance.raw == keypoints_raw

    def test_raw(self, keypoints_raw):
        keypoints_instance = Keypoints(raw_data=keypoints_raw)

        assert isinstance(keypoints_instance.raw, dict)
        assert keypoints_instance.raw == keypoints_raw


class TestPolygon(object):
    def test_init(self, polygon):
        polygon_instance = Polygon(faces=polygon["faces"])

        assert isinstance(polygon_instance, AnnotationType)
        assert dict(polygon_instance) == polygon
        assert isinstance(polygon_instance, Polygon)
        assert isinstance(polygon_instance.faces, list)
        assert len(polygon_instance.faces) == len(polygon["faces"])

        for face_i, face_instance in enumerate(polygon_instance.faces):
            face_sample = polygon["faces"][face_i]
            assert isinstance(face_instance, Face)
            assert isinstance(face_instance.contours, list)
            assert len(face_instance.contours) == len(face_sample["contours"])

            for contour_i, contour_instance in enumerate(face_instance.contours):
                contour_sample = face_sample["contours"][contour_i]
                assert isinstance(contour_instance, Contour)
                assert isinstance(contour_instance.points, list)
                assert len(contour_instance.points) == len(contour_sample["points"])

                for point_i, point_instance in enumerate(contour_instance.points):
                    point_sample = contour_sample["points"][point_i]
                    assert isinstance(point_instance, Point)
                    assert point_instance.x == point_sample["x"]
                    assert point_instance.y == point_sample["y"]

    def test_init_raw(self, polygon_raw):
        polygon_instance = Polygon(
            faces=[
                Face(
                    contours=[
                        Contour(
                            points=[
                                Point(x=point_raw["x"], y=point_raw["y"])
                                for point_raw in contour_raw
                            ]
                        )
                        for contour_raw in face_raw
                    ]
                )
                for face_raw in polygon_raw["points"]
            ]
        )
        polygon_raw_instance = Polygon(raw_data=polygon_raw)

        assert polygon_raw_instance == polygon_instance
        assert polygon_instance.raw == polygon_raw

    def test_raw(self, polygon_raw):
        polygon_instance = Polygon(raw_data=polygon_raw)

        assert isinstance(polygon_instance.raw, dict)
        assert polygon_instance.raw == polygon_raw


class TestPolyline(object):
    def test_init(self, polyline):
        polyline_instance = Polyline(paths=polyline["paths"])

        assert isinstance(polyline_instance, AnnotationType)
        assert dict(polyline_instance) == polyline
        assert isinstance(polyline_instance, Polyline)
        assert isinstance(polyline_instance.paths, list)
        assert len(polyline_instance.paths) == len(polyline["paths"])

        for path_i, path_instance in enumerate(polyline_instance.paths):
            path_sample = polyline["paths"][path_i]
            assert isinstance(path_instance, Path)
            assert isinstance(path_instance.points, list)
            assert len(path_instance.points) == len(path_sample["points"])

            for point_i, point_instance in enumerate(path_instance.points):
                point_sample = path_sample["points"][point_i]
                assert isinstance(point_instance, Point)
                assert point_instance.x == point_sample["x"]
                assert point_instance.y == point_sample["y"]

    def test_init_raw(self, polyline_raw):
        polyline_instance = Polyline(
            paths=[
                Path(
                    points=[
                        Point(x=point_raw["x"], y=point_raw["y"])
                        for point_raw in path_raw
                    ]
                )
                for path_raw in polyline_raw["points"]
            ]
        )
        polyline_raw_instance = Polyline(raw_data=polyline_raw)

        assert polyline_raw_instance == polyline_instance
        assert polyline_instance.raw == polyline_raw

    def test_raw(self, polyline_raw):
        polyline_instance = Polyline(raw_data=polyline_raw)

        assert isinstance(polyline_instance.raw, dict)
        assert polyline_instance.raw == polyline_raw


class TestRotatedBox(object):
    def test_init(self, rotated_box):
        init_keys = list(rotated_box.keys())

        for k in iter(init_keys):
            init_partial_data = rotated_box.copy()
            del init_partial_data[k]

            try:
                RotatedBox(**init_partial_data)
            except (TypeError, error.ValidationError):
                pass
            else:
                raise Exception(
                    f"RotatedBox initialization did not fail when missing the field '{k}'"
                )

        rotated_box.update(
            {
                "cx": int(rotated_box["cx"]),
                "width": float(rotated_box["width"]),
                "height": Decimal(rotated_box["height"]),
            }
        )

        rotated_box_instance = RotatedBox(
            cx=rotated_box["cx"],
            cy=rotated_box["cy"],
            width=rotated_box["width"],
            height=rotated_box["height"],
            angle=rotated_box["angle"],
        )

        assert isinstance(rotated_box_instance, AnnotationType)
        assert isinstance(rotated_box_instance, RotatedBox)
        assert isinstance(rotated_box_instance.cx, int)
        assert isinstance(rotated_box_instance.width, float)
        assert isinstance(rotated_box_instance.height, Decimal)
        assert isinstance(rotated_box_instance.angle, float)

        rotated_box_dict_keys = list(dict(rotated_box_instance).keys())
        rotated_box_dict_keys.sort()

        assert rotated_box_dict_keys == [
            "angle",
            "cx",
            "cy",
            "height",
            "width",
        ]

    def test_init_raw(self, rotated_box):
        rotated_box_instance = RotatedBox(
            cx=rotated_box["cx"],
            cy=rotated_box["cy"],
            width=rotated_box["width"],
            height=rotated_box["height"],
            angle=rotated_box["angle"],
        )
        rotated_box_raw_instance = RotatedBox(raw_data=rotated_box)

        assert rotated_box_raw_instance == rotated_box_instance
        assert rotated_box_instance.raw == rotated_box

    def test_raw(self, rotated_box):
        rotated_box_instance = RotatedBox(
            cx=rotated_box["cx"],
            cy=rotated_box["cy"],
            width=rotated_box["width"],
            height=rotated_box["height"],
            angle=rotated_box["angle"],
        )

        assert isinstance(rotated_box_instance.raw, dict)
        assert rotated_box_instance.raw == rotated_box
