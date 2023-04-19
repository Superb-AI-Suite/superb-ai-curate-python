from __future__ import annotations

import decimal
import json
from typing import Dict, List, NewType, Optional, Tuple, Union

from spb_curate import util
from spb_curate.abstract.superb_ai_object import SuperbAIObject


class AnnotationType(SuperbAIObject):
    _property_fields = set(["raw"])
    _raw_data_child_cls = (None, None)
    _raw_data_field_map = {}
    _object_type = ""

    def __init__(
        self,
        **params,
    ):
        """Initialize an annotation type instance. This initialisation
        method will load the data from a raw form if it exists in the
        init parameters.

        Parameters
        ----------
        faces
            used to contruct a polygon
        raw_data
            raw form of the annotation type using nested lists
        """
        raw_data = params.get("raw_data", None)

        super(AnnotationType, self).__init__(**params)
        self.load_from_raw_data(data=raw_data, **params)

    def load_from_raw_data(self, data: Optional[any] = None, **params):
        """Recursively initalise and load a class instance from a 'raw data' form.

        Parameters
        ----------
        data, optional
            the raw data to be loaded into the class instance, by default None
        """
        if data is None:
            return

        prepared_data = params.copy()

        if isinstance(data, dict):
            data = {v: data[k] for k, v in self._raw_data_field_map.items()} or data
            for field_key, item_data in data.items():
                if (
                    field_key in self._field_class_map
                    and self._field_class_map[field_key]
                ):
                    if isinstance(item_data, list):
                        value = [
                            self._field_class_map[field_key](**{"raw_data": x})
                            for x in item_data
                        ]
                    else:
                        value = self._field_class_map[field_key](
                            **{"raw_data": item_data}
                        )
                else:
                    value = item_data

                prepared_data.update({field_key: value})

            self.load_from_dict(data=prepared_data)
        elif isinstance(data, list):
            children_key, field_cls = self._raw_data_child_cls

            if field_cls:
                items: field_cls = []
                for item_data in data:
                    items.append(field_cls(**{"raw_data": item_data}))

                prepared_data.update({children_key: items})
                self.load_from_dict(data=prepared_data)

        if "raw_data" in self:
            del self.raw_data

    @property
    def raw(self):
        raise NotImplemented

    def __str__(self):
        return json.dumps(
            self.raw,
            sort_keys=True,
            indent=2,
            cls=self.ReprJSONEncoder,
        )


NumberType = Union[int, float, decimal.Decimal]


PointRawType = NewType(
    "PointRawType",
    Dict[str, NumberType],
)


class Point(AnnotationType):
    _field_class_map = {"x": None, "y": None}

    def __init__(
        self,
        *,
        x: Optional[NumberType] = None,
        y: Optional[NumberType] = None,
        raw_data: Optional[PointRawType] = None,
        **params,
    ):
        """Initialize a point used in annotations.

        Parameters
        ----------
        x
            x coordinate
        y
            y coordinate
        raw_data
            a raw form of the point type, {"x": 0, "y": 1}
        """
        super(Point, self).__init__(x=x, y=y, raw_data=raw_data, **params)

    def _init_volatile_fields(
        self,
        x: Optional[NumberType] = None,
        y: Optional[NumberType] = None,
        raw_data: Optional[PointRawType] = None,
        **params,
    ) -> None:
        super(Point, self)._init_volatile_fields(
            x=x,
            y=y,
            raw_data=raw_data,
            **params,
        )

        native_args = [("x", x), ("y", y)]
        self._init_volatile_fields_validate(
            key_value_pair_lists=[native_args, ("raw_data", raw_data)]
        )
        self._init_volatile_fields_load(fields=native_args)

    @property
    def raw(self):
        return {"x": self.x, "y": self.y}


class Contour(AnnotationType):
    _field_class_map = {"points": Point}
    _raw_data_child_cls: Tuple[str, AnnotationType] = ("points", Point)

    def __init__(
        self,
        *,
        points: Optional[List[Point]] = None,
        raw_data: Optional[List[PointRawType]] = None,
        **params,
    ):
        """Initialize a Contour used in annotations.

        Parameters
        ----------
        points
            used to construct a contour.
        raw_data
            a raw form of the contour type, [{"x": 0, "y": 1}, {"x": 1, "y": 1}]
        """
        super(Contour, self).__init__(points=points, raw_data=raw_data, **params)

    def _init_volatile_fields(
        self,
        points: Optional[List[Point]] = None,
        raw_data: Optional[List[PointRawType]] = None,
        **params,
    ) -> None:
        super(Contour, self)._init_volatile_fields(
            points=points,
            raw_data=raw_data,
            **params,
        )

        native_args = [("points", points)]
        self._init_volatile_fields_validate(
            key_value_pair_lists=[native_args, ("raw_data", raw_data)]
        )
        self._init_volatile_fields_load(fields=native_args)

    @property
    def raw(self):
        return [point.raw for point in self.points]


class Face(AnnotationType):
    _field_class_map = {"contours": Contour}
    _raw_data_child_cls: Tuple[str, AnnotationType] = ("contours", Contour)

    def __init__(
        self,
        *,
        contours: Optional[List[Contour]] = None,
        raw_data: Optional[List[List[PointRawType]]] = None,
        **params,
    ):
        """Initialize a face used in annotations.

        Parameters
        ----------
        contours
            used to contruct a face
        raw_data
            a raw form of the face type, [[{"x": 0, "y": 1}, {"x": 1, "y": 1}, {"x": 0, "y": 0}]]
        """
        super(Face, self).__init__(contours=contours, raw_data=raw_data, **params)

    def _init_volatile_fields(
        self,
        contours: Optional[List[Contour]] = None,
        raw_data: Optional[List[List[PointRawType]]] = None,
        **params,
    ) -> None:
        super(Face, self)._init_volatile_fields(
            contours=contours,
            raw_data=raw_data,
            **params,
        )

        native_args = [("contours", contours)]
        self._init_volatile_fields_validate(
            key_value_pair_lists=[native_args, ("raw_data", raw_data)]
        )
        self._init_volatile_fields_load(fields=native_args)

    @property
    def raw(self):
        return [contour.raw for contour in self.contours]


class BoundingBox(AnnotationType):
    _object_type = "box"

    def __init__(
        self,
        *,
        x: Optional[NumberType] = None,
        y: Optional[NumberType] = None,
        width: Optional[NumberType] = None,
        height: Optional[NumberType] = None,
        **params,
    ):
        """Initialize a bounding box used for annotations.

        Parameters
        ----------
        x
            x coordinate
        y
            y coordinate
        width
            width of the bounding box
        height
            height of the bounding box
        raw_data
            the raw data form of the BoundingBox, a dictionary of the fields respectively
        """
        super(BoundingBox, self).__init__(
            x=x, y=y, width=width, height=height, **params
        )

    def _init_volatile_fields(
        self,
        x: Optional[NumberType] = None,
        y: Optional[NumberType] = None,
        width: Optional[NumberType] = None,
        height: Optional[NumberType] = None,
        raw_data: Optional[Dict[str, NumberType]] = None,
        **params,
    ) -> None:
        super(BoundingBox, self)._init_volatile_fields(
            x=x,
            y=y,
            width=width,
            height=height,
            raw_data=raw_data,
            **params,
        )

        native_args = [
            ("x", x),
            ("y", y),
            ("width", width),
            ("height", height),
        ]

        self._init_volatile_fields_validate(
            key_value_pair_lists=[native_args, ("raw_data", raw_data)]
        )
        self._init_volatile_fields_load(fields=native_args)

    @property
    def raw(self):
        return self.to_dict()


class KeypointState(AnnotationType):
    _field_class_map = {"visible": None, "valid": None}

    def __init__(
        self,
        *,
        visible: Optional[bool] = None,
        valid: Optional[bool] = None,
        raw_data: Optional[Dict[str, bool]] = None,
        **params,
    ):
        """Possible states for Keypoints.

        Parameters
        ----------
        visible
            Visibility of Keypoint
        valid
            Validity of Keypoint
        raw_data
            a raw form of the KeypointState, {"visible": True, "valid": True}
        """
        super(KeypointState, self).__init__(
            visible=visible, valid=valid, raw_data=raw_data, **params
        )

    def _init_volatile_fields(
        self,
        visible: Optional[bool] = None,
        valid: Optional[bool] = None,
        raw_data: Optional[Dict[str, bool]] = None,
        **params,
    ) -> None:
        super(KeypointState, self)._init_volatile_fields(
            visible=visible,
            valid=valid,
            raw_data=raw_data,
            **params,
        )

        native_args = [("visible", visible), ("valid", valid)]
        self._init_volatile_fields_validate(
            key_value_pair_lists=[native_args, ("raw_data", raw_data)]
        )
        self._init_volatile_fields_load(fields=native_args)

    @property
    def raw(self):
        return {"visible": self.visible, "valid": self.valid}


KeypointStateRawType = NewType("KeypointStateRawType", Dict[str, bool])

KeypointRawType = NewType(
    "KeypointRawType",
    Dict[str, Union[NumberType, KeypointStateRawType, str]],
)


class Keypoint(AnnotationType):
    _field_class_map = {"x": None, "y": None, "visible": None, "name": None}

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        x: Optional[NumberType] = None,
        y: Optional[NumberType] = None,
        state: Optional[KeypointState] = None,
        raw_data: Optional[KeypointRawType] = None,
        **params,
    ):
        """Initialize a Keypoint used in annotations.

        Parameters
        ----------
        name
            name of the Keypoint
        x
            x coordinate
        y
            y coordinate
        state
            state of Keypoint
        raw_data
            a raw form of the point type, {"x": 0, "y": 1, "visible": False, "name": "..."}
        """
        super(Keypoint, self).__init__(
            name=name, x=x, y=y, state=state, raw_data=raw_data, **params
        )

    def _init_volatile_fields(
        self,
        name: Optional[str] = None,
        x: Optional[NumberType] = None,
        y: Optional[NumberType] = None,
        state: Optional[KeypointState] = None,
        raw_data: Optional[KeypointRawType] = None,
        **params,
    ) -> None:
        super(Keypoint, self)._init_volatile_fields(
            name=name,
            x=x,
            y=y,
            state=state,
            raw_data=raw_data,
            **params,
        )

        native_args = [("name", name), ("x", x), ("y", y), ("state", state)]
        self._init_volatile_fields_validate(
            key_value_pair_lists=[native_args, ("raw_data", raw_data)]
        )
        # load the fields from raw_data if it exists there
        native_args = [(k, v) for k, v in raw_data.items()] if raw_data else native_args
        self._init_volatile_fields_load(fields=native_args)

    @property
    def raw(self):
        return self.to_dict()


EdgeRawType = NewType(
    "EdgeRawType",
    Dict[str, int],
)


class Edge(AnnotationType):
    def __init__(
        self,
        *,
        x: Optional[int] = None,
        y: Optional[int] = None,
        raw_data: Optional[EdgeRawType] = None,
        **params,
    ):
        """Initialize an Edge used in annotations.

        Parameters
        ----------
        x
            the first endpoint of the edge
        y
            the second endpoint of the edge
        raw_data
            a raw form of the Edge type, {"x": 0, "y": 1}
        """
        super(Edge, self).__init__(x=x, y=y, raw_data=raw_data, **params)

    def _init_volatile_fields(
        self,
        x: Optional[int] = None,
        y: Optional[int] = None,
        raw_data: Optional[EdgeRawType] = None,
        **params,
    ) -> None:
        super(Edge, self)._init_volatile_fields(
            x=x,
            y=y,
            raw_data=raw_data,
            **params,
        )

        native_args = [("x", x), ("y", y)]
        self._init_volatile_fields_validate(
            key_value_pair_lists=[native_args, ("raw_data", raw_data)]
        )
        self._init_volatile_fields_load(fields=native_args)

    @property
    def raw(self):
        return {"x": self.x, "y": self.y}


KeypointsRawType = NewType(
    "KeypointsRawType", Dict[str, Union[List[KeypointRawType], List[EdgeRawType]]]
)


class Keypoints(AnnotationType):
    _field_class_map = {"points": Keypoint, "edges": Edge}
    _object_type = "keypoint"

    def __init__(
        self,
        *,
        points: Optional[List[Keypoint]] = None,
        edges: Optional[List[Edge]] = None,
        raw_data: Optional[KeypointsRawType] = None,
        **params,
    ):
        """Initialize a Path used in annotations.

        Parameters
        ----------
        points
            used to plot the keypoints.
        edges
            used to construct the keypoint's edges.
        raw_data
            a raw form of the path type	{"points": [{"x": 100, "y": 100, "state": {"visible": True, "valid": True}, "name": "left_eye"}, {"x": 200, "y": 300, "state": {"visible": True, "valid": True}, "name": "right_eye"}, {"state": {"visible": False, "valid": False}, "name": "nose"}], "edges": [{"x": 0, "y": 1}, {"x": 0, "y": 2}]}
        """
        super(Keypoints, self).__init__(
            points=points, edges=edges, raw_data=raw_data, **params
        )

    def _init_volatile_fields(
        self,
        points: Optional[List[Keypoint]] = None,
        edges: Optional[List[Edge]] = None,
        raw_data: Optional[KeypointsRawType] = None,
        **params,
    ) -> None:
        super(Keypoints, self)._init_volatile_fields(
            points=points,
            edges=edges,
            raw_data=raw_data,
            **params,
        )

        native_args = [("points", points), ("edges", edges)]
        self._init_volatile_fields_validate(
            key_value_pair_lists=[native_args, ("raw_data", raw_data)]
        )
        self._init_volatile_fields_load(fields=native_args)

    @property
    def raw(self):
        return {
            "edges": [edge.raw for edge in self.edges],
            "points": [point.raw for point in self.points],
        }


class Path(AnnotationType):
    _field_class_map = {"points": Point}
    _raw_data_child_cls: Tuple[str, AnnotationType] = ("points", Point)

    def __init__(
        self,
        *,
        points: Optional[List[Point]] = None,
        raw_data: Optional[List[PointRawType]] = None,
        **params,
    ):
        """Initialize a Path used in annotations.

        Parameters
        ----------
        points
            used to construct a path.
        raw_data
            a raw form of the path type, [{"x": 0, "y": 1}, {"x": 1, "y": 1}]
        """
        super(Path, self).__init__(points=points, raw_data=raw_data, **params)

    def _init_volatile_fields(
        self,
        points: Optional[List[Point]] = None,
        raw_data: Optional[List[PointRawType]] = None,
        **params,
    ) -> None:
        super(Path, self)._init_volatile_fields(
            points=points,
            raw_data=raw_data,
            **params,
        )

        native_args = [("points", points)]
        self._init_volatile_fields_validate(
            key_value_pair_lists=[native_args, ("raw_data", raw_data)]
        )
        self._init_volatile_fields_load(fields=native_args)

    @property
    def raw(self):
        return [point.raw for point in self.points]


class Polygon(AnnotationType):
    _field_class_map = {"faces": Face}
    _raw_data_field_map = {"points": "faces"}
    _object_type = "polygon"

    def __init__(
        self,
        *,
        faces: Optional[List[Face]] = None,
        raw_data: Optional[Dict[str, List[List[List[PointRawType]]]]] = None,
        **params,
    ):
        """Initialize a polygon used for annotations.

        Parameters
        ----------
        faces
            used to contruct a polygon
        raw_data
            a raw form of the polygon, {"points": [[[{"x": 0, "y": 0}, {"x": 0, "y": 1}, {"x": 1, "y": 1}, {"x": 1, "y": 0}, {"x": 0, "y": 0}]]]}
        """

        super(Polygon, self).__init__(faces=faces, raw_data=raw_data, **params)

    def _init_volatile_fields(
        self,
        faces: Optional[List[Face]] = None,
        raw_data: Optional[Dict[str, List[List[List[PointRawType]]]]] = None,
        **params,
    ) -> None:
        super(Polygon, self)._init_volatile_fields(
            faces=faces,
            raw_data=raw_data,
            **params,
        )

        native_args = [("faces", faces)]
        self._init_volatile_fields_validate(
            key_value_pair_lists=[native_args, ("raw_data", raw_data)]
        )
        self._init_volatile_fields_load(fields=native_args)

    @property
    def raw(self):
        return {"points": [face.raw for face in self.faces]}

    def to_dict(self):
        return self.raw


class Polyline(AnnotationType):
    _field_class_map = {"paths": Path}
    _raw_data_field_map = {"points": "paths"}
    _object_type = "polyline"

    def __init__(
        self,
        *,
        paths: Optional[List[Path]] = None,
        raw_data: Optional[List[List[List[PointRawType]]]] = None,
        **params,
    ):
        """Initialize a polyline used for annotations.

        Parameters
        ----------
        paths
            used to contruct a polyline
        raw_data
            a raw form of the polyline, {"points": [[{"x": 0, "y": 0}, {"x": 0, "y": 1}, {"x": 1, "y": 1}]]}
        """

        super(Polyline, self).__init__(paths=paths, raw_data=raw_data, **params)

    def _init_volatile_fields(
        self,
        paths: Optional[List[Path]] = None,
        raw_data: Optional[List[List[List[PointRawType]]]] = None,
        **params,
    ) -> None:
        super(Polyline, self)._init_volatile_fields(
            paths=paths,
            raw_data=raw_data,
            **params,
        )

        native_args = [("paths", paths)]
        self._init_volatile_fields_validate(
            key_value_pair_lists=[native_args, ("raw_data", raw_data)]
        )
        self._init_volatile_fields_load(fields=native_args)

    @property
    def raw(self):
        return {"points": [path.raw for path in self.paths]}

    def to_dict(self):
        return self.raw


class RotatedBox(AnnotationType):
    _object_type = "rbox"

    def __init__(
        self,
        *,
        cx: Optional[NumberType] = None,
        cy: Optional[NumberType] = None,
        width: Optional[NumberType] = None,
        height: Optional[NumberType] = None,
        angle: Optional[NumberType] = None,
        raw_data: Optional[Dict[str, NumberType]] = None,
        **params,
    ):
        """Initialize a rotated box used for annotations.

        Parameters
        ----------
        cx
            cx coordinate
        cy
            cy coordinate
        width
            width of the rotated box
        height
            height of the rotated box
        angle
            angle of the rotated box
        raw_data
            the raw data form of the RotatedBox a dict of the values
        """
        super(RotatedBox, self).__init__(
            cx=cx,
            cy=cy,
            width=width,
            height=height,
            angle=angle,
            raw_data=raw_data,
            **params,
        )

    def _init_volatile_fields(
        self,
        cx: Optional[NumberType] = None,
        cy: Optional[NumberType] = None,
        width: Optional[NumberType] = None,
        height: Optional[NumberType] = None,
        angle: Optional[NumberType] = None,
        raw_data: Optional[Dict[str, NumberType]] = None,
        **params,
    ) -> None:
        super(RotatedBox, self)._init_volatile_fields(
            cx=cx,
            cy=cy,
            width=width,
            height=height,
            angle=angle,
            raw_data=raw_data,
            **params,
        )

        native_args = [
            ("cx", cx),
            ("cy", cy),
            ("width", width),
            ("height", height),
            ("angle", angle),
        ]

        self._init_volatile_fields_validate(
            key_value_pair_lists=[native_args, ("raw_data", raw_data)]
        )
        self._init_volatile_fields_load(fields=native_args)

    @property
    def raw(self):
        return self.to_dict()


class Cuboid2D(AnnotationType):
    _field_class_map = {"near": BoundingBox, "far": BoundingBox}
    _object_type = "cuboid2d"

    def __init__(
        self,
        *,
        near: Optional[BoundingBox] = None,
        far: Optional[BoundingBox] = None,
        raw_data: Optional[Dict[str, NumberType]] = None,
        **params,
    ):
        """Initialize a 2d cuboid used for annotations.

        Parameters
        ----------
        near
            a bounding box
        far
            a bounding box
        raw_data
            the raw data form of the Cuboid2D, a dict with two BoundingBox like dicts
        """
        super(Cuboid2D, self).__init__(
            near=near,
            far=far,
            raw_data=raw_data,
            **params,
        )

    def _init_volatile_fields(
        self,
        near: Optional[BoundingBox] = None,
        far: Optional[BoundingBox] = None,
        raw_data: Optional[Dict[str, NumberType]] = None,
        **params,
    ) -> None:
        super(Cuboid2D, self)._init_volatile_fields(
            near=near,
            far=far,
            raw_data=raw_data,
            **params,
        )

        native_args = [
            ("near", near),
            ("far", far),
        ]

        self._init_volatile_fields_validate(
            key_value_pair_lists=[native_args, ("raw_data", raw_data)]
        )
        self._init_volatile_fields_load(fields=native_args)

    @property
    def raw(self):
        return self.to_dict()


class Category(AnnotationType):
    _object_type = "category"

    def __init__(
        self,
        *,
        value: Optional[Union[str, List[str]]] = None,
        raw_data: Optional[Dict[str, Union[str, List[str]]]] = None,
        **params,
    ):
        """Initialize a category used for annotations.

        Parameters
        ----------
        value
            the value or list of values
        raw_data
            the raw data form of the Category, a dict with name and value details
        """
        super(Category, self).__init__(
            value=value,
            raw_data=raw_data,
            **params,
        )

    def _init_volatile_fields(
        self,
        value: Optional[Union[str, List[str]]] = None,
        raw_data: Optional[Dict[str, Union[str, List[str]]]] = None,
        **params,
    ) -> None:
        super(Category, self)._init_volatile_fields(
            value=value,
            raw_data=raw_data,
            **params,
        )

        native_args = []

        self._init_volatile_fields_validate(
            key_value_pair_lists=[native_args, ("raw_data", raw_data)]
        )
        # value is optional, so included seperately here
        self._init_volatile_fields_load(fields=native_args + [("value", value)])

    @property
    def raw(self):
        return self.to_dict()
