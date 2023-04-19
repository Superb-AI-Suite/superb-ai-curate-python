import pytest

from spb_curate import error
from spb_curate.curate import (
    Annotation,
    BoundingBox,
    Category,
    Cuboid2D,
    Image,
    ImageSourceLocal,
    ImageSourceUrl,
    Keypoints,
    Polygon,
    Polyline,
    RotatedBox,
)


class TestAnnotation(object):
    def test_init(self, annotation):
        # Test when required fields are missing
        init_keys = list(annotation.keys())
        init_keys.remove("image_id")
        init_keys.remove("image_key")

        for k in iter(init_keys):
            if k.startswith("_"):
                continue

            init_partial_data = annotation.copy()
            del init_partial_data[k]

            try:
                Annotation(**init_partial_data)
            except (TypeError, error.ValidationError):
                pass
            else:
                raise Exception(
                    f"Annotation initialization did not fail when missing the field '{k}'"
                )

        # Check it doesn't fail when one of the keys is present
        for k in ["image_id", "image_key"]:
            init_partial_data = annotation.copy()
            del init_partial_data[k]
            Annotation(**init_partial_data)

        # Check it fails when both of the keys is missing
        sample_annotation_missing_fields_init = annotation.copy()
        del sample_annotation_missing_fields_init["image_id"]
        del sample_annotation_missing_fields_init["image_key"]
        try:
            Annotation(**sample_annotation_missing_fields_init)
        except (TypeError, error.ValidationError):
            pass
        else:
            raise Exception(
                f'Annotation initialization did not fail when missing the field \'["image_id", "image_key"]\''
            )

        init_data = annotation.copy()
        init_data.update(
            {"annotation_value": BoundingBox(**annotation["annotation_value"])}
        )

        annotation_instance = Annotation(**init_data)

        assert isinstance(annotation_instance, Annotation)
        assert isinstance(annotation_instance.image_id, str)
        assert isinstance(annotation_instance.annotation_class, str)
        assert isinstance(annotation_instance.annotation_type, str)
        assert isinstance(
            annotation_instance.annotation_value,
            (
                BoundingBox,
                Category,
                Cuboid2D,
                Keypoints,
                Polygon,
                Polyline,
                RotatedBox,
            ),
        )
        assert isinstance(annotation_instance.metadata, dict)

    def test_init_annotation_value(
        self,
        annotation,
        bounding_box,
        cuboid_2d,
        category,
        keypoints_raw,
        polygon_raw,
        polyline_raw,
        rotated_box,
    ):
        annotation_types = [
            ("box", bounding_box),
            ("cuboid2d", cuboid_2d),
            ("category", category),
            ("keypoint", keypoints_raw),
            ("polygon", polygon_raw),
            ("polyline", polyline_raw),
            ("rbox", rotated_box),
        ]

        for annotation_type, annotation_value in annotation_types:
            annotation["annotation_type"] = annotation_type
            annotation["annotation_value"] = annotation_value
            annotation_instance = Annotation.construct_from_dict(data=annotation)

            assert isinstance(annotation_instance, Annotation)
            assert isinstance(
                annotation_instance.annotation_value,
                Annotation._discriminator_map["annotation_value"][1][annotation_type],
            )
            assert annotation_instance.annotation_value.raw == annotation_value

        annotation_types = [
            ("box", BoundingBox(raw_data=bounding_box)),
            ("cuboid2d", Cuboid2D(raw_data=cuboid_2d)),
            ("category", Category(raw_data=category)),
            ("keypoint", Keypoints(raw_data=keypoints_raw)),
            ("polygon", Polygon(raw_data=polygon_raw)),
            ("polyline", Polyline(raw_data=polyline_raw)),
            ("rbox", RotatedBox(raw_data=rotated_box)),
        ]

        for annotation_type, annotation_value in annotation_types:
            # Check using an AnnotationType object auto fills the `annotation_type` field
            annotation["annotation_type"] = ""
            annotation["annotation_value"] = annotation_value
            annotation_instance = Annotation(**annotation)
            assert annotation_instance.annotation_type == annotation_type

    def test_init_raw_annotation_value(
        self,
        annotation,
        bounding_box,
        cuboid_2d,
        category,
        keypoints_raw,
        polygon_raw,
        polyline_raw,
        rotated_box,
    ):
        annotation_types_raw = [
            ("box", bounding_box, BoundingBox),
            ("cuboid2d", cuboid_2d, Cuboid2D),
            ("category", category, Category),
            ("keypoint", keypoints_raw, Keypoints),
            ("polygon", polygon_raw, Polygon),
            ("polyline", polyline_raw, Polyline),
            ("rbox", rotated_box, RotatedBox),
        ]

        for annotation_type, annotation_value, annotation_cls in annotation_types_raw:
            annotation["annotation_type"] = annotation_type
            annotation["annotation_value"] = annotation_value

            assert isinstance(annotation_value, (dict, list))

            annotation_instance = Annotation(**annotation)

            assert isinstance(annotation_instance, Annotation)
            assert annotation_instance.annotation_type == annotation_type
            assert isinstance(annotation_instance.annotation_value, annotation_cls)
            assert annotation_instance.annotation_value.raw == annotation_value

            # Test the requirement of an `annotation_type` when the `annotation_value` is "raw input"
            annotation["annotation_type"] = ""
            annotation["annotation_value"] = annotation_value

            with pytest.raises(error.ValidationError):
                annotation_instance = Annotation(**annotation)

    def test_fail_init_raw_annotation_value(
        self,
        annotation,
        bounding_box,
        cuboid_2d,
        category,
        keypoints_raw,
        polygon_raw,
        polyline_raw,
        rotated_box,
    ):
        annotation_types = [
            ("", bounding_box),
            ("", cuboid_2d),
            ("", category),
            ("", keypoints_raw),
            ("", polygon_raw),
            ("", polyline_raw),
            ("", rotated_box),
        ]

        for annotation_type, annotation_value in annotation_types:
            annotation["annotation_type"] = annotation_type
            annotation["annotation_value"] = annotation_value

            with pytest.raises(error.ValidationError):
                Annotation(**annotation)


class TestDataset(object):
    pass


class TestJob(object):
    pass


class TestImage(object):
    @pytest.mark.parametrize(
        "curate_image_fn",
        ["image_source_local", "image_source_url"],
    )
    def test_init(self, curate_image_fn, request):
        curate_image = request.getfixturevalue(curate_image_fn)

        init_keys = curate_image.keys()
        for k in iter(init_keys):
            if k.startswith("_"):
                continue
            init_partial_data = curate_image.copy()
            del init_partial_data[k]

            try:
                Image(**init_partial_data)
            except (TypeError, error.ValidationError):
                pass
            else:
                raise Exception(
                    f"Image initialization did not fail when missing the field '{k}'"
                )

        init_data = curate_image.copy()
        if curate_image["source"]["type"] == "LOCAL":
            init_data["source"] = ImageSourceLocal(asset=curate_image["source"]["path"])
        elif curate_image["source"]["type"] == "URL":
            init_data["source"] = ImageSourceUrl(url=curate_image["source"]["url"])
        else:
            raise Exception(f'Unexpected source type: {init_data["source"]["type"]}')

        image_instance = Image(**init_data)

        assert isinstance(image_instance, Image)
        assert isinstance(image_instance.key, str)
        assert isinstance(image_instance.source, (ImageSourceLocal, ImageSourceUrl))
        assert isinstance(image_instance.metadata, dict)


class TestImageSource(object):
    def test_image_source_url(
        self, image_source_url, image_source_urls_invalid, image_source_urls_valid
    ):
        image_source_instance = ImageSourceUrl(url=image_source_url["source"]["url"])

        assert isinstance(image_source_instance, ImageSourceUrl)

        image_source_dict = dict(image_source_instance)

        assert image_source_dict["type"] is "URL"
        assert image_source_dict["url"] is image_source_url["source"]["url"]

        for url in image_source_urls_valid:
            assert ImageSourceUrl(url=url).url == url

        for url in image_source_urls_invalid:
            with pytest.raises(error.ValidationError):
                ImageSourceUrl(url=url)

    def test_image_source_local(self, image_source_local):
        image_source_path_instance = ImageSourceLocal(
            asset=image_source_local["source"]["path"]
        )

        assert isinstance(image_source_path_instance, ImageSourceLocal)
        assert isinstance(image_source_path_instance.get_asset(), bytes)

        image_source_asset_dict = dict(image_source_path_instance)

        assert image_source_asset_dict["type"] is "LOCAL"
        assert (
            image_source_path_instance.get_asset_size()
            == image_source_local["source"]["file_size"]
        )

        image_source_str = ImageSourceLocal(
            asset=str(image_source_local["source"]["path"])
        )

        assert (
            image_source_str.get_asset_size()
            == image_source_local["source"]["file_size"]
        )

        with open(image_source_local["source"]["path"], "rb") as fp:
            image_source_bytes = ImageSourceLocal(asset=fp.read())

        assert (
            image_source_bytes.get_asset_size()
            == image_source_local["source"]["file_size"]
        )
