from enum import Enum


class IouType(str, Enum):
    """
    Types of IoU.
    """

    BOX = "bbox"
    POLYGON = "segm"


class JobType(Enum):
    """
    Available types of a job.
    """

    ANNOTATION_IMPORT = "ANNOTATION_IMPORT"
    DELETE_IMAGES = "DELETE_IMAGES"
    IMAGE_IMPORT = "IMAGE_IMPORT"
    IMPORT_PREDICTIONS = "IMPORT_PREDICTIONS"
    UPDATE_SLICE = "UPDATE_SLICE"
    UPDATE_SLICE_BY_QUERY = "UPDATE_SLICE_BY_QUERY"

    def __str__(self):
        return self.value


class Split(str, Enum):
    """
    Types of Split.
    """

    TRAIN = "TRAIN"
    VAL = "VAL"


class SearchFieldMappingType(str, Enum):
    ANNOTATION_CLASS = "annotations.class_count"
    ANNOTATION_METADATA = "annotations.metadata"
    IMAGE_METADATA = "images.metadata"

    def __str__(self):
        return self.value


class SupportedImageFormat(str, Enum):
    BMP = "BMP"
    JPG = "JPG"
    JPEG = "JPEG"
    PNG = "PNG"
    MPO = "MPO"
    WEBP = "WEBP"
