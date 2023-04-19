from spb_curate import curate

OBJECT_MAPPING = {
    curate.Annotation._object_type: curate.Annotation,
    curate.Dataset._object_type: curate.Dataset,
    curate.Image._object_type: curate.Image,
    curate.Job._object_type: curate.Job,
    curate.Slice._object_type: curate.Slice,
}
