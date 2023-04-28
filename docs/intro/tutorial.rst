.. _intro-tutorial:

========
Tutorial
========

Setting the credentials
=======================
.. code-block:: python

    import spb_curate

    spb_curate.access_key = "..."
    spb_curate.team_name = "..."


Create your first dataset
========================

By the end of this tutorial you will have created a dataset and added an image
(via local storage or URL) with an annotation.

.. code-block:: python

    import time

    from spb_curate import curate

    dataset = curate.create_dataset(
        name="My first dataset",
        description="For setting up the Superb AI Curate Python client"
    )

    image_1_key = "unique-image-key-1"
    image_2_key = "unique-image-key-2"

    images = [
        # Add image from local storage
        curate.Image(
            key=image_1_key,
            source=curate.ImageSourceLocal(asset="/path/to/image"),
            metadata={"weather": "clear", "timeofday": "daytime"},
        ),

        # Add image from URL
        curate.Image(
            key=image_2_key,
            source=curate.ImageSourceUrl(url="http://example.com/path/to/image.jpg"),
            metadata={"weather": "sunny", "timeofday": "daytime"},
        ),
    ]

    job = dataset.add_images(images=images)
    job.wait_until_complete()

    time.sleep(5)

    box_x, box_y, box_w, box_h = (0, 0, 100, 100)

    annotations = [
        curate.Annotation(
            image_key=image_1_key,
            annotation_class="dog",
            annotation_value=curate.BoundingBox(
                x=box_x, y=box_y, width=box_w, height=box_h),
            metadata={"iscrowd": "true"},
        )
    ]

    job = dataset.add_annotations(annotations=annotations)
    job.wait_until_complete()