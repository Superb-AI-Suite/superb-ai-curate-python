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


Creat your first dataset
========================

Create a dataset and add a new image from local storage.

.. code-block:: python

    from spb_curate import curate

    dataset = curate.create_dataset(
        name="My first dataset",
        description="For setting up the Superb AI Curate Python client"
    )

    images = [
        curate.Image(
            key="<unique image key>",
            source=curate.ImageSourceLocal(asset="/path/to/image"),
            metadata={"weather": "clear", "timeofday": "daytime"},
        )
    ]

    job = dataset.add_images(images=images)
    job.wait_until_complete()
