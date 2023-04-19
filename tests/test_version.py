from spb_curate import version


def test_version():
    assert isinstance(version.VERSION, str)
    assert version.VERSION != ""
