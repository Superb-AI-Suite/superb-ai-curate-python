try:
    import importlib.metadata as metadata
except ModuleNotFoundError:
    import importlib_metadata as metadata


VERSION = metadata.version("superb-ai-curate")
