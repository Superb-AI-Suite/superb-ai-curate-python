import os
import sys

from sphinx_pyproject import SphinxConfig

sys.path.append(os.path.abspath("."))

# Configuration from pyproject.toml
config = SphinxConfig(globalns=globals())

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = config["project"]
copyright = config["copyright"]
author = config.author
release = version = config.version
documentation_summary = config.description

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = config["extensions"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_baseurl = config["html_baseurl"]
html_theme = config["html_theme"]
html_static_path = []

# Auto Type Hints
typehints_defaults = config["typehints_defaults"]


# Napoleon settings
