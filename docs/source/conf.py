# flake8: noqa
import os
import sys

sys.path.insert(0, os.path.abspath("../.."))


# -- Project information -----------------------------------------------------
import aqneodriver as pkg
import datetime

now = datetime.datetime.now()
project = pkg._version.__title__
authors = pkg._version.__authors__
copyright = "{year}, {authors}".format(year=now.year, authors=",".join(authors))
author = authors[0]
release = pkg.__version__

# -- General configuration ---------------------------------------------------
autosummary_generate = True
autoclass_content = "both"  # include both class docstring and __init__

autodoc_default_options = {
    "member-order": "bysource",
    "special-members": "__init__, __call__",
    "exclude-members": "__weakref__",
    "inherited-members": True,
}

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx_autodoc_typehints",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosectionlabel",
    "sphinx_rtd_theme"
    # 'sphinxcontrib.katex',
]

# Disable docstring inheritance
autodoc_inherit_docstrings = False

always_document_param_types = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates", "_templates/autosummary"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
import sphinx_rtd_theme

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_theme = "sphinx_rtd_theme"

html_theme_options = {
    "canonical_url": "",
    # 'analytics_id': 'UA-XXXXXXX-1',  #  Provided by Google in your dashboard
    "logo_only": False,
    "display_version": True,
    "prev_next_buttons_location": "bottom",
    "style_external_links": False,
    "vcs_pageview_mode": "",
    # 'style_nav_header_background': 'white',
    # Toc options
    "collapse_navigation": True,
    "sticky_navigation": True,
    "navigation_depth": 4,
    "includehidden": True,
    "titles_only": False,
}

intersphinx_mapping = {
    "pydent": ("https://aquariumbio.github.io/trident/", None),
    "omegaconf": ("https://omegaconf.readthedocs.io/en/latest/", None),
}
