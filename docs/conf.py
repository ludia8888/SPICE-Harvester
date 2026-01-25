"""
Sphinx configuration for the repo-wide documentation site.

Design goals:
- Keep docs and code consistent by generating auto-managed pages at build time.
- Build from Markdown-first sources via MyST.
- Provide Python API reference via autodoc/autosummary.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import date
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKEND_ROOT = REPO_ROOT / "backend"

# Make backend packages importable for autodoc (e.g., `bff.*`, `shared.*`).
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(BACKEND_ROOT))


project = "SPICE Harvester"
author = "SPICE Harvester"
copyright = f"{date.today().year}, {author}"


extensions = [
    # Markdown (MyST)
    "myst_parser",
    # API docs
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    # Cross-references and usability
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.extlinks",
    "sphinx.ext.todo",
    "sphinx.ext.duration",
    # Diagrams / UI helpers
    "sphinx.ext.graphviz",
    "sphinxcontrib.mermaid",
    "sphinx_copybutton",
    "sphinx_design",
]


source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}
root_doc = "index"

templates_path = ["_templates"]
exclude_patterns = [
    "_build",
    "Thumbs.db",
    ".DS_Store",
]

todo_include_todos = True

# MyST (Markdown) configuration
myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "tasklist",
]
myst_heading_anchors = 3

# Sphinx core quality-of-life options
autosectionlabel_prefix_document = True
autosummary_generate = True
autodoc_default_options = {
    "members": True,
    "show-inheritance": True,
}
autodoc_typehints = "description"
napoleon_google_docstring = True
napoleon_numpy_docstring = True

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}


html_theme = "sphinx_rtd_theme"
html_title = "SPICE Harvester Docs"
html_static_path = ["_static"]

# Make copybutton work well for shell examples.
copybutton_prompt_text = r">>> |\.\.\. |\$ "
copybutton_prompt_is_regexp = True

# Mermaid settings (best-effort; harmless if diagrams are not used).
mermaid_version = os.environ.get("SPHINX_MERMAID_VERSION", "10.6.1")


def _run(cmd: list[str]) -> None:
    subprocess.check_call(cmd, cwd=str(REPO_ROOT))


def _run_repo_generators() -> None:
    """
    Keep doc sources consistent with code by running the repo's generators.

    This happens during Sphinx's `builder-inited` hook so it runs before files are read.
    Set `SPICE_DOCS_SKIP_GENERATION=1` to disable (useful for debugging build issues).
    """
    if os.environ.get("SPICE_DOCS_SKIP_GENERATION") == "1":
        return

    python = sys.executable
    generators = [
        ["python", "scripts/generate_api_reference.py"],
        ["python", "scripts/generate_architecture_reference.py"],
        ["python", "scripts/generate_backend_methods.py"],
        ["python", "scripts/generate_error_taxonomy.py"],
        ["python", "scripts/generate_pipeline_tooling_reference.py"],
    ]
    for args in generators:
        _run([python, *args[1:]] if args[0] == "python" else args)


def _on_builder_inited(app) -> None:  # noqa: ANN001
    _run_repo_generators()


def setup(app) -> None:  # noqa: D401
    """Sphinx entrypoint."""
    app.connect("builder-inited", _on_builder_inited)
