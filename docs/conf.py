"""
Sphinx configuration for the repo-wide documentation site.

Design goals:
- Keep docs and code consistent by generating auto-managed pages at build time.
- Build from Markdown-first sources via MyST.
- Provide full-code Python API reference via AutoAPI (AST-based, no imports).
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import warnings
from datetime import date
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKEND_ROOT = REPO_ROOT / "backend"

# Make backend packages importable for autodoc (e.g., `bff.*`, `shared.*`).
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(BACKEND_ROOT))

# Keep doc builds clean and deterministic (avoid noisy 3p warnings).
warnings.filterwarnings(
    "ignore",
    message=r".*pkg_resources is deprecated as an API.*",
    category=UserWarning,
)


class _AutoAPILogNoiseFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401
        msg = record.getMessage()
        if record.name.startswith("sphinx.autoapi") and msg.startswith("Unknown type: placeholder"):
            return False
        return True


logging.getLogger("sphinx.autoapi._mapper").addFilter(_AutoAPILogNoiseFilter())

_LINKCODE_REPO_URL: str | None = None
_LINKCODE_REV: str = "main"
_LINKCODE_LOOKUP: dict[str, tuple[str, int, int]] = {}


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
    # Source links / in-doc code browsing (must be loaded before AutoAPI so AutoAPI can
    # register the viewcode hooks and avoid importing modules with side effects).
    "sphinx.ext.viewcode",
    # Full-code API docs without importing modules (AST-based).
    "autoapi.extension",
    # Cross-references and usability
    "sphinx.ext.intersphinx",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.extlinks",
    # Enterprise doc QA / traceability
    "sphinx.ext.linkcode",
    "sphinx.ext.doctest",
    "sphinx.ext.coverage",
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

# AutoAPI configuration: index backend Python packages with proper structure.
#
# We target the backend directory specifically to preserve package hierarchy
# (e.g., bff.services.ontology_agent_models instead of flat ontology_agent_models).
autoapi_type = "python"
autoapi_dirs = [str(BACKEND_ROOT)]
autoapi_root = "reference/autoapi"
# Keep AutoAPI's own index/toctree generation enabled so its pages are discoverable.
# We avoid unwanted injection into the root toctree by explicitly linking to it in `docs/index.md`.
autoapi_add_toctree_entry = True
autoapi_file_patterns = ["*.py"]
autoapi_ignore = [
    "*/.git/*",
    "*/__pycache__/*",
    "*/.pytest_cache/*",
    "*/.mypy_cache/*",
    "*/.venv*/*",
    "*/venv/*",
    "*/env/*",
    "*/node_modules/*",
    "*/docs/_build/*",
    "*/tests/*",
    "*_test.py",
    "*/test_*.py",
    "*/conftest.py",
]
autoapi_options = [
    "members",
    "undoc-members",
    "show-inheritance",
    "show-module-summary",
    "imported-members",
]
# Include class __init__ docstrings and show class content from both class and __init__
autoapi_python_class_content = "both"
# Keep the original module names without flattening
autoapi_keep_files = True

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# Reduce non-actionable noise from large-scale code indexing.
suppress_warnings = [
    "autoapi.python_import_resolution",
    "ref.python",
]

# Doc QA defaults
coverage_show_missing_items = True
linkcheck_retries = 2
linkcheck_timeout = 10


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
        ["python", "scripts/generate_repo_file_inventory.py"],
    ]
    for args in generators:
        _run([python, *args[1:]] if args[0] == "python" else args)


def _normalize_git_remote_url(url: str) -> str | None:
    url = url.strip()
    if not url:
        return None

    if url.startswith("git@") and ":" in url:
        # git@github.com:org/repo.git -> https://github.com/org/repo
        host, path = url.split(":", 1)
        host = host.replace("git@", "")
        url = f"https://{host}/{path}"
    if url.startswith("ssh://git@"):
        # ssh://git@github.com/org/repo.git -> https://github.com/org/repo
        url = url.replace("ssh://git@", "https://", 1)

    if url.endswith(".git"):
        url = url[: -len(".git")]
    return url


def _detect_repo_url() -> str | None:
    env_url = os.environ.get("SPICE_DOCS_REPO_URL")
    if env_url:
        return _normalize_git_remote_url(env_url)
    try:
        remote = subprocess.check_output(
            ["git", "config", "--get", "remote.origin.url"],
            cwd=str(REPO_ROOT),
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return None
    return _normalize_git_remote_url(remote)


def _detect_git_rev() -> str:
    env_rev = os.environ.get("SPICE_DOCS_GIT_REF")
    if env_rev:
        return env_rev.strip()
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=str(REPO_ROOT),
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return "main"


def _init_linkcode(app) -> None:  # noqa: ANN001
    global _LINKCODE_REPO_URL, _LINKCODE_REV, _LINKCODE_LOOKUP

    _LINKCODE_REPO_URL = _detect_repo_url()
    _LINKCODE_REV = _detect_git_rev()

    lookup: dict[str, tuple[str, int, int]] = {}
    all_objects = getattr(app.env, "autoapi_all_objects", None)
    if all_objects:
        for full_name, obj in all_objects.items():
            file_path = obj.obj.get("file_path")
            if not file_path:
                continue
            try:
                rel = str(Path(file_path).resolve().relative_to(REPO_ROOT))
            except Exception:
                continue
            start = int(obj.obj.get("from_line_no") or 1)
            end = int(obj.obj.get("to_line_no") or start)
            lookup[full_name] = (rel, start, end)

    _LINKCODE_LOOKUP = lookup


def linkcode_resolve(domain: str, info: dict) -> str | None:
    # Called by `sphinx.ext.linkcode`. Must not import repo modules.
    if domain != "py":
        return None
    if not _LINKCODE_REPO_URL:
        return None

    module = (info.get("module") or "").strip()
    fullname = (info.get("fullname") or "").strip()
    if not module:
        return None

    key = f"{module}.{fullname}" if fullname else module
    entry = _LINKCODE_LOOKUP.get(key) or _LINKCODE_LOOKUP.get(module)
    if not entry:
        return None

    rel_path, start, end = entry
    if end != start:
        anchor = f"#L{start}-L{end}"
    else:
        anchor = f"#L{start}"
    return f"{_LINKCODE_REPO_URL}/blob/{_LINKCODE_REV}/{rel_path}{anchor}"


def _on_builder_inited(app) -> None:  # noqa: ANN001
    _run_repo_generators()


def _autodoc_process_docstring(app, what: str, name: str, obj, options, lines) -> None:  # noqa: ANN001
    """
    Process docstrings for AutoAPI modules.

    For docstrings that contain Markdown-style code fences (```), we wrap them
    in a literal block to prevent RST parsing errors. Otherwise, we preserve
    the docstring as-is to allow proper rendering of descriptions.
    """
    # AutoAPI emits this event with obj/options set to None (it is not using
    # Sphinx autodoc proper). Avoid mutating real autodoc docstrings.
    if obj is not None:
        return
    if not lines:
        return

    # Only wrap in code-block if the docstring contains Markdown code fences
    # that would break RST parsing
    content = "\n".join(lines)
    has_code_fence = "```" in content
    has_problematic_chars = any(c in content for c in ["***", "___", "```"])

    if has_code_fence or has_problematic_chars:
        wrapped: list[str] = [".. code-block:: text", ""]
        wrapped.extend([f"   {line.rstrip()}" for line in lines])
        lines[:] = wrapped


def _viewcode_find_source_clamped(app, modname: str):  # noqa: ANN001
    """
    Provide viewcode source/tags for AutoAPI modules without importing them.

    Why:
    - Some repo scripts have import-time side effects (network calls, bucket creation, etc).
    - Sphinx viewcode will import modules unless `viewcode-find-source` returns a result.
    - AutoAPI already provides a static-analysis implementation, but we defensively clamp
      tag line numbers to avoid rare index issues in Sphinx's HTML post-processing.
    """
    try:
        from autoapi.extension import viewcode_find as autoapi_viewcode_find  # noqa: WPS433
    except Exception:
        return None

    result = autoapi_viewcode_find(app, modname)
    if not result:
        return None

    source, tags = result
    max_line = max(1, len(source.splitlines()))

    clamped = {}
    for name, (type_, start, end) in tags.items():
        start_i = int(start) if start is not None else 1
        end_i = int(end) if end is not None else start_i
        start_i = max(1, min(start_i, max_line))
        end_i = max(start_i, min(end_i, max_line))
        clamped[name] = (type_, start_i, end_i)

    return source, clamped


def setup(app) -> None:  # noqa: D401
    """Sphinx entrypoint."""
    # Ensure viewcode can always render modules without importing them (side-effect safety).
    app.connect("viewcode-find-source", _viewcode_find_source_clamped, priority=0)
    # Run repo generators early so doc sources stay code-backed.
    app.connect("builder-inited", _on_builder_inited, priority=300)
    # Make AutoAPI docstrings robust to Markdown-ish formatting.
    app.connect("autodoc-process-docstring", _autodoc_process_docstring, priority=400)
    # Build the linkcode lookup after AutoAPI has populated env.autoapi_all_objects.
    app.connect("builder-inited", _init_linkcode, priority=700)
