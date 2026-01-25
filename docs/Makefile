# Minimal Sphinx makefile (works on macOS/Linux).
#
# Use the active Python environment (venv) to avoid picking up a globally
# installed `sphinx-build` that may have a different set of extensions.
SPHINXBUILD = python -m sphinx
SOURCEDIR = .
BUILDDIR = _build

.PHONY: help clean

help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)"

clean:
	rm -rf "$(BUILDDIR)"

# Catch-all target: `make html`, `make linkcheck`, etc.
%:
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)"
