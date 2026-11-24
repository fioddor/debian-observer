"""The package version - the single source of truth.

Hatchling reads this file ([tool.hatch.version] in
pyproject.toml), so the built wheel, the sdist and the
runtime all see one value. Kept in its own module so the
devcontainer image can copy just this file before the editable
install (a version bump invalidates only this layer).
"""

__version__ = "1.1.0rc2"
