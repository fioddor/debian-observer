"""Tests for the eventizer shape gate (the eventizer registry).

The registry's ``match_table_shape`` returns the *first* registered shape
whose requirements the columns carry — it cannot discriminate
eventizers when one shape is a superset of another. The gate
therefore takes the **shape declared by the selected eventizer**:
``assert_supported_table_shape(columns, *, table, shape=None)``.
``shape=None`` keeps the registry default.
"""

from __future__ import annotations

import pytest

from debian_observer.normalization.udddump import (
    UDDDumpError,
    assert_supported_table_shape,
    TableShape,
)

DEPENDENCIES_SHAPE = TableShape(
    name="dependencies",
    required_columns=frozenset(
        {"package", "version", "architecture", "distribution",
         "release", "depends", "pre_depends"}
    ),
)


class TestShapeGate:
    def test_shape_none_keeps_r1_behaviour(self) -> None:
        # gate against the registry; packages-shaped
        # columns pass (required: version + any of package/source).
        shape = assert_supported_table_shape(
            ["id", "package", "version", "architecture"], table="packages"
        )
        assert shape.name == "packages"

    def test_gate_uses_the_declared_shape(self) -> None:
        # The dependencies shape demands columns the packages shape
        # does not: gating with it must fail on packages columns.
        with pytest.raises(UDDDumpError, match="dependencies"):
            assert_supported_table_shape(
                ["id", "package", "version", "architecture"],
                table="packages",
                shape=DEPENDENCIES_SHAPE,
            )

    def test_gate_passes_when_the_declared_shape_fits(self) -> None:
        columns = [
            "package", "version", "architecture", "distribution",
            "release", "depends", "pre_depends", "provides",
        ]
        shape = assert_supported_table_shape(
            columns, table="packages", shape=DEPENDENCIES_SHAPE
        )
        assert shape.name == "dependencies"

    def test_error_names_the_eventizer_shapes(self) -> None:
        # The message names the table's own columns and the shapes
        # that were demanded (perimeter rule).
        with pytest.raises(UDDDumpError) as exc:
            assert_supported_table_shape(
                ["id", "source", "version"], table="sources",
                shape=DEPENDENCIES_SHAPE,
            )
        message = str(exc.value)
        assert "sources" in message
        assert "dependencies" in message
        assert "pre_depends" in message

    def test_table_shape_is_frozen(self) -> None:
        with pytest.raises(AttributeError):
            DEPENDENCIES_SHAPE.name = "renamed"