"""
Apply human-in-the-loop patches to structure analysis outputs.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from shared.models.structure_analysis import BoundingBox, SheetStructureAnalysisResponse
from shared.models.structure_patch import SheetStructurePatch, SheetStructurePatchOp

from funnel.services.structure_analysis import FunnelStructureAnalyzer


def _resolve_table_index(
    tables: List[Dict[str, Any]] | List[Any], op: SheetStructurePatchOp
) -> Optional[int]:
    if op.table_index is not None:
        idx = int(op.table_index)
        if 0 <= idx < len(tables):
            return idx
        return None
    if op.table_id:
        for i, t in enumerate(tables):
            try:
                if str(getattr(t, "id", None) or t.get("id")) == str(op.table_id):
                    return i
            except Exception:
                continue
    return None


def apply_structure_patch(
    analysis: SheetStructureAnalysisResponse,
    *,
    patch: SheetStructurePatch,
    grid: List[List[Any]],
    merged_cells: Optional[List[Any]] = None,
    cell_style_hints: Optional[List[List[int]]] = None,
    include_complex_types: bool = True,
    options: Optional[Dict[str, Any]] = None,
) -> SheetStructureAnalysisResponse:
    """
    Apply a stored patch to an analysis result.

    Strategy:
    - For table-affecting ops, re-analyze the target bbox with overrides (mode/header/bbox)
      so headers/schema/provenance stay consistent.
    - For KV ops, filter the extracted KV lists.
    """
    opts = options or {}

    tables = list(analysis.tables or [])
    global_kv = list(analysis.key_values or [])

    # Apply ops sequentially
    for op in patch.ops:
        if op.op == "remove_key_value":
            if not op.key:
                continue
            global_kv = [kv for kv in global_kv if kv.key != op.key]
            for t in tables:
                if getattr(t, "mode", None) == "property" and getattr(t, "key_values", None):
                    t.key_values = [kv for kv in (t.key_values or []) if kv.key != op.key]
            continue

        if op.op == "remove_table":
            idx = _resolve_table_index(tables, op)
            if idx is None:
                continue
            tables.pop(idx)
            continue

        idx = _resolve_table_index(tables, op)
        if idx is None:
            continue

        current = tables[idx]
        current_bbox = getattr(current, "bbox", None)
        bbox: BoundingBox
        if op.bbox is not None:
            bbox = op.bbox
        elif isinstance(current_bbox, BoundingBox):
            bbox = current_bbox
        else:
            # Unexpected; skip
            continue

        mode = op.mode or getattr(current, "mode", None)
        header_rows = op.header_rows if op.header_rows is not None else getattr(current, "header_rows", 1)
        header_cols = op.header_cols if op.header_cols is not None else getattr(current, "header_cols", 1)

        if op.op == "set_table_bbox":
            pass
        elif op.op == "set_table_mode":
            pass
        elif op.op == "set_header_rows":
            mode = "table"
        elif op.op == "set_header_cols":
            mode = "transposed"

        override_header_rows = int(header_rows) if mode == "table" else None
        override_header_cols = int(header_cols) if mode == "transposed" else None

        new_table = FunnelStructureAnalyzer.analyze_bbox(
            grid,
            bbox=bbox,
            include_complex_types=include_complex_types,
            merged_cells=merged_cells,
            cell_style_hints=cell_style_hints,
            options=opts,
            table_id=str(getattr(current, "id", f"table_{idx+1}")),
            override_mode=mode,
            override_header_rows=override_header_rows,
            override_header_cols=override_header_cols,
        )

        tables[idx] = new_table

    new_meta = dict(analysis.metadata or {})
    new_meta.update(
        {
            "patch_applied": True,
            "patch_signature": patch.sheet_signature,
            "patch_ops": len(patch.ops),
        }
    )

    return SheetStructureAnalysisResponse(
        tables=tables,
        key_values=global_kv,
        metadata=new_meta,
        warnings=list(analysis.warnings or []),
    )

