"""
🔥 THINK ULTRA! Structure Analysis Engine (Funnel)

Goal: Convert "messy spreadsheet grids" into normalized table blocks and metadata.

Core modules:
A) Data Island detection (multi-table split)
B) Orientation / mode inference (table vs transposed vs property)
C) Merged cell flattening (optional when merge ranges provided)
D) Key-Value extraction (form parsing)
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import statistics
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import re

from shared.models.common import DataType
from shared.models.structure_analysis import (
    BoundingBox,
    CellAddress,
    ColumnProvenance,
    DetectedTable,
    KeyValueItem,
    MergeRange,
    SheetStructureAnalysisResponse,
)

from funnel.services.type_inference import FunnelTypeInferenceService


@dataclass(frozen=True)
class _CellInfo:
    row: int
    col: int
    raw: Any
    text: str
    inferred_type: str
    is_empty: bool
    score: float
    is_typed: bool
    is_label_like: bool
    is_header_like: bool


class FunnelStructureAnalyzer:
    """Structure analyzer for sheet-like 2D grids."""

    # Conservative defaults; override via request.options
    DEFAULTS: Dict[str, Any] = {
        "max_header_scan": 3,
        "min_component_cells": 3,
        "min_bbox_area": 6,
        "min_density": 0.22,
        "core_score_threshold": 0.70,
        "expand_score_threshold": 0.45,
        "kv_search_radius": 4,
        # How many record rows to include in DetectedTable.sample_rows.
        # - Default 20 for preview.
        # - Set to a larger number (or -1) for full extraction (e.g., import).
        "sample_row_limit": 20,
    }

    @staticmethod
    def _is_blank(value: Any) -> bool:
        if value is None:
            return True
        return str(value).strip() == ""

    @classmethod
    def analyze(
        cls,
        grid: List[List[Any]],
        *,
        include_complex_types: bool = True,
        merged_cells: Optional[List[MergeRange]] = None,
        max_tables: int = 5,
        options: Optional[Dict[str, Any]] = None,
    ) -> SheetStructureAnalysisResponse:
        opts = dict(cls.DEFAULTS)
        if options:
            opts.update(options)

        if not grid:
            return SheetStructureAnalysisResponse(
                tables=[],
                key_values=[],
                metadata={"rows": 0, "cols": 0, "islands_found": 0},
            )

        normalized_grid_base = cls._normalize_grid(grid)
        cell_map_base, row_stats_base = cls._score_cells(normalized_grid_base, include_complex_types)
        islands_base = cls._detect_data_islands(
            normalized_grid_base,
            cell_map_base,
            row_stats=row_stats_base,
            max_tables=max_tables,
            opts=opts,
        )

        # Apply merged-cell flattening only within detected data-island regions.
        normalized_grid = normalized_grid_base
        if merged_cells and islands_base:
            normalized_grid = cls._flatten_merged_cells(
                normalized_grid_base,
                merged_cells,
                include_complex_types,
                fill_boxes=islands_base,
            )

        cell_map, row_stats = cls._score_cells(normalized_grid, include_complex_types)
        islands = cls._detect_data_islands(
            normalized_grid,
            cell_map,
            row_stats=row_stats,
            max_tables=max_tables,
            opts=opts,
        )

        tables: List[DetectedTable] = []
        covered_boxes: List[BoundingBox] = []
        for idx, bbox in enumerate(islands):
            table = cls._analyze_island(
                normalized_grid,
                cell_map,
                bbox=bbox,
                include_complex_types=include_complex_types,
                table_id=f"table_{idx+1}",
                opts=opts,
            )
            tables.append(table)
            covered_boxes.append(bbox)

        kv_items = cls._extract_key_values(
            normalized_grid,
            cell_map,
            exclude_boxes=covered_boxes,
            include_complex_types=include_complex_types,
            opts=opts,
        )

        return SheetStructureAnalysisResponse(
            tables=tables,
            key_values=kv_items,
            metadata={
                "rows": len(normalized_grid),
                "cols": max((len(r) for r in normalized_grid), default=0),
                "islands_found": len(islands),
            },
        )

    # ---------------------------
    # Module A: Data islands
    # ---------------------------

    @classmethod
    def _detect_data_islands(
        cls,
        grid: List[List[Any]],
        cell_map: Dict[Tuple[int, int], _CellInfo],
        *,
        row_stats: Dict[int, Dict[str, int]],
        max_tables: int,
        opts: Dict[str, Any],
    ) -> List[BoundingBox]:
        core_thr = float(opts["core_score_threshold"])
        expand_thr = float(opts["expand_score_threshold"])
        max_header_scan = int(opts["max_header_scan"])
        min_cells = int(opts["min_component_cells"])
        min_area = int(opts["min_bbox_area"])
        min_density = float(opts["min_density"])

        cols = max((len(r) for r in grid), default=0)
        col_non_empty = [0 for _ in range(cols)]
        for r, row in enumerate(grid):
            for c in range(min(cols, len(row))):
                if not cls._is_blank(row[c]):
                    col_non_empty[c] += 1

        core_points = {(r, c) for (r, c), ci in cell_map.items() if ci.score >= core_thr}
        if not core_points:
            # Fallback: allow weaker points when nothing looks typed.
            core_points = {(r, c) for (r, c), ci in cell_map.items() if ci.score >= expand_thr}
        else:
            # If we already have strong signals somewhere, still allow detection of "text-only tables"
            # by including structured (row/col dense) string cells, even when their score < core_thr.
            structured_thr = float(opts.get("structured_score_threshold", 0.35))
            for (r, c), ci in cell_map.items():
                if ci.score < structured_thr:
                    continue
                rs = row_stats.get(r, {})
                if rs.get("non_empty", 0) < 2:
                    continue
                if c < 0 or c >= len(col_non_empty) or col_non_empty[c] < 2:
                    continue
                core_points.add((r, c))

        components = cls._connected_components(core_points)
        candidates: List[Tuple[float, BoundingBox]] = []

        for comp in components:
            if len(comp) < min_cells:
                continue
            bbox = cls._bbox_for_points(comp)
            bbox = cls._expand_bbox_to_dense_region(
                grid,
                bbox,
                cell_map=cell_map,
                expand_threshold=expand_thr,
                max_header_scan=max_header_scan,
            )
            bbox = cls._tighten_bbox(grid, bbox)
            area = cls._bbox_area(bbox)
            if area < min_area:
                continue

            non_empty = cls._count_non_empty_in_bbox(grid, bbox)
            density = non_empty / area if area else 0.0
            if density < min_density:
                continue

            typed_cells = sum(
                1
                for (r, c) in comp
                if cell_map.get((r, c)) and cell_map[(r, c)].is_typed
            )

            # Light penalty for sparse top title-like rows
            title_penalty = 0.0
            for r in range(bbox.top, min(bbox.top + 2, bbox.bottom + 1)):
                rs = row_stats.get(r, {})
                if rs.get("non_empty", 0) <= 2 and rs.get("typed", 0) == 0:
                    title_penalty += 0.5

            score = (
                len(comp)
                + typed_cells * 2.0
                + density * 10.0
                - title_penalty
            )
            candidates.append((score, bbox))

        candidates.sort(reverse=True, key=lambda t: t[0])
        selected = [b for _, b in candidates[:max_tables]]

        # Post-process: split bboxes that contain separator rows (memo lines etc.)
        split_boxes: List[BoundingBox] = []
        for b in selected:
            split_boxes.extend(
                cls._split_bbox_by_row_separators(grid, b, cell_map=cell_map, opts=opts)
            )

        # Hybrid doc split: Key-Value header + line-item table without blank gaps
        hybrid_split: List[BoundingBox] = []
        for b in split_boxes:
            hybrid_split.extend(cls._split_bbox_by_row_profile(grid, b, opts=opts))

        # Re-rank after split
        ranked: List[Tuple[float, BoundingBox]] = []
        for b in hybrid_split:
            ranked.append((cls._bbox_quality_score(grid, b, cell_map=cell_map), b))
        ranked.sort(reverse=True, key=lambda t: t[0])
        return [b for _, b in ranked[:max_tables]]

    @classmethod
    def _bbox_quality_score(
        cls, grid: List[List[Any]], bbox: BoundingBox, *, cell_map: Dict[Tuple[int, int], _CellInfo]
    ) -> float:
        area = cls._bbox_area(bbox)
        if area <= 0:
            return 0.0
        non_empty = cls._count_non_empty_in_bbox(grid, bbox)
        density = non_empty / area
        typed = 0
        scored = 0
        for r in range(bbox.top, bbox.bottom + 1):
            for c in range(bbox.left, bbox.right + 1):
                ci = cell_map.get((r, c))
                if not ci:
                    continue
                if ci.is_typed:
                    typed += 1
                if ci.score >= 0.50:
                    scored += 1
        return density * 10.0 + typed * 2.0 + scored * 0.5 + min(5.0, area / 50.0)

    @classmethod
    def _split_bbox_by_row_separators(
        cls,
        grid: List[List[Any]],
        bbox: BoundingBox,
        *,
        cell_map: Dict[Tuple[int, int], _CellInfo],
        opts: Dict[str, Any],
    ) -> List[BoundingBox]:
        """
        Split a bbox into multiple bboxes when internal separator rows exist.

        Separator rows:
        - empty rows
        - memo/annotation rows with very low density and no typed evidence
        """
        top, left, bottom, right = bbox.top, bbox.left, bbox.bottom, bbox.right
        if bottom - top + 1 < 4:
            return [bbox]

        def row_non_empty(r: int) -> int:
            return sum(1 for c in range(left, right + 1) if not cls._is_blank(grid[r][c]))

        def row_typed(r: int) -> int:
            return sum(1 for c in range(left, right + 1) if (ci := cell_map.get((r, c))) is not None and ci.is_typed)

        def row_has_memo_like_text(r: int) -> bool:
            texts: List[str] = []
            for c in range(left, right + 1):
                v = grid[r][c]
                if cls._is_blank(v):
                    continue
                texts.append(str(v).strip())

            if not texts:
                return False

            # Domain-neutral heuristic: separator/memo rows often look like "TAG + sentence"
            # rather than tabular cells. Avoid keyword lists; rely on shape.
            for t in texts:
                if len(t) >= 50:
                    return True

            def is_tag_like(t: str) -> bool:
                tt = t.strip()
                if not tt:
                    return False
                if len(tt) > 6:
                    return False
                if any(ch.isdigit() for ch in tt):
                    return False
                # Works for Unicode letters (Korean/Latin/etc.)
                return tt.replace("_", "").isalpha()

            def is_sentence_like(t: str) -> bool:
                tt = t.strip()
                if len(tt) >= 25:
                    return True
                if " " in tt and len(tt) >= 8:
                    return True
                return False

            tag_like = any(is_tag_like(t) for t in texts)
            sentence_like = any(is_sentence_like(t) for t in texts)
            return tag_like and sentence_like

        sep_rows: List[int] = []
        for r in range(top, bottom + 1):
            ne = row_non_empty(r)
            if ne == 0:
                sep_rows.append(r)
                continue
            if row_typed(r) == 0 and ne <= 2 and row_has_memo_like_text(r):
                sep_rows.append(r)

        if not sep_rows:
            return [bbox]

        segments: List[Tuple[int, int]] = []
        start = top
        for r in range(top, bottom + 1):
            if r in sep_rows:
                if start <= r - 1:
                    segments.append((start, r - 1))
                start = r + 1
        if start <= bottom:
            segments.append((start, bottom))

        min_area = int(opts["min_bbox_area"])
        min_density = float(opts["min_density"])
        out: List[BoundingBox] = []
        for seg_top, seg_bottom in segments:
            b = BoundingBox(top=seg_top, left=left, bottom=seg_bottom, right=right)
            b = cls._tighten_bbox(grid, b)
            area = cls._bbox_area(b)
            if area < min_area:
                continue
            density = cls._count_non_empty_in_bbox(grid, b) / area if area else 0.0
            if density < min_density:
                continue
            out.append(b)

        return out or [bbox]

    @classmethod
    def _split_bbox_by_row_profile(
        cls, grid: List[List[Any]], bbox: BoundingBox, *, opts: Dict[str, Any]
    ) -> List[BoundingBox]:
        """
        Split hybrid blocks where top rows are narrow (key-value) and bottom rows are wide (table).
        """
        top, left, bottom, right = bbox.top, bbox.left, bbox.bottom, bbox.right
        width = right - left + 1
        if width < 3 or bottom - top + 1 < 6:
            return [bbox]

        active_counts: List[Tuple[int, int]] = []
        for r in range(top, bottom + 1):
            ne = sum(1 for c in range(left, right + 1) if not cls._is_blank(grid[r][c]))
            if ne == 0:
                continue
            active_counts.append((r, ne))

        if len(active_counts) < 6:
            return [bbox]

        wide_thr = 3
        best_split: Optional[int] = None
        for split_row in range(top + 2, bottom - 1):
            top_part = [ne for r, ne in active_counts if r < split_row]
            bottom_part = [ne for r, ne in active_counts if r >= split_row]
            if len(top_part) < 3 or len(bottom_part) < 3:
                continue

            top_narrow_ratio = sum(1 for ne in top_part if ne <= 2) / len(top_part)
            bottom_wide_ratio = sum(1 for ne in bottom_part if ne >= wide_thr) / len(bottom_part)

            if top_narrow_ratio >= 0.75 and bottom_wide_ratio >= 0.75:
                best_split = split_row
                break

        if best_split is None:
            return [bbox]

        b1 = cls._tighten_bbox(grid, BoundingBox(top=top, left=left, bottom=best_split - 1, right=right))
        b2 = cls._tighten_bbox(grid, BoundingBox(top=best_split, left=left, bottom=bottom, right=right))

        min_area = int(opts["min_bbox_area"])
        out: List[BoundingBox] = []
        for b in (b1, b2):
            if cls._bbox_area(b) >= min_area:
                out.append(b)
        return out or [bbox]

    @classmethod
    def _expand_bbox_to_dense_region(
        cls,
        grid: List[List[Any]],
        bbox: BoundingBox,
        *,
        cell_map: Dict[Tuple[int, int], _CellInfo],
        expand_threshold: float,
        max_header_scan: int,
    ) -> BoundingBox:
        """
        Expand a bbox derived from "core" cells to cover adjacent string/header cells that
        belong to the same table island (e.g., name column next to price column).
        """
        rows = len(grid)
        cols = max((len(r) for r in grid), default=0)
        top, left, bottom, right = bbox.top, bbox.left, bbox.bottom, bbox.right

        def row_non_empty(r: int, l: int, rr: int) -> int:
            return sum(1 for c in range(l, rr + 1) if not cls._is_blank(grid[r][c]))

        def col_non_empty(c: int, t: int, b: int) -> int:
            return sum(1 for r in range(t, b + 1) if not cls._is_blank(grid[r][c]))

        def row_scored(r: int, l: int, rr: int) -> int:
            return sum(
                1
                for c in range(l, rr + 1)
                if (ci := cell_map.get((r, c))) is not None and ci.score >= expand_threshold
            )

        def col_scored(c: int, t: int, b: int) -> int:
            return sum(
                1
                for r in range(t, b + 1)
                if (ci := cell_map.get((r, c))) is not None and ci.score >= expand_threshold
            )

        # Iteratively expand until boundaries look like separators
        changed = True
        while changed:
            changed = False
            height = bottom - top + 1
            width = right - left + 1

            # Expand left/right based on non-empty and scored support in the adjacent column
            if left - 1 >= 0:
                ne = col_non_empty(left - 1, top, bottom)
                sc = col_scored(left - 1, top, bottom)
                if ne >= max(2, int(height * 0.20)) and (sc >= 1 or ne >= int(height * 0.50)):
                    left -= 1
                    changed = True

            if right + 1 < cols:
                ne = col_non_empty(right + 1, top, bottom)
                sc = col_scored(right + 1, top, bottom)
                if ne >= max(2, int(height * 0.20)) and (sc >= 1 or ne >= int(height * 0.50)):
                    right += 1
                    changed = True

            # Expand bottom based on density in the adjacent row
            if bottom + 1 < rows:
                ne = row_non_empty(bottom + 1, left, right)
                sc = row_scored(bottom + 1, left, right)
                if ne >= max(2, int(width * 0.20)) and (sc >= 1 or ne >= int(width * 0.50)):
                    bottom += 1
                    changed = True

            # Expand up (limited, header-oriented)
            if top - 1 >= 0:
                ne = row_non_empty(top - 1, left, right)
                sc = row_scored(top - 1, left, right)
                if ne == 0:
                    pass
                else:
                    # Prefer header-like expansion: allow up to max_header_scan rows
                    if (bbox.top - (top - 1)) <= max_header_scan and (sc >= 1 or ne >= int(width * 0.30)):
                        top -= 1
                        changed = True

        return BoundingBox(top=top, left=left, bottom=bottom, right=right)

    # ---------------------------
    # Module B: Orientation / mode
    # ---------------------------

    @classmethod
    def _analyze_island(
        cls,
        grid: List[List[Any]],
        cell_map: Dict[Tuple[int, int], _CellInfo],
        *,
        bbox: BoundingBox,
        include_complex_types: bool,
        table_id: str,
        opts: Dict[str, Any],
    ) -> DetectedTable:
        sub = cls._slice_bbox(grid, bbox)
        height = len(sub)
        width = max((len(r) for r in sub), default=0)
        max_header_scan = min(int(opts["max_header_scan"]), max(0, height - 1), max(0, width - 1))

        # Property (Key-Value) mode heuristic
        property_candidate = cls._score_property_mode(sub, bbox=bbox, cell_map=cell_map)

        best_row = cls._best_header_row_candidate(sub, bbox=bbox, cell_map=cell_map, max_k=max_header_scan, include_complex_types=include_complex_types)
        best_col = cls._best_header_col_candidate(sub, bbox=bbox, cell_map=cell_map, max_k=max_header_scan, include_complex_types=include_complex_types)

        # Decide final mode
        if property_candidate.confidence >= max(best_row.confidence, best_col.confidence) and property_candidate.confidence >= 0.70:
            kv = cls._extract_property_table_kv(sub, bbox=bbox, cell_map=cell_map)
            return DetectedTable(
                id=table_id,
                bbox=bbox,
                mode="property",
                confidence=property_candidate.confidence,
                reason=property_candidate.reason,
                header_rows=0,
                header_cols=0,
                headers=[],
                sample_rows=[],
                inferred_schema=None,
                key_values=kv,
            )

        if best_col.confidence > best_row.confidence + 0.05:
            headers, rows, field_row_offsets = cls._pivot_transposed(sub, header_cols=best_col.k)
            inferred = cls._infer_schema(headers, rows, include_complex_types=include_complex_types)
            provenance = cls._build_transposed_column_provenance(
                headers,
                bbox=bbox,
                header_cols=best_col.k,
                field_row_offsets=field_row_offsets,
            )

            row_limit = opts.get("sample_row_limit", 20)
            if row_limit is None:
                sample_rows = rows
            else:
                try:
                    row_limit = int(row_limit)
                except Exception:
                    row_limit = 20
                sample_rows = rows if row_limit < 0 else rows[: min(row_limit, len(rows))]
            return DetectedTable(
                id=table_id,
                bbox=bbox,
                mode="transposed",
                confidence=best_col.confidence,
                reason=best_col.reason,
                header_rows=0,
                header_cols=best_col.k,
                header_grid=[[h] for h in headers],
                headers=headers,
                sample_rows=sample_rows,
                inferred_schema=inferred,
                column_provenance=provenance,
            )

        headers, rows, header_grid = cls._extract_table(sub, header_rows=best_row.k)
        inferred = cls._infer_schema(headers, rows, include_complex_types=include_complex_types)
        provenance = cls._build_table_column_provenance(headers, bbox=bbox, header_rows=best_row.k)

        row_limit = opts.get("sample_row_limit", 20)
        if row_limit is None:
            sample_rows = rows
        else:
            try:
                row_limit = int(row_limit)
            except Exception:
                row_limit = 20
            sample_rows = rows if row_limit < 0 else rows[: min(row_limit, len(rows))]
        return DetectedTable(
            id=table_id,
            bbox=bbox,
            mode="table",
            confidence=best_row.confidence,
            reason=best_row.reason,
            header_rows=best_row.k,
            header_cols=0,
            header_grid=header_grid,
            headers=headers,
            sample_rows=sample_rows,
            inferred_schema=inferred,
            column_provenance=provenance,
        )

    @dataclass(frozen=True)
    class _CandidateScore:
        k: int
        confidence: float
        reason: str

    @classmethod
    def _score_property_mode(
        cls,
        sub: List[List[Any]],
        *,
        bbox: BoundingBox,
        cell_map: Dict[Tuple[int, int], _CellInfo],
    ) -> _CandidateScore:
        height = len(sub)
        width = max((len(r) for r in sub), default=0)
        if height < 3 or width < 2 or width > 4:
            return cls._CandidateScore(k=0, confidence=0.0, reason="Not a property-table shape")

        # Property tables are effectively "2-column" forms (key/value).
        # If 3rd+ columns are actively populated, prefer table/transposed.
        active_cols = 0
        active_min = max(2, int(height * 0.30))
        for c in range(width):
            non_empty = 0
            for r in range(height):
                v = sub[r][c] if c < len(sub[r]) else None
                if v is not None and str(v).strip() != "":
                    non_empty += 1
            if non_empty >= active_min:
                active_cols += 1
        if active_cols > 2:
            return cls._CandidateScore(
                k=0,
                confidence=0.0,
                reason=f"Not a property table (active_cols={active_cols})",
            )

        keys: List[str] = []
        key_text_like = 0
        for r in range(height):
            v = sub[r][0] if 0 < len(sub[r]) else None
            t = "" if v is None else str(v).strip()
            if t:
                keys.append(t)
                ci = cell_map.get((bbox.top + r, bbox.left + 0))
                if ci and ci.is_header_like:
                    key_text_like += 1

        if not keys:
            return cls._CandidateScore(k=0, confidence=0.0, reason="No keys found")

        unique_ratio = len(set(keys)) / len(keys)
        key_quality = key_text_like / max(1, len(keys))

        # Values column should have some typed cells (money/date/number/etc) or non-empty density
        value_cells = 0
        value_typed = 0
        for r in range(height):
            v = sub[r][1] if 1 < len(sub[r]) else None
            t = "" if v is None else str(v).strip()
            if not t:
                continue
            value_cells += 1
            ci = cell_map.get((bbox.top + r, bbox.left + 1))
            if ci and ci.is_typed:
                value_typed += 1

        if value_cells == 0:
            return cls._CandidateScore(k=0, confidence=0.0, reason="No values found")

        typed_ratio = value_typed / value_cells if value_cells else 0.0
        confidence = min(1.0, 0.35 + unique_ratio * 0.35 + key_quality * 0.2 + typed_ratio * 0.25)
        return cls._CandidateScore(
            k=0,
            confidence=confidence,
            reason=f"Property-mode heuristic (unique_keys={unique_ratio:.2f}, key_quality={key_quality:.2f}, typed_values={typed_ratio:.2f})",
        )

    @classmethod
    def _best_header_row_candidate(
        cls,
        sub: List[List[Any]],
        *,
        bbox: BoundingBox,
        cell_map: Dict[Tuple[int, int], _CellInfo],
        max_k: int,
        include_complex_types: bool,
    ) -> _CandidateScore:
        best = cls._CandidateScore(k=1, confidence=0.0, reason="No candidate")
        height = len(sub)
        width = max((len(r) for r in sub), default=0)
        if height < 2 or width < 2:
            return best

        for k in range(1, max(1, max_k) + 1):
            header_score = cls._header_row_score(sub, bbox=bbox, cell_map=cell_map, header_rows=k)
            columns = cls._extract_columns_from_sub(sub[k:])
            consistency = cls._axis_type_consistency(columns, include_complex_types=include_complex_types)
            conf = min(1.0, 0.45 * header_score + 0.55 * consistency)
            reason = f"HeaderRow k={k}: header={header_score:.2f}, col_consistency={consistency:.2f}"
            if conf > best.confidence:
                best = cls._CandidateScore(k=k, confidence=conf, reason=reason)
        return best

    @classmethod
    def _best_header_col_candidate(
        cls,
        sub: List[List[Any]],
        *,
        bbox: BoundingBox,
        cell_map: Dict[Tuple[int, int], _CellInfo],
        max_k: int,
        include_complex_types: bool,
    ) -> _CandidateScore:
        best = cls._CandidateScore(k=1, confidence=0.0, reason="No candidate")
        height = len(sub)
        width = max((len(r) for r in sub), default=0)
        if height < 2 or width < 2:
            return best

        for k in range(1, max(1, max_k) + 1):
            header_score = cls._header_col_score(sub, bbox=bbox, cell_map=cell_map, header_cols=k)
            rows = cls._extract_rows_from_sub(sub, start_col=k)
            consistency = cls._axis_type_consistency(rows, include_complex_types=include_complex_types)
            conf = min(1.0, 0.45 * header_score + 0.55 * consistency)
            reason = f"HeaderCol k={k}: header={header_score:.2f}, row_consistency={consistency:.2f}"
            if conf > best.confidence:
                best = cls._CandidateScore(k=k, confidence=conf, reason=reason)
        return best

    @classmethod
    def _header_row_score(
        cls,
        sub: List[List[Any]],
        *,
        bbox: BoundingBox,
        cell_map: Dict[Tuple[int, int], _CellInfo],
        header_rows: int,
    ) -> float:
        total = 0
        good = 0
        uniq: set[str] = set()
        for r in range(min(header_rows, len(sub))):
            row = sub[r]
            for c, v in enumerate(row):
                t = "" if v is None else str(v).strip()
                if not t:
                    continue
                total += 1
                ci = cell_map.get((bbox.top + r, bbox.left + c))
                if (
                    ci
                    and (ci.is_header_like or (ci.is_label_like and cls._looks_like_kv_label(t)))
                    and not ci.is_typed
                    and not cls._looks_like_data_value_text(t)
                ):
                    good += 1
                uniq.add(t.lower())
        if total == 0:
            return 0.0
        uniqueness = min(1.0, len(uniq) / total)
        return min(1.0, (good / total) * 0.75 + uniqueness * 0.25)

    @classmethod
    def _header_col_score(
        cls,
        sub: List[List[Any]],
        *,
        bbox: BoundingBox,
        cell_map: Dict[Tuple[int, int], _CellInfo],
        header_cols: int,
    ) -> float:
        height = len(sub)
        total = 0
        good = 0
        uniq: set[str] = set()
        for r in range(height):
            row = sub[r]
            for c in range(min(header_cols, len(row))):
                v = row[c]
                t = "" if v is None else str(v).strip()
                if not t:
                    continue
                total += 1
                ci = cell_map.get((bbox.top + r, bbox.left + c))
                if (
                    ci
                    and (ci.is_header_like or (ci.is_label_like and cls._looks_like_kv_label(t)))
                    and not ci.is_typed
                    and not cls._looks_like_data_value_text(t)
                ):
                    good += 1
                uniq.add(t.lower())
        if total == 0:
            return 0.0
        uniqueness = min(1.0, len(uniq) / total)
        return min(1.0, (good / total) * 0.75 + uniqueness * 0.25)

    @classmethod
    def _axis_type_consistency(cls, sequences: List[List[Any]], *, include_complex_types: bool) -> float:
        if not sequences:
            return 0.0
        scores: List[float] = []
        for seq in sequences:
            values = [v for v in seq if v is not None and str(v).strip() != ""]
            if len(values) < 2:
                continue
            result = FunnelTypeInferenceService.infer_column_type(
                values,
                column_name=None,
                include_complex_types=include_complex_types,
            ).inferred_type
            base = result.confidence
            if result.type == DataType.STRING.value:
                # For text-only tables, use structural consistency instead of always penalizing.
                str_values = [str(v).strip() for v in values if str(v).strip()]
                struct = cls._string_sequence_consistency(str_values)
                base = 0.40 + 0.50 * struct
            scores.append(base)
        if not scores:
            return 0.0
        return sum(scores) / len(scores)

    @classmethod
    def _string_sequence_consistency(cls, values: List[str]) -> float:
        """
        Estimate how "table-like" a string-only sequence is.

        Signals:
        - length stability
        - token-count stability
        - formatting signature stability (digits/letters mix)
        """
        if len(values) < 2:
            return 0.0

        lengths = [len(v) for v in values]
        mean_len = statistics.mean(lengths) if lengths else 0.0
        cv_len = (statistics.pstdev(lengths) / mean_len) if mean_len else 0.0
        len_score = max(0.0, 1.0 - min(1.0, cv_len))

        token_counts = [len(re.findall(r"[A-Za-z가-힣0-9]+", v)) for v in values]
        mean_tok = statistics.mean(token_counts) if token_counts else 0.0
        cv_tok = (statistics.pstdev(token_counts) / mean_tok) if mean_tok else 0.0
        tok_score = max(0.0, 1.0 - min(1.0, cv_tok))

        def sig(s: str) -> str:
            out = []
            for ch in s:
                if ch.isdigit():
                    out.append("0")
                elif "A" <= ch <= "Z" or "a" <= ch <= "z":
                    out.append("a")
                elif "가" <= ch <= "힣":
                    out.append("h")
                elif ch.isspace():
                    out.append(" ")
                else:
                    out.append(ch)
            return "".join(out).strip()

        sigs: Dict[str, int] = {}
        for v in values:
            s = sig(v)
            sigs[s] = sigs.get(s, 0) + 1
        mode = max(sigs.values()) if sigs else 0
        sig_score = mode / len(values) if values else 0.0

        return max(0.0, min(1.0, 0.35 * len_score + 0.25 * tok_score + 0.40 * sig_score))

    # ---------------------------
    # Module C: Merged cell flattening
    # ---------------------------

    @classmethod
    def _flatten_merged_cells(
        cls,
        grid: List[List[Any]],
        merged_cells: List[MergeRange],
        include_complex_types: bool,
        *,
        fill_boxes: Optional[List[BoundingBox]] = None,
    ) -> List[List[Any]]:
        out = [list(r) for r in grid]
        for mr in merged_cells:
            if fill_boxes is not None and not any(cls._bboxes_intersect(mr, b) for b in fill_boxes):
                continue
            top_left = cls._get_cell(out, mr.top, mr.left)
            if top_left is None or str(top_left).strip() == "":
                continue
            if not cls._should_fill_merge(top_left, mr, include_complex_types=include_complex_types):
                continue
            for r in range(mr.top, mr.bottom + 1):
                cls._ensure_row_len(out, r, mr.right + 1)
                for c in range(mr.left, mr.right + 1):
                    if r == mr.top and c == mr.left:
                        continue
                    out[r][c] = top_left
        return out

    @classmethod
    def _should_fill_merge(cls, value: Any, mr: MergeRange, *, include_complex_types: bool) -> bool:
        text = str(value).strip()
        inferred_type = cls._infer_single_value_type(text, include_complex_types)
        if inferred_type != DataType.STRING.value:
            return True

        height = mr.bottom - mr.top + 1
        width = mr.right - mr.left + 1

        # Very large *wide* blocks are likely decorative titles; tall/narrow blocks
        # are frequently category/group cells in real-world Excel.
        if height * width >= 20 and width >= 3:
            return False

        # If it looks like a label/category, allow fill
        if cls._is_header_like_text(text) or cls._is_label_like_text(text):
            return True

        return False

    @staticmethod
    def _bboxes_intersect(a: BoundingBox, b: BoundingBox) -> bool:
        return not (
            a.right < b.left
            or a.left > b.right
            or a.bottom < b.top
            or a.top > b.bottom
        )

    # ---------------------------
    # Module D: Key-Value extraction
    # ---------------------------

    @classmethod
    def _extract_key_values(
        cls,
        grid: List[List[Any]],
        cell_map: Dict[Tuple[int, int], _CellInfo],
        *,
        exclude_boxes: List[BoundingBox],
        include_complex_types: bool,
        opts: Dict[str, Any],
    ) -> List[KeyValueItem]:
        radius = int(opts["kv_search_radius"])
        items: List[KeyValueItem] = []

        def _in_excluded(r: int, c: int) -> bool:
            for b in exclude_boxes:
                if b.top <= r <= b.bottom and b.left <= c <= b.right:
                    return True
            return False

        # 1) Value-driven extraction: strong typed values find nearest label (left/up)
        for (r, c), ci in cell_map.items():
            if _in_excluded(r, c):
                continue
            if ci.is_empty:
                continue
            if not ci.is_typed:
                continue

            label = cls._find_nearest_label(cell_map, r, c, radius=radius)
            if label is None:
                continue

            key_ci, score, reason = label
            key = key_ci.text.rstrip(":").strip()
            if not key:
                continue

            inferred_type = cls._infer_single_value_type(ci.text, include_complex_types)
            items.append(
                KeyValueItem(
                    key=key,
                    value=ci.raw,
                    value_type=inferred_type,
                    confidence=min(1.0, 0.60 + score * 0.40),
                    key_cell=CellAddress(row=key_ci.row, col=key_ci.col),
                    value_cell=CellAddress(row=r, col=c),
                    reason=reason,
                )
            )

        # 2) Label-driven extraction: label cells look for nearby values (right/down)
        for (r, c), label_ci in cell_map.items():
            if _in_excluded(r, c):
                continue
            if label_ci.is_empty:
                continue
            if label_ci.is_typed:
                continue
            if not (label_ci.is_label_like or label_ci.is_header_like):
                continue

            key = label_ci.text.rstrip(":").strip()
            if not key:
                continue
            if not cls._looks_like_kv_label(label_ci.text):
                continue

            # Find a value cell to the right first, then below
            best_value: Optional[Tuple[_CellInfo, float, str]] = None
            for dc in range(1, radius + 1):
                vci = cell_map.get((r, c + dc))
                if not vci or vci.is_empty:
                    continue
                # Avoid pairing label-to-label when the candidate clearly looks like a label
                if not vci.is_typed and cls._looks_like_explicit_kv_label(vci.text):
                    continue
                score = 1.0 / dc
                reason = f"Value found to the right of label (distance={dc})"
                best_value = (vci, score, reason)
                break

            if best_value is None:
                for dr in range(1, radius + 1):
                    vci = cell_map.get((r + dr, c))
                    if not vci or vci.is_empty:
                        continue
                    # Below-label pairing is riskier; keep it strict unless the label is explicit (':')
                    if dr > 1 and not label_ci.text.strip().endswith(":"):
                        continue
                    if not vci.is_typed and cls._looks_like_explicit_kv_label(vci.text):
                        continue
                    score = 0.9 / dr
                    reason = f"Value found below label (distance={dr})"
                    best_value = (vci, score, reason)
                    break

            if best_value is None:
                continue

            vci, score, reason = best_value
            inferred_type = vci.inferred_type
            base = 0.55 if inferred_type == DataType.STRING.value else 0.70
            items.append(
                KeyValueItem(
                    key=key,
                    value=vci.raw,
                    value_type=inferred_type,
                    confidence=min(1.0, base + score * 0.35),
                    key_cell=CellAddress(row=r, col=c),
                    value_cell=CellAddress(row=vci.row, col=vci.col),
                    reason=reason,
                )
            )

        # Deduplicate by key: keep the highest confidence
        best_by_key: Dict[str, KeyValueItem] = {}
        for item in items:
            prev = best_by_key.get(item.key)
            if prev is None or item.confidence > prev.confidence:
                best_by_key[item.key] = item
        return list(best_by_key.values())

    @classmethod
    def _looks_like_kv_label(cls, text: str) -> bool:
        t = text.strip()
        if not t:
            return False
        # Explicit "Label:" pattern
        if t.endswith(":") or (":" in t and len(t) <= 40):
            return True
        # Domain-neutral: allow short, non-numeric tokens as labels (e.g., "Total", "날짜")
        # but avoid over-accepting value-like strings.
        if len(t) <= 12 and not any(ch.isdigit() for ch in t) and not cls._looks_like_data_value_text(t):
            return True
        return False

    @classmethod
    def _looks_like_explicit_kv_label(cls, text: str) -> bool:
        """
        Strict label detector used to avoid pairing label-to-label in KV extraction.

        Keep this intentionally conservative (punctuation-based) so ordinary short strings
        like names ("홍길동") aren't mistaken as labels.
        """
        t = text.strip()
        return bool(t) and (t.endswith(":") or (":" in t and len(t) <= 40))

    @classmethod
    def _looks_like_data_value_text(cls, text: str) -> bool:
        """
        Heuristic: some strings are much more likely to be data values than headers/labels.
        This helps avoid over-selecting multi-header rows in text-only tables.
        """
        t = text.strip()
        if not t:
            return False
        if any(ch in t for ch in ("(", ")", "[", "]")) and not t.endswith(":"):
            return True
        if re.search(r"\d", t) and len(t) > 4:
            return True
        if len(t) > 30:
            return True
        return False

    @classmethod
    def _find_nearest_label(
        cls, cell_map: Dict[Tuple[int, int], _CellInfo], row: int, col: int, *, radius: int
    ) -> Optional[Tuple[_CellInfo, float, str]]:
        best: Optional[Tuple[_CellInfo, float, str]] = None

        # Priority 1: left (same row)
        for dc in range(1, radius + 1):
            ci = cell_map.get((row, col - dc))
            if not ci or ci.is_empty:
                continue
            if ci.is_typed:
                continue
            if not (ci.is_label_like or ci.is_header_like):
                continue
            score = 1.0 / dc
            reason = f"Label found to the left (distance={dc})"
            best = (ci, score, reason)
            break

        # Priority 2: above (same col)
        for dr in range(1, radius + 1):
            ci = cell_map.get((row - dr, col))
            if not ci or ci.is_empty:
                continue
            if ci.is_typed:
                continue
            if not (ci.is_label_like or ci.is_header_like):
                continue
            score = 0.9 / dr
            reason = f"Label found above (distance={dr})"
            if best is None or score > best[1]:
                best = (ci, score, reason)
            break

        return best

    @classmethod
    def _extract_property_table_kv(
        cls,
        sub: List[List[Any]],
        *,
        bbox: BoundingBox,
        cell_map: Dict[Tuple[int, int], _CellInfo],
    ) -> List[KeyValueItem]:
        items: List[KeyValueItem] = []
        height = len(sub)
        for r in range(height):
            key = "" if len(sub[r]) < 1 or sub[r][0] is None else str(sub[r][0]).strip()
            if not key:
                continue
            value = sub[r][1] if len(sub[r]) >= 2 else None
            value_text = "" if value is None else str(value).strip()
            if not value_text:
                continue

            value_ci = cell_map.get((bbox.top + r, bbox.left + 1))
            inferred_type = value_ci.inferred_type if value_ci else DataType.STRING.value
            confidence = 0.9 if value_ci and value_ci.is_typed else 0.7
            items.append(
                KeyValueItem(
                    key=key.rstrip(":").strip(),
                    value=value,
                    value_type=inferred_type,
                    confidence=confidence,
                    key_cell=CellAddress(row=bbox.top + r, col=bbox.left + 0),
                    value_cell=CellAddress(row=bbox.top + r, col=bbox.left + 1),
                    reason="Property table row",
                )
            )
        return items

    # ---------------------------
    # Normalization helpers
    # ---------------------------

    @classmethod
    def _pivot_transposed(
        cls, sub: List[List[Any]], *, header_cols: int
    ) -> Tuple[List[str], List[List[Any]], List[int]]:
        height = len(sub)
        width = max((len(r) for r in sub), default=0)
        header_cols = max(1, header_cols)

        # Detect optional record-name row (top row, after header cols)
        record_names = [
            str(v).strip()
            for v in (sub[0][header_cols:] if sub else [])
            if v is not None and str(v).strip()
        ]
        looks_like_record_names = bool(record_names) and len(set(record_names)) == len(record_names)
        top_left = "" if not sub or not sub[0] or sub[0][0] is None else str(sub[0][0]).strip()

        start_row = 1 if looks_like_record_names and top_left == "" else 0
        field_rows = sub[start_row:]
        field_row_offsets = list(range(start_row, start_row + len(field_rows)))

        headers = []
        for r in range(len(field_rows)):
            key_val = field_rows[r][0] if field_rows[r] else None
            key = "" if cls._is_blank(key_val) else str(key_val).strip()
            headers.append(key or f"field_{r+1}")
        headers = cls._dedupe_headers(headers)

        rows: List[List[Any]] = []
        for c in range(header_cols, width):
            record = []
            for r in range(len(field_rows)):
                row = field_rows[r]
                record.append(row[c] if c < len(row) else "")
            rows.append(record)
        return headers, rows, field_row_offsets

    @classmethod
    def _build_table_column_provenance(
        cls, headers: List[str], *, bbox: BoundingBox, header_rows: int
    ) -> List[ColumnProvenance]:
        prov: List[ColumnProvenance] = []
        data_top = bbox.top + max(0, header_rows)
        for i, h in enumerate(headers):
            header_cells = [
                CellAddress(row=bbox.top + r, col=bbox.left + i) for r in range(header_rows)
            ]
            prov.append(
                ColumnProvenance(
                    field=h,
                    column_index=i,
                    header_cells=header_cells,
                    data_bbox=BoundingBox(
                        top=data_top,
                        left=bbox.left + i,
                        bottom=bbox.bottom,
                        right=bbox.left + i,
                    ),
                )
            )
        return prov

    @classmethod
    def _build_transposed_column_provenance(
        cls,
        headers: List[str],
        *,
        bbox: BoundingBox,
        header_cols: int,
        field_row_offsets: List[int],
    ) -> List[ColumnProvenance]:
        prov: List[ColumnProvenance] = []
        data_left = bbox.left + max(1, header_cols)
        for i, h in enumerate(headers):
            row_offset = field_row_offsets[i] if i < len(field_row_offsets) else i
            header_cell = CellAddress(row=bbox.top + row_offset, col=bbox.left)
            prov.append(
                ColumnProvenance(
                    field=h,
                    column_index=i,
                    header_cells=[header_cell],
                    data_bbox=BoundingBox(
                        top=bbox.top + row_offset,
                        left=data_left,
                        bottom=bbox.top + row_offset,
                        right=bbox.right,
                    ),
                )
            )
        return prov

    @classmethod
    def _extract_table(
        cls, sub: List[List[Any]], *, header_rows: int
    ) -> Tuple[List[str], List[List[Any]], List[List[str]]]:
        header_rows = max(1, header_rows)
        if not sub:
            return [], [], []

        width = max((len(r) for r in sub), default=0)
        header_parts: List[List[str]] = []
        for r in range(min(header_rows, len(sub))):
            row = sub[r]
            parts = []
            for c in range(width):
                parts.append(str(row[c]).strip() if c < len(row) and row[c] is not None else "")
            header_parts.append(parts)

        headers: List[str] = []
        for c in range(width):
            pieces = [hp[c] for hp in header_parts if hp[c]]
            headers.append(" / ".join(pieces) if pieces else f"col_{c+1}")
        headers = cls._dedupe_headers(headers)

        data_rows = sub[header_rows:]
        rows: List[List[Any]] = []
        for row in data_rows:
            normalized = []
            for c in range(width):
                normalized.append(row[c] if c < len(row) else "")
            # Skip fully empty rows
            if all(cls._is_blank(v) for v in normalized):
                continue
            rows.append(normalized)

        return headers, rows, header_parts

    @classmethod
    def _infer_schema(
        cls, headers: List[str], rows: List[List[Any]], *, include_complex_types: bool
    ) -> List[Any]:
        if not headers or not rows:
            return []
        # Build columns
        cols: List[List[Any]] = [[] for _ in headers]
        for row in rows:
            for i, h in enumerate(headers):
                if i < len(row):
                    cols[i].append(row[i])
        return [
            FunnelTypeInferenceService.infer_column_type(
                col, column_name=headers[i], include_complex_types=include_complex_types
            )
            for i, col in enumerate(cols)
        ]

    @classmethod
    def _dedupe_headers(cls, headers: List[str]) -> List[str]:
        seen: Dict[str, int] = {}
        out: List[str] = []
        for h in headers:
            base = h.strip() or "column"
            key = base.lower()
            n = seen.get(key, 0) + 1
            seen[key] = n
            out.append(base if n == 1 else f"{base}_{n}")
        return out

    # ---------------------------
    # Cell scoring & utilities
    # ---------------------------

    @classmethod
    def _normalize_grid(cls, grid: List[List[Any]]) -> List[List[Any]]:
        max_cols = max((len(r) for r in grid), default=0)
        return [list(r) + [""] * (max_cols - len(r)) for r in grid]

    @classmethod
    def _score_cells(
        cls, grid: List[List[Any]], include_complex_types: bool
    ) -> Tuple[Dict[Tuple[int, int], _CellInfo], Dict[int, Dict[str, int]]]:
        cell_map: Dict[Tuple[int, int], _CellInfo] = {}
        row_stats: Dict[int, Dict[str, int]] = {}

        for r, row in enumerate(grid):
            non_empty = 0
            typed = 0
            for c, v in enumerate(row):
                text = "" if v is None else str(v).strip()
                if text == "":
                    continue
                non_empty += 1

                inferred_type = cls._infer_single_value_type(text, include_complex_types)
                is_typed = inferred_type != DataType.STRING.value
                if is_typed:
                    typed += 1

                is_label_like = cls._is_label_like_text(text)
                is_header_like = cls._is_header_like_text(text)
                score = cls._cell_score(
                    text,
                    inferred_type=inferred_type,
                    row=r,
                    non_empty_in_row=None,  # filled later for penalties
                )

                cell_map[(r, c)] = _CellInfo(
                    row=r,
                    col=c,
                    raw=v,
                    text=text,
                    inferred_type=inferred_type,
                    is_empty=False,
                    score=score,
                    is_typed=is_typed,
                    is_label_like=is_label_like,
                    is_header_like=is_header_like,
                )
            row_stats[r] = {"non_empty": non_empty, "typed": typed}

        # Second pass: apply title-like penalties using row sparsity info
        for (r, c), ci in list(cell_map.items()):
            if ci.is_typed:
                continue
            rs = row_stats.get(r, {})
            if r <= 2 and rs.get("non_empty", 0) <= 2 and rs.get("typed", 0) == 0:
                # Domain-neutral: top sparse rows are more likely to be titles/ornaments.
                # Use only structural cues (position + sparsity + length).
                penalty = 0.25 if len(ci.text) <= 40 else 0.35
                new_score = max(0.0, ci.score - penalty)
                cell_map[(r, c)] = _CellInfo(**{**ci.__dict__, "score": new_score})

        return cell_map, row_stats

    @classmethod
    @lru_cache(maxsize=20000)
    def _infer_single_value_type(cls, text: str, include_complex_types: bool) -> str:
        res = FunnelTypeInferenceService.infer_column_type(
            [text], column_name=None, include_complex_types=include_complex_types
        ).inferred_type
        return res.type

    @classmethod
    def _cell_score(
        cls,
        text: str,
        *,
        inferred_type: str,
        row: int,
        non_empty_in_row: Optional[int],
    ) -> float:
        if inferred_type != DataType.STRING.value:
            return 1.0

        lower = text.lower()
        if len(text) >= 80:
            return 0.15

        if cls._is_label_like_text(text):
            return 0.75

        if cls._is_header_like_text(text):
            return 0.70

        # Text-only tables are common; treat short/medium strings as weak "data-like" signals
        if len(text) <= 2:
            return 0.35
        if len(text) <= 40:
            return 0.50
        return 0.30

    @classmethod
    def _is_label_like_text(cls, text: str) -> bool:
        t = text.strip()
        if not t:
            return False
        if ":" in t or t.endswith(":"):
            return True
        if re.fullmatch(r"[A-Za-z_ ]{1,40}", t):
            return True
        if re.fullmatch(r"[가-힣0-9_ ]{1,20}", t):
            # Short Korean labels like "납품처", "공급자"
            return True
        return False

    @classmethod
    def _is_header_like_text(cls, text: str) -> bool:
        t = text.strip()
        if not t or len(t) > 40:
            return False
        if any(ch.isdigit() for ch in t) and len(t) > 12:
            return False
        # Prefer short tokens / single line-ish headers
        tokens = re.split(r"\s+", t)
        return len(tokens) <= 6

    @staticmethod
    def _connected_components(points: Iterable[Tuple[int, int]]) -> List[List[Tuple[int, int]]]:
        pts = set(points)
        comps: List[List[Tuple[int, int]]] = []
        while pts:
            start = pts.pop()
            stack = [start]
            comp = [start]
            while stack:
                r, c = stack.pop()
                for dr in (-1, 0, 1):
                    for dc in (-1, 0, 1):
                        if dr == 0 and dc == 0:
                            continue
                        nb = (r + dr, c + dc)
                        if nb in pts:
                            pts.remove(nb)
                            stack.append(nb)
                            comp.append(nb)
            comps.append(comp)
        return comps

    @staticmethod
    def _bbox_for_points(points: Sequence[Tuple[int, int]]) -> BoundingBox:
        rows = [p[0] for p in points]
        cols = [p[1] for p in points]
        return BoundingBox(top=min(rows), left=min(cols), bottom=max(rows), right=max(cols))

    @staticmethod
    def _tighten_bbox(grid: List[List[Any]], bbox: BoundingBox) -> BoundingBox:
        top, bottom, left, right = bbox.top, bbox.bottom, bbox.left, bbox.right

        def row_empty(r: int) -> bool:
            row = grid[r]
            return all((row[c] is None or str(row[c]).strip() == "") for c in range(left, right + 1))

        def col_empty(c: int) -> bool:
            return all((grid[r][c] is None or str(grid[r][c]).strip() == "") for r in range(top, bottom + 1))

        while top <= bottom and row_empty(top):
            top += 1
        while bottom >= top and row_empty(bottom):
            bottom -= 1
        while left <= right and col_empty(left):
            left += 1
        while right >= left and col_empty(right):
            right -= 1

        return BoundingBox(top=top, left=left, bottom=bottom, right=right)

    @staticmethod
    def _bbox_area(bbox: BoundingBox) -> int:
        return (bbox.bottom - bbox.top + 1) * (bbox.right - bbox.left + 1)

    @classmethod
    def _count_non_empty_in_bbox(cls, grid: List[List[Any]], bbox: BoundingBox) -> int:
        count = 0
        for r in range(bbox.top, bbox.bottom + 1):
            row = grid[r]
            for c in range(bbox.left, bbox.right + 1):
                if c < len(row) and not cls._is_blank(row[c]):
                    count += 1
        return count

    @staticmethod
    def _slice_bbox(grid: List[List[Any]], bbox: BoundingBox) -> List[List[Any]]:
        return [row[bbox.left : bbox.right + 1] for row in grid[bbox.top : bbox.bottom + 1]]

    @staticmethod
    def _extract_columns_from_sub(rows: List[List[Any]]) -> List[List[Any]]:
        if not rows:
            return []
        width = max((len(r) for r in rows), default=0)
        cols: List[List[Any]] = [[] for _ in range(width)]
        for r in rows:
            for i in range(width):
                cols[i].append(r[i] if i < len(r) else "")
        return cols

    @staticmethod
    def _extract_rows_from_sub(rows: List[List[Any]], *, start_col: int) -> List[List[Any]]:
        out: List[List[Any]] = []
        for r in rows:
            out.append(r[start_col:] if start_col < len(r) else [])
        return out

    @staticmethod
    def _ensure_row_len(grid: List[List[Any]], row: int, length: int) -> None:
        if row < 0 or row >= len(grid):
            return
        if len(grid[row]) < length:
            grid[row].extend([""] * (length - len(grid[row])))

    @staticmethod
    def _get_cell(grid: List[List[Any]], row: int, col: int) -> Any:
        if row < 0 or row >= len(grid):
            return None
        r = grid[row]
        if col < 0 or col >= len(r):
            return None
        return r[col]
