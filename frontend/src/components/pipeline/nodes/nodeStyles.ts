import { Position } from 'reactflow'
import type { IconName } from '@blueprintjs/core'

/* ── Handle definition ────────────────────────────── */
export type HandleDef = {
  id: string
  label: string
  position: Position
}

/* ── Node catalog entry ───────────────────────────── */
export type NodeCatalogEntry = {
  label: string
  icon: IconName
  color: string
  category: 'input' | 'transform' | 'output'
  inputs: HandleDef[]
  outputs: HandleDef[]
}

/* ── Full catalog mapping transform type → visual config */
export const NODE_CATALOG: Record<string, NodeCatalogEntry> = {
  /* ── Input ─────────────────────────────────────── */
  source: {
    label: 'Source Dataset',
    icon: 'database',
    color: '#15B371',
    category: 'input',
    inputs: [],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },

  /* ── Transforms ────────────────────────────────── */
  filter: {
    label: 'Filter',
    icon: 'filter',
    color: '#2B95D6',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  select: {
    label: 'Select Columns',
    icon: 'column-layout',
    color: '#2B95D6',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  drop: {
    label: 'Drop Columns',
    icon: 'delete',
    color: '#2B95D6',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  cast: {
    label: 'Type Cast',
    icon: 'wrench',
    color: '#2B95D6',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  map: {
    label: 'Map / Rename',
    icon: 'edit',
    color: '#2B95D6',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  compute: {
    label: 'Compute / Derive',
    icon: 'function',
    color: '#7157D9',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  join: {
    label: 'Join',
    icon: 'join-table',
    color: '#D9822B',
    category: 'transform',
    inputs: [
      { id: 'left', label: 'Left dataset', position: Position.Left },
      { id: 'right', label: 'Right dataset', position: Position.Left },
    ],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  union: {
    label: 'Union',
    icon: 'merge-links',
    color: '#D9822B',
    category: 'transform',
    inputs: [
      { id: 'left', label: 'Left dataset', position: Position.Left },
      { id: 'right', label: 'Right dataset', position: Position.Left },
    ],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  aggregate: {
    label: 'Aggregate',
    icon: 'group-objects',
    color: '#D9822B',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  window: {
    label: 'Window Function',
    icon: 'timeline-bar-chart',
    color: '#D9822B',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  sort: {
    label: 'Sort / Order',
    icon: 'sort',
    color: '#2B95D6',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  limit: {
    label: 'Limit / Sample',
    icon: 'cut',
    color: '#2B95D6',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  dedupe: {
    label: 'Deduplicate',
    icon: 'duplicate',
    color: '#2B95D6',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  pivot: {
    label: 'Pivot',
    icon: 'pivot-table',
    color: '#7157D9',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  unpivot: {
    label: 'Unpivot / Melt',
    icon: 'ungroup-objects',
    color: '#7157D9',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  flatten: {
    label: 'Flatten JSON',
    icon: 'diagram-tree',
    color: '#7157D9',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  split: {
    label: 'Split Column',
    icon: 'split-columns',
    color: '#7157D9',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  fill: {
    label: 'Fill Null',
    icon: 'blank',
    color: '#2B95D6',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  normalize: {
    label: 'Normalize',
    icon: 'clean',
    color: '#2B95D6',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  udf: {
    label: 'Custom UDF',
    icon: 'code',
    color: '#F5498B',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },
  lookup: {
    label: 'Lookup',
    icon: 'search-template',
    color: '#D9822B',
    category: 'transform',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  },

  /* ── Output ────────────────────────────────────── */
  output: {
    label: 'Output',
    icon: 'export',
    color: '#0D8050',
    category: 'output',
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [],
  },
}

/* ── Helper: get catalog entry with fallback ──────── */
export const getCatalog = (type: string): NodeCatalogEntry =>
  NODE_CATALOG[type] ?? {
    label: type,
    icon: 'cog' as IconName,
    color: '#aaa',
    category: 'transform' as const,
    inputs: [{ id: 'in', label: 'Dataset', position: Position.Left }],
    outputs: [{ id: 'out', label: 'Dataset', position: Position.Right }],
  }

/* ── Map pipeline node type → ReactFlow node type ── */
export const resolveRFNodeType = (type: string): string => {
  const cat = getCatalog(type).category
  if (cat === 'input') return 'inputNode'
  if (cat === 'output') return 'outputNode'
  return 'transformNode'
}

/* ── Quick-access toolbar transforms (most common) ── */
export const TOOLBAR_TRANSFORMS = ['filter', 'join', 'aggregate', 'compute', 'select'] as const

/* ── All transform types for "More..." menu ───────── */
export const ALL_TRANSFORM_TYPES = Object.entries(NODE_CATALOG)
  .filter(([, v]) => v.category === 'transform')
  .map(([k, v]) => ({ type: k, ...v }))
