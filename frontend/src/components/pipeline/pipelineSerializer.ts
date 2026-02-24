/**
 * pipelineSerializer.ts
 *
 * Translation layer between the flat config used by PipelineRightPanel UI
 * and the nested metadata structure expected by the backend pipeline executor.
 *
 * Backend reads:  node.get("metadata") → { operation: "filter", expression: "..." }
 * Frontend edits: config.filter_column, config.filter_op, config.filter_value
 *
 * This module bridges the two at save/load boundaries.
 */

type FlatConfig = Record<string, unknown>

/* ── Helpers ────────────────────────────────────────────── */

/** Comma-separated string → trimmed non-empty array */
function csvToArray(value: unknown): string[] {
  if (Array.isArray(value)) return value.map((v) => String(v).trim()).filter(Boolean)
  if (typeof value === 'string') return value.split(',').map((s) => s.trim()).filter(Boolean)
  return []
}

/** Array → comma-separated string */
function arrayToCsv(value: unknown): string {
  if (Array.isArray(value)) return value.join(', ')
  if (typeof value === 'string') return value
  return ''
}

/* ── Frontend transformType ↔ Backend operation mapping ── */

const OPERATION_MAP: Record<string, string> = {
  filter: 'filter',
  join: 'join',
  compute: 'compute',
  aggregate: 'aggregate',
  select: 'select',
  drop: 'drop',
  cast: 'cast',
  sort: 'sort',
  dedupe: 'dedupe',
  union: 'union',
  map: 'rename',
  normalize: 'normalize',
  split: 'split',
  flatten: 'explode',
  window: 'window',
  pivot: 'pivot',
  udf: 'udf',
  limit: 'limit',
  fill: 'fill',
  unpivot: 'unpivot',
  lookup: 'lookup',
}

const REVERSE_OPERATION_MAP: Record<string, string> = {
  rename: 'map',
  explode: 'flatten',
}

/* ── Filter expression synthesis / parsing ────────────── */

function synthesizeFilterExpression(
  column: string,
  op: string,
  value: string,
): string {
  if (!column) return ''
  switch (op) {
    case 'eq':
      return `${column} == '${value}'`
    case 'neq':
      return `${column} != '${value}'`
    case 'gt':
      return `${column} > ${value}`
    case 'gte':
      return `${column} >= ${value}`
    case 'lt':
      return `${column} < ${value}`
    case 'lte':
      return `${column} <= ${value}`
    case 'contains':
      return `${column} LIKE '%${value}%'`
    case 'not_null':
      return `${column} IS NOT NULL`
    case 'is_null':
      return `${column} IS NULL`
    default:
      return `${column} == '${value}'`
  }
}

function parseFilterExpression(expr: string): {
  column: string
  op: string
  value: string
} {
  const trimmed = expr.trim()
  if (!trimmed) return { column: '', op: 'eq', value: '' }

  // IS NOT NULL
  const notNullMatch = trimmed.match(/^(\w+)\s+IS\s+NOT\s+NULL$/i)
  if (notNullMatch) return { column: notNullMatch[1], op: 'not_null', value: '' }

  // IS NULL
  const nullMatch = trimmed.match(/^(\w+)\s+IS\s+NULL$/i)
  if (nullMatch) return { column: nullMatch[1], op: 'is_null', value: '' }

  // LIKE (contains)
  const likeMatch = trimmed.match(/^(\w+)\s+LIKE\s+'%(.*)%'$/i)
  if (likeMatch) return { column: likeMatch[1], op: 'contains', value: likeMatch[2] }

  // Comparison: col >= val, col == 'val', etc.
  const cmpMatch = trimmed.match(/^(\w+)\s*(==|!=|>=|<=|>|<)\s*'?([^']*)'?$/)
  if (cmpMatch) {
    const opMap: Record<string, string> = {
      '==': 'eq',
      '!=': 'neq',
      '>': 'gt',
      '>=': 'gte',
      '<': 'lt',
      '<=': 'lte',
    }
    return {
      column: cmpMatch[1],
      op: opMap[cmpMatch[2]] ?? 'eq',
      value: cmpMatch[3],
    }
  }

  // Can't parse — return raw expression so it's preserved
  return { column: '', op: 'eq', value: '' }
}

/* ── configToMetadata: flat UI config → backend metadata ─ */

export function configToMetadata(
  transformType: string,
  config: FlatConfig,
): Record<string, unknown> {
  const operation = OPERATION_MAP[transformType] ?? transformType

  switch (transformType) {
    case 'filter': {
      // If the user provided a raw expression (from backend round-trip), prefer it
      const col = String(config.filter_column ?? '')
      const op = String(config.filter_op ?? 'eq')
      const val = String(config.filter_value ?? '')
      let expression = ''
      if (col) {
        expression = synthesizeFilterExpression(col, op, val)
      }
      // Raw expression override (from backend-loaded or advanced edit)
      if (typeof config.expression === 'string' && config.expression) {
        expression = config.expression
      }
      return { operation, expression }
    }

    case 'join': {
      const joinType = String(config.join_type ?? config.joinType ?? 'inner')
      const joinKeys = config.join_keys as Array<{ left: string; right: string }> | undefined

      if (Array.isArray(joinKeys) && joinKeys.length > 1) {
        return {
          operation,
          joinType,
          leftKeys: joinKeys.map((k) => k.left).filter(Boolean),
          rightKeys: joinKeys.map((k) => k.right).filter(Boolean),
        }
      }

      const leftKey = joinKeys?.[0]?.left ?? String(config.left_key ?? config.leftKey ?? '')
      const rightKey = joinKeys?.[0]?.right ?? String(config.right_key ?? config.rightKey ?? '')
      return { operation, joinType, leftKey, rightKey }
    }

    case 'compute': {
      const computeOp = String(config.compute_op ?? 'custom')
      const src = String(config.source_column ?? '')
      const alias = String(config.output_alias ?? '')
      const expr = String(config.expression ?? '')

      if (computeOp === 'custom') {
        if (alias && expr) {
          return { operation, assignments: [{ column: alias, expression: expr }] }
        }
        return { operation, expression: expr }
      }
      // Built-in ops: synthesize expression
      const builtInMap: Record<string, string> = {
        multiply: `${src} * ${config.operand ?? 1}`,
        divide: `${src} / ${config.operand ?? 1}`,
        add: `${src} + ${config.operand ?? 0}`,
        subtract: `${src} - ${config.operand ?? 0}`,
        round: `round(${src})`,
        upper: `upper(${src})`,
        lower: `lower(${src})`,
        trim: `trim(${src})`,
      }
      if (src && alias) {
        return {
          operation,
          assignments: [{ column: alias, expression: builtInMap[computeOp] ?? src }],
        }
      }
      return { operation, expression: expr }
    }

    case 'aggregate': {
      const groupBy = csvToArray(config.group_by ?? config.groupBy)
      const aggList = config.aggregations as Array<{ agg_column: string; agg_fn: string }> | undefined
      let aggregates: Array<{ column: string; op: string; alias: string }>
      if (Array.isArray(aggList) && aggList.length > 0) {
        aggregates = aggList
          .filter((a) => a.agg_column)
          .map((a) => ({ column: a.agg_column, op: a.agg_fn, alias: `${a.agg_fn}_${a.agg_column}` }))
      } else {
        const aggCol = String(config.agg_column ?? '')
        const aggFn = String(config.agg_fn ?? 'sum')
        aggregates = aggCol ? [{ column: aggCol, op: aggFn, alias: `${aggFn}_${aggCol}` }] : []
      }
      return { operation, groupBy, aggregates }
    }

    case 'select':
      return { operation, columns: csvToArray(config.columns) }

    case 'drop':
      return { operation, columns: csvToArray(config.columns) }

    case 'cast': {
      const col = String(config.column ?? '')
      const targetType = String(config.target_type ?? 'string')
      return { operation, casts: col ? [{ column: col, type: targetType }] : [] }
    }

    case 'sort': {
      const col = String(config.column ?? '')
      const order = String(config.order ?? 'asc')
      const sortCol = order === 'desc' ? `-${col}` : col
      return { operation, columns: col ? [sortCol] : [] }
    }

    case 'dedupe':
      return { operation, columns: csvToArray(config.columns) }

    case 'union':
      return { operation, unionMode: String(config.union_mode ?? config.unionMode ?? 'strict') }

    case 'map':
      return { operation, rename: config.rename ?? {} }

    case 'normalize':
      return {
        operation,
        columns: csvToArray(config.columns),
        trim: config.trim !== false,
        empty_to_null: config.empty_to_null !== false,
        whitespace_to_null: config.whitespace_to_null !== false,
        lowercase: config.lowercase === true,
        uppercase: config.uppercase === true,
      }

    case 'split':
      return { operation, condition: String(config.condition ?? config.expression ?? '') }

    case 'flatten':
      return { operation, columns: config.column ? [String(config.column)] : [] }

    case 'window':
      return {
        operation,
        window: {
          partitionBy: csvToArray(config.partition_by),
          orderBy: csvToArray(config.order_by),
          outputColumn: String(config.output_column ?? 'row_number'),
        },
      }

    case 'pivot':
      return {
        operation,
        pivot: {
          index: csvToArray(config.index_columns),
          columns: String(config.pivot_column ?? ''),
          values: String(config.values_column ?? ''),
          agg: String(config.agg_op ?? 'sum'),
        },
      }

    case 'udf':
      return {
        operation,
        udfId: String(config.udf_id ?? config.udfId ?? ''),
        ...(config.udf_version != null && config.udf_version !== ''
          ? { udfVersion: Number(config.udf_version ?? config.udfVersion) }
          : {}),
      }

    // Backend not-yet-implemented — store in metadata for future
    case 'limit':
      return { operation, limit: Number(config.limit ?? 0) || undefined }

    case 'fill':
      return {
        operation,
        columns: csvToArray(config.columns),
        strategy: String(config.strategy ?? 'value'),
        fillValue: config.fill_value ?? config.fillValue ?? '',
      }

    case 'unpivot':
      return {
        operation,
        idColumns: csvToArray(config.id_columns),
        valueColumns: csvToArray(config.value_columns),
        varName: String(config.var_name ?? 'variable'),
        valueName: String(config.value_name ?? 'value'),
      }

    case 'lookup':
      return {
        operation,
        lookupDatasetId: config.lookup_dataset_id ?? config.lookupDatasetId ?? '',
        lookupDatasetName: config.lookup_dataset_name ?? config.lookupDatasetName ?? '',
        keyColumn: config.key_column ?? config.keyColumn ?? '',
        lookupKeyColumn: config.lookup_key_column ?? config.lookupKeyColumn ?? '',
        returnColumns: csvToArray(config.return_columns),
      }

    default:
      return operation ? { operation } : {}
  }
}

/* ── metadataToConfig: backend metadata → flat UI config ─ */

export function metadataToConfig(
  transformType: string,
  metadata: Record<string, unknown>,
): FlatConfig {
  switch (transformType) {
    case 'filter': {
      const expr = String(metadata.expression ?? '')
      const parsed = parseFilterExpression(expr)
      return {
        filter_column: parsed.column,
        filter_op: parsed.op,
        filter_value: parsed.value,
        expression: expr,
      }
    }

    case 'join': {
      const joinType = String(metadata.joinType ?? metadata.join_type ?? 'inner')
      const leftKeys = metadata.leftKeys as string[] | undefined
      const rightKeys = metadata.rightKeys as string[] | undefined

      if (Array.isArray(leftKeys) && leftKeys.length > 0) {
        const join_keys = leftKeys.map((lk, i) => ({
          left: lk,
          right: String((rightKeys ?? [])[i] ?? ''),
        }))
        return {
          join_type: joinType,
          join_keys,
          left_key: leftKeys[0] ?? '',
          right_key: String((rightKeys ?? [])[0] ?? ''),
        }
      }

      const leftKey = String(metadata.leftKey ?? metadata.left_key ?? '')
      const rightKey = String(metadata.rightKey ?? metadata.right_key ?? '')
      return {
        join_type: joinType,
        left_key: leftKey,
        right_key: rightKey,
        join_keys: leftKey ? [{ left: leftKey, right: rightKey }] : undefined,
      }
    }

    case 'compute': {
      const assignments = metadata.assignments as
        | Array<{ column?: string; expression?: string }>
        | undefined
      if (Array.isArray(assignments) && assignments.length > 0) {
        return {
          compute_op: 'custom',
          output_alias: String(assignments[0].column ?? ''),
          expression: String(assignments[0].expression ?? ''),
          source_column: '',
        }
      }
      return {
        compute_op: 'custom',
        expression: String(metadata.expression ?? ''),
        source_column: String(metadata.targetColumn ?? metadata.target_column ?? ''),
        output_alias: String(metadata.targetColumn ?? metadata.target_column ?? ''),
      }
    }

    case 'aggregate': {
      const groupBy = metadata.groupBy as string[] | undefined
      const aggregates = metadata.aggregates as
        | Array<{ column?: string; op?: string; alias?: string }>
        | undefined
      const aggregations = (aggregates ?? []).map((a) => ({
        agg_column: String(a.column ?? ''),
        agg_fn: String(a.op ?? 'sum'),
      }))
      return {
        group_by: arrayToCsv(groupBy),
        aggregations: aggregations.length > 0 ? aggregations : undefined,
        agg_column: String(aggregates?.[0]?.column ?? ''),
        agg_fn: String(aggregates?.[0]?.op ?? 'sum'),
      }
    }

    case 'select':
      return { columns: arrayToCsv(metadata.columns) }

    case 'drop':
      return { columns: arrayToCsv(metadata.columns) }

    case 'cast': {
      const casts = metadata.casts as Array<{ column?: string; type?: string }> | undefined
      return {
        column: String(casts?.[0]?.column ?? ''),
        target_type: String(casts?.[0]?.type ?? 'string'),
      }
    }

    case 'sort': {
      const columns = (metadata.columns as string[]) ?? []
      const first = String(columns[0] ?? '')
      const isDesc = first.startsWith('-')
      return {
        column: isDesc ? first.slice(1) : first,
        order: isDesc ? 'desc' : 'asc',
      }
    }

    case 'dedupe':
      return { columns: arrayToCsv(metadata.columns) }

    case 'union':
      return { union_mode: String(metadata.unionMode ?? metadata.union_mode ?? 'strict') }

    case 'map':
      return { rename: metadata.rename ?? {} }

    case 'normalize':
      return {
        columns: arrayToCsv(metadata.columns),
        trim: metadata.trim !== false,
        empty_to_null: (metadata.empty_to_null ?? metadata.emptyToNull) !== false,
        whitespace_to_null: (metadata.whitespace_to_null ?? metadata.whitespaceToNull) !== false,
        lowercase: metadata.lowercase === true,
        uppercase: metadata.uppercase === true,
      }

    case 'split':
      return { condition: String(metadata.condition ?? metadata.expression ?? '') }

    case 'flatten': {
      const cols = (metadata.columns as string[]) ?? []
      return { column: String(cols[0] ?? '') }
    }

    case 'window': {
      const w = (metadata.window ?? {}) as Record<string, unknown>
      return {
        partition_by: arrayToCsv(w.partitionBy),
        order_by: arrayToCsv(w.orderBy),
        output_column: String(w.outputColumn ?? w.output_column ?? 'row_number'),
        window_function: 'row_number',
      }
    }

    case 'pivot': {
      const p = (metadata.pivot ?? {}) as Record<string, unknown>
      return {
        index_columns: arrayToCsv(p.index),
        pivot_column: String(p.columns ?? ''),
        values_column: String(p.values ?? ''),
        agg_op: String(p.agg ?? 'sum'),
      }
    }

    case 'udf':
      return {
        udf_id: String(metadata.udfId ?? metadata.udf_id ?? ''),
        udf_version: metadata.udfVersion ?? metadata.udf_version ?? '',
      }

    case 'limit':
      return { limit: metadata.limit ?? '' }

    case 'fill':
      return {
        columns: arrayToCsv(metadata.columns),
        strategy: String(metadata.strategy ?? 'value'),
        fill_value: String(metadata.fillValue ?? metadata.fill_value ?? ''),
      }

    case 'unpivot':
      return {
        id_columns: arrayToCsv(metadata.idColumns ?? metadata.id_columns),
        value_columns: arrayToCsv(metadata.valueColumns ?? metadata.value_columns),
        var_name: String(metadata.varName ?? metadata.var_name ?? 'variable'),
        value_name: String(metadata.valueName ?? metadata.value_name ?? 'value'),
      }

    case 'lookup':
      return {
        lookup_dataset_id: String(metadata.lookupDatasetId ?? metadata.lookup_dataset_id ?? ''),
        lookup_dataset_name: String(metadata.lookupDatasetName ?? metadata.lookup_dataset_name ?? ''),
        key_column: String(metadata.keyColumn ?? metadata.key_column ?? ''),
        lookup_key_column: String(metadata.lookupKeyColumn ?? metadata.lookup_key_column ?? ''),
        return_columns: arrayToCsv(metadata.returnColumns ?? metadata.return_columns),
      }

    default:
      return {}
  }
}

/* ── nodeToBackend: ReactFlow node → backend node ──────── */

export function nodeToBackend(rfNode: {
  id: string
  position: { x: number; y: number }
  data: { transformType: string; config: Record<string, unknown>; label: string }
}): Record<string, unknown> {
  const { transformType, config } = rfNode.data

  // Determine backend node type
  let nodeType: 'input' | 'transform' | 'output'
  if (transformType === 'source') nodeType = 'input'
  else if (transformType === 'output') nodeType = 'output'
  else nodeType = 'transform'

  // Build metadata
  let metadata: Record<string, unknown>
  if (nodeType === 'input') {
    metadata = {
      datasetId: config.dataset_id ?? config.datasetId ?? '',
      datasetName: config.dataset_name ?? config.datasetName ?? '',
    }
  } else if (nodeType === 'output') {
    metadata = {
      outputName: config.output_name ?? config.outputName ?? config.name ?? rfNode.data.label ?? '',
    }
    // Preserve expectations if present
    if (config.expectations) {
      metadata.expectations = config.expectations
    }
  } else {
    metadata = configToMetadata(transformType, config)
  }

  // Preserve any existing metadata fields that we don't explicitly handle
  // (backward compat: backend-created pipelines may have extra fields like
  //  schemaChecks, read config, etc.)
  const existingMeta = config.metadata
  if (typeof existingMeta === 'object' && existingMeta !== null && !Array.isArray(existingMeta)) {
    metadata = { ...(existingMeta as Record<string, unknown>), ...metadata }
  }

  return {
    id: rfNode.id,
    type: nodeType,
    metadata,
    x: rfNode.position.x,
    y: rfNode.position.y,
    name: String(config.name ?? rfNode.data.label ?? ''),
  }
}

/* ── backendNodeToConfig: backend node → {transformType, config} ─ */

export function backendNodeToConfig(
  backendNode: Record<string, unknown>,
): { transformType: string; config: FlatConfig } {
  const rawType = String(backendNode.type ?? 'transform').toLowerCase()
  const metadata = (
    typeof backendNode.metadata === 'object' &&
    backendNode.metadata !== null &&
    !Array.isArray(backendNode.metadata)
      ? backendNode.metadata
      : {}
  ) as Record<string, unknown>
  const operation = String(metadata.operation ?? '')

  // Resolve frontend transformType
  let transformType: string
  if (rawType === 'input' || rawType === 'read_dataset') {
    transformType = 'source'
  } else if (rawType === 'output') {
    transformType = 'output'
  } else if (operation) {
    transformType = REVERSE_OPERATION_MAP[operation] ?? operation
  } else if (rawType !== 'transform') {
    // Legacy: node.type itself might be the operation (e.g., "filter")
    transformType = REVERSE_OPERATION_MAP[rawType] ?? rawType
  } else {
    transformType = 'filter' // fallback
  }

  // Convert metadata to flat config for UI panels
  let flatConfig: FlatConfig
  if (transformType === 'source') {
    flatConfig = {
      dataset_id: metadata.datasetId ?? metadata.dataset_id ?? backendNode.dataset_id ?? '',
      dataset_name: metadata.datasetName ?? metadata.dataset_name ?? backendNode.dataset_name ?? '',
    }
  } else if (transformType === 'output') {
    flatConfig = {
      output_name: metadata.outputName ?? metadata.output_name ?? backendNode.output_name ?? '',
    }
    if (metadata.expectations) {
      flatConfig.expectations = metadata.expectations
    }
  } else {
    flatConfig = metadataToConfig(transformType, metadata)
  }

  // Always store: name, type, and original metadata for round-trip preservation
  flatConfig.name = String(backendNode.name ?? metadata.name ?? flatConfig.name ?? '')
  flatConfig.type = backendNode.type
  flatConfig.metadata = metadata

  return { transformType, config: flatConfig }
}
