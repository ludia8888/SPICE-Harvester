import { useState } from 'react'
import { Icon, Button, FormGroup, InputGroup, HTMLSelect, Tag } from '@blueprintjs/core'
import { JsonViewer } from '../JsonViewer'
import type { Node } from 'reactflow'
import { getCatalog } from './nodes/nodeStyles'
import { ExpectationEditor } from './ExpectationEditor'
import type { Expectation } from './ExpectationEditor'

type ColumnSchema = { name: string; type?: string }

type LegendColorEntry = { id: string; label: string; color: string }

type Props = {
  isOpen: boolean
  node: Node | null
  onClose: () => void
  onUpdateConfig: (nodeId: string, config: Record<string, unknown>) => void
  onDeleteNode: (nodeId: string) => void
  datasets?: Array<{ dataset_id: string; name: string }>
  /** Output schema columns for the "Outputs" tab */
  outputSchema?: ColumnSchema[]
  /** Upstream source columns for column pickers */
  upstreamColumns?: ColumnSchema[]
  /** Legend colors for node color assignment */
  legendColors?: LegendColorEntry[]
  /** Callback to set a node's legend color */
  onSetNodeColor?: (nodeId: string, colorId: string | null) => void
}

/* ── Column input: HTMLSelect when columns available, InputGroup fallback ── */
const ColumnInput = ({
  value,
  onChange,
  columns,
  id,
  placeholder,
  small,
  fill = true,
}: {
  value: string
  onChange: (v: string) => void
  columns: ColumnSchema[]
  id?: string
  placeholder?: string
  small?: boolean
  fill?: boolean
}) => {
  if (columns.length > 0) {
    const isCustom = !!value && !columns.some((c) => c.name === value)
    return (
      <HTMLSelect
        id={id}
        value={value || ''}
        onChange={(e) => onChange(e.target.value)}
        fill={fill}
      >
        <option value="">{placeholder || '– select column –'}</option>
        {isCustom && (
          <option value={value}>{value} (custom)</option>
        )}
        {columns.map((c) => (
          <option key={c.name} value={c.name}>
            {c.name}{c.type ? ` (${c.type})` : ''}
          </option>
        ))}
      </HTMLSelect>
    )
  }
  return (
    <InputGroup
      id={id}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder || 'column name'}
      small={small}
      fill={fill}
    />
  )
}

/* ── Clickable column chips for multi-value (comma-separated) fields ── */
const ColumnChips = ({
  columns,
  currentValue,
  onAdd,
}: {
  columns: ColumnSchema[]
  currentValue: string
  onAdd: (colName: string) => void
}) => {
  if (columns.length === 0) return null
  const existing = new Set(
    currentValue
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean),
  )
  const available = columns.filter((c) => !existing.has(c.name))
  if (available.length === 0) return null
  return (
    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 3, marginTop: 4 }}>
      {available.slice(0, 20).map((c) => (
        <Tag
          key={c.name}
          minimal
          interactive
          onClick={() => onAdd(c.name)}
          style={{ fontSize: 10, cursor: 'pointer' }}
          title={`Click to add "${c.name}" (${c.type ?? 'unknown'})`}
        >
          + {c.name}
        </Tag>
      ))}
      {available.length > 20 && (
        <Tag minimal style={{ fontSize: 10, opacity: 0.5 }}>
          +{available.length - 20} more
        </Tag>
      )}
    </div>
  )
}

/* ── Multi-column input: InputGroup + column chips ────────────────────── */
const MultiColumnInput = ({
  value,
  onChange,
  columns,
  id,
  placeholder,
}: {
  value: string
  onChange: (v: string) => void
  columns: ColumnSchema[]
  id?: string
  placeholder?: string
}) => {
  const addColumn = (colName: string) => {
    const parts = value
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean)
    if (!parts.includes(colName)) {
      parts.push(colName)
    }
    onChange(parts.join(', '))
  }
  return (
    <>
      <InputGroup
        id={id}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder || 'e.g. col1, col2'}
      />
      <ColumnChips columns={columns} currentValue={value} onAdd={addColumn} />
    </>
  )
}

export const PipelineRightPanel = ({
  isOpen,
  node,
  onClose,
  onUpdateConfig,
  onDeleteNode,
  datasets,
  outputSchema,
  upstreamColumns,
  legendColors,
  onSetNodeColor,
}: Props) => {
  const [activeTab, setActiveTab] = useState<'configure' | 'outputs'>('configure')
  const cols = upstreamColumns ?? []

  if (!node) return null

  const data = node.data as { label: string; transformType: string; config: Record<string, unknown>; legendColorId?: string | null; legendColor?: string | null }
  const catalog = getCatalog(data.transformType)
  const config = data.config ?? {}

  const update = (key: string, value: unknown) => {
    onUpdateConfig(node.id, { ...config, [key]: value })
  }

  return (
    <div className={`pipeline-right-panel${isOpen ? ' is-open' : ''}`}>
      {/* Header with colored indicator */}
      <div className="pipeline-right-panel-header">
        <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          <div
            style={{
              width: 24,
              height: 24,
              borderRadius: 6,
              background: catalog.color,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              flexShrink: 0,
            }}
          >
            <Icon icon={catalog.icon} size={12} color="#fff" />
          </div>
          <div style={{ minWidth: 0 }}>
            <div style={{ fontWeight: 600, fontSize: 13, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
              {String(config.name ?? data.label ?? catalog.label)}
            </div>
            <Tag minimal round style={{ fontSize: 10, marginTop: 2 }}>
              {catalog.label}
            </Tag>
          </div>
        </div>
        <button className="pipeline-right-panel-close" onClick={onClose}>
          <Icon icon="cross" size={14} />
        </button>
      </div>

      {/* Tabs: Configure | Outputs */}
      <div className="pipeline-right-panel-tabs">
        <button
          className={`pipeline-right-panel-tab${activeTab === 'configure' ? ' is-active' : ''}`}
          onClick={() => setActiveTab('configure')}
        >
          Configure
        </button>
        <button
          className={`pipeline-right-panel-tab${activeTab === 'outputs' ? ' is-active' : ''}`}
          onClick={() => setActiveTab('outputs')}
        >
          Outputs{outputSchema && outputSchema.length > 0 ? ` (${outputSchema.length})` : ''}
        </button>
      </div>

      <div className="pipeline-right-panel-body">
        {activeTab === 'configure' && (
          <>
            {/* Common: Node name */}
            <FormGroup label="Name" labelFor="node-name">
              <InputGroup
                id="node-name"
                value={String(config.name ?? '')}
                placeholder={catalog.label}
                onChange={(e) => update('name', e.target.value)}
              />
            </FormGroup>

            {/* Legend color assignment */}
            {legendColors && legendColors.length > 0 && (
              <FormGroup label="Color" labelFor="node-color">
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span
                    style={{
                      width: 16,
                      height: 16,
                      borderRadius: 4,
                      background: data.legendColorId
                        ? legendColors.find((c) => c.id === data.legendColorId)?.color ?? catalog.color
                        : catalog.color,
                      flexShrink: 0,
                      border: '1px solid rgba(255,255,255,0.15)',
                    }}
                  />
                  <HTMLSelect
                    id="node-color"
                    value={data.legendColorId ?? ''}
                    onChange={(e) => onSetNodeColor?.(node.id, e.target.value || null)}
                    fill
                  >
                    <option value="">Default</option>
                    {legendColors.map((c) => (
                      <option key={c.id} value={c.id}>
                        {c.label}
                      </option>
                    ))}
                  </HTMLSelect>
                </div>
              </FormGroup>
            )}

            {/* Source config */}
            {data.transformType === 'source' && (
              <>
                <FormGroup label="Dataset" labelFor="dataset-id">
                  {datasets && datasets.length > 0 ? (
                    <HTMLSelect
                      id="dataset-id"
                      value={String(config.dataset_id ?? config.datasetId ?? '')}
                      onChange={(e) => {
                        const ds = datasets.find((d) => d.dataset_id === e.target.value)
                        update('dataset_id', e.target.value)
                        if (ds) update('dataset_name', ds.name)
                      }}
                      fill
                    >
                      <option value="">Select a dataset...</option>
                      {datasets.map((ds) => (
                        <option key={ds.dataset_id} value={ds.dataset_id}>{ds.name}</option>
                      ))}
                    </HTMLSelect>
                  ) : (
                    <InputGroup
                      id="dataset-id"
                      value={String(config.dataset_id ?? config.datasetId ?? '')}
                      placeholder="dataset_id or name"
                      onChange={(e) => update('dataset_id', e.target.value)}
                    />
                  )}
                </FormGroup>
              </>
            )}

            {/* Filter config */}
            {data.transformType === 'filter' && (
              <>
                <FormGroup label="Column" labelFor="filter-col">
                  <ColumnInput
                    id="filter-col"
                    value={String(config.filter_column ?? '')}
                    onChange={(v) => update('filter_column', v)}
                    columns={cols}
                    placeholder="e.g. status"
                  />
                </FormGroup>
                <FormGroup label="Operator" labelFor="filter-op">
                  <HTMLSelect
                    id="filter-op"
                    value={String(config.filter_op ?? 'eq')}
                    onChange={(e) => update('filter_op', e.target.value)}
                    fill
                    options={[
                      { value: 'eq', label: '= (equals)' },
                      { value: 'neq', label: '!= (not equals)' },
                      { value: 'gt', label: '> (greater than)' },
                      { value: 'gte', label: '>= (greater or equal)' },
                      { value: 'lt', label: '< (less than)' },
                      { value: 'lte', label: '<= (less or equal)' },
                      { value: 'contains', label: 'contains' },
                      { value: 'not_null', label: 'is not null' },
                      { value: 'is_null', label: 'is null' },
                    ]}
                  />
                </FormGroup>
                {!['not_null', 'is_null'].includes(String(config.filter_op)) && (
                  <FormGroup label="Value" labelFor="filter-val">
                    <InputGroup
                      id="filter-val"
                      value={String(config.filter_value ?? '')}
                      placeholder="e.g. active"
                      onChange={(e) => update('filter_value', e.target.value)}
                    />
                  </FormGroup>
                )}
              </>
            )}

            {/* Join config */}
            {data.transformType === 'join' && (() => {
              const joinKeys = (config.join_keys as Array<{ left: string; right: string }>) ??
                (config.left_key ? [{ left: String(config.left_key), right: String(config.right_key ?? '') }] : [{ left: '', right: '' }])
              const updateKeys = (newKeys: Array<{ left: string; right: string }>) => update('join_keys', newKeys)
              return (
                <>
                  <FormGroup label="Join Type" labelFor="join-type">
                    <HTMLSelect
                      id="join-type"
                      value={String(config.join_type ?? 'inner')}
                      onChange={(e) => update('join_type', e.target.value)}
                      fill
                      options={[
                        { value: 'inner', label: 'Inner Join' },
                        { value: 'left', label: 'Left Join' },
                        { value: 'right', label: 'Right Join' },
                        { value: 'outer', label: 'Full Outer Join' },
                        { value: 'cross', label: 'Cross Join' },
                      ]}
                    />
                  </FormGroup>
                  <FormGroup label="Join Keys">
                    {joinKeys.map((pair, i) => (
                      <div key={i} style={{ display: 'flex', gap: 4, marginBottom: 4, alignItems: 'center' }}>
                        <div style={{ flex: 1 }}>
                          <ColumnInput
                            id={`join-left-${i}`}
                            value={pair.left}
                            onChange={(v) => {
                              const list = [...joinKeys]
                              list[i] = { ...list[i], left: v }
                              updateKeys(list)
                            }}
                            columns={cols}
                            placeholder="Left key"
                            small
                          />
                        </div>
                        <span style={{ fontSize: 11, color: 'var(--foundry-text-muted)' }}>=</span>
                        <div style={{ flex: 1 }}>
                          <ColumnInput
                            id={`join-right-${i}`}
                            value={pair.right}
                            onChange={(v) => {
                              const list = [...joinKeys]
                              list[i] = { ...list[i], right: v }
                              updateKeys(list)
                            }}
                            columns={cols}
                            placeholder="Right key"
                            small
                          />
                        </div>
                        {i > 0 && (
                          <Button small minimal icon="cross" onClick={() => {
                            const list = [...joinKeys]
                            list.splice(i, 1)
                            updateKeys(list)
                          }} />
                        )}
                      </div>
                    ))}
                  </FormGroup>
                  <Button small icon="plus" onClick={() => updateKeys([...joinKeys, { left: '', right: '' }])}>
                    Add Key Pair
                  </Button>
                </>
              )
            })()}

            {/* Compute config */}
            {data.transformType === 'compute' && (
              <>
                <FormGroup label="Source Column" labelFor="compute-src">
                  <ColumnInput
                    id="compute-src"
                    value={String(config.source_column ?? '')}
                    onChange={(v) => update('source_column', v)}
                    columns={cols}
                    placeholder="e.g. price"
                  />
                </FormGroup>
                <FormGroup label="Operation" labelFor="compute-op">
                  <HTMLSelect
                    id="compute-op"
                    value={String(config.compute_op ?? 'custom')}
                    onChange={(e) => update('compute_op', e.target.value)}
                    fill
                    options={[
                      { value: 'multiply', label: 'Multiply' },
                      { value: 'divide', label: 'Divide' },
                      { value: 'add', label: 'Add' },
                      { value: 'subtract', label: 'Subtract' },
                      { value: 'round', label: 'Round' },
                      { value: 'upper', label: 'Uppercase' },
                      { value: 'lower', label: 'Lowercase' },
                      { value: 'trim', label: 'Trim' },
                      { value: 'custom', label: 'Custom Expression' },
                    ]}
                  />
                </FormGroup>
                {config.compute_op === 'custom' && (
                  <FormGroup label="Expression" labelFor="compute-expr">
                    <InputGroup
                      id="compute-expr"
                      value={String(config.expression ?? '')}
                      placeholder="e.g. col_a * col_b"
                      onChange={(e) => update('expression', e.target.value)}
                    />
                  </FormGroup>
                )}
                {config.compute_op && ['multiply', 'divide', 'add', 'subtract'].includes(String(config.compute_op)) && (
                  <FormGroup label="Operand" labelFor="compute-operand">
                    <InputGroup
                      id="compute-operand"
                      type="number"
                      value={String(config.operand ?? '')}
                      placeholder={['multiply', 'divide'].includes(String(config.compute_op)) ? 'e.g. 1.0' : 'e.g. 0'}
                      onChange={(e) => update('operand', e.target.value !== '' ? Number(e.target.value) : '')}
                    />
                  </FormGroup>
                )}
                <FormGroup label="Output Alias" labelFor="compute-alias">
                  <InputGroup
                    id="compute-alias"
                    value={String(config.output_alias ?? '')}
                    placeholder="e.g. total_price"
                    onChange={(e) => update('output_alias', e.target.value)}
                  />
                </FormGroup>
              </>
            )}

            {/* Aggregate config */}
            {data.transformType === 'aggregate' && (() => {
              const aggList = (config.aggregations as Array<{ agg_column: string; agg_fn: string }>) ??
                (config.agg_column ? [{ agg_column: String(config.agg_column), agg_fn: String(config.agg_fn ?? 'sum') }] : [{ agg_column: '', agg_fn: 'sum' }])
              const updateAggs = (newList: Array<{ agg_column: string; agg_fn: string }>) => update('aggregations', newList)
              const AGG_OPTIONS = [
                { value: 'sum', label: 'SUM' },
                { value: 'avg', label: 'AVG' },
                { value: 'count', label: 'COUNT' },
                { value: 'min', label: 'MIN' },
                { value: 'max', label: 'MAX' },
                { value: 'first', label: 'FIRST' },
                { value: 'last', label: 'LAST' },
              ]
              return (
                <>
                  <FormGroup label="Group By" labelFor="agg-groupby">
                    <MultiColumnInput
                      id="agg-groupby"
                      value={String(config.group_by ?? '')}
                      onChange={(v) => update('group_by', v)}
                      columns={cols}
                      placeholder="e.g. category, region"
                    />
                  </FormGroup>
                  <FormGroup label="Aggregations">
                    {aggList.map((agg, i) => (
                      <div key={i} style={{ display: 'flex', gap: 4, marginBottom: 4, alignItems: 'center' }}>
                        <div style={{ flex: 1 }}>
                          <ColumnInput
                            id={`agg-col-${i}`}
                            value={agg.agg_column}
                            onChange={(v) => {
                              const list = [...aggList]
                              list[i] = { ...list[i], agg_column: v }
                              updateAggs(list)
                            }}
                            columns={cols}
                            placeholder="column"
                            small
                          />
                        </div>
                        <HTMLSelect
                          value={agg.agg_fn}
                          onChange={(e) => {
                            const list = [...aggList]
                            list[i] = { ...list[i], agg_fn: e.target.value }
                            updateAggs(list)
                          }}
                          options={AGG_OPTIONS}
                        />
                        {aggList.length > 1 && (
                          <Button small minimal icon="cross" onClick={() => {
                            const list = [...aggList]
                            list.splice(i, 1)
                            updateAggs(list)
                          }} />
                        )}
                      </div>
                    ))}
                  </FormGroup>
                  <Button small icon="plus" onClick={() => updateAggs([...aggList, { agg_column: '', agg_fn: 'sum' }])}>
                    Add Aggregation
                  </Button>
                </>
              )
            })()}

            {/* Select config */}
            {data.transformType === 'select' && (
              <FormGroup label="Columns" labelFor="select-cols">
                <MultiColumnInput
                  id="select-cols"
                  value={String(config.columns ?? '')}
                  onChange={(v) => update('columns', v)}
                  columns={cols}
                  placeholder="e.g. id, name, email"
                />
              </FormGroup>
            )}

            {/* Cast config */}
            {data.transformType === 'cast' && (
              <>
                <FormGroup label="Column" labelFor="cast-col">
                  <ColumnInput
                    id="cast-col"
                    value={String(config.column ?? '')}
                    onChange={(v) => update('column', v)}
                    columns={cols}
                    placeholder="e.g. price"
                  />
                </FormGroup>
                <FormGroup label="Target Type" labelFor="cast-type">
                  <HTMLSelect
                    id="cast-type"
                    value={String(config.target_type ?? 'string')}
                    onChange={(e) => update('target_type', e.target.value)}
                    fill
                    options={['string', 'integer', 'float', 'double', 'boolean', 'date', 'timestamp']}
                  />
                </FormGroup>
              </>
            )}

            {/* Sort config */}
            {data.transformType === 'sort' && (
              <>
                <FormGroup label="Column" labelFor="sort-col">
                  <ColumnInput
                    id="sort-col"
                    value={String(config.column ?? '')}
                    onChange={(v) => update('column', v)}
                    columns={cols}
                    placeholder="e.g. created_at"
                  />
                </FormGroup>
                <FormGroup label="Order" labelFor="sort-order">
                  <HTMLSelect
                    id="sort-order"
                    value={String(config.order ?? 'asc')}
                    onChange={(e) => update('order', e.target.value)}
                    fill
                    options={[{ value: 'asc', label: 'Ascending' }, { value: 'desc', label: 'Descending' }]}
                  />
                </FormGroup>
              </>
            )}

            {/* Dedupe config */}
            {data.transformType === 'dedupe' && (
              <>
                <FormGroup label="Columns" labelFor="dedupe-cols">
                  <MultiColumnInput
                    id="dedupe-cols"
                    value={String(config.columns ?? '')}
                    onChange={(v) => update('columns', v)}
                    columns={cols}
                    placeholder="e.g. id, email"
                  />
                </FormGroup>
                <FormGroup label="Keep" labelFor="dedupe-keep">
                  <HTMLSelect
                    id="dedupe-keep"
                    value={String(config.keep ?? 'first')}
                    onChange={(e) => update('keep', e.target.value)}
                    fill
                    options={[{ value: 'first', label: 'First' }, { value: 'last', label: 'Last' }]}
                  />
                </FormGroup>
              </>
            )}

            {/* Drop config */}
            {data.transformType === 'drop' && (
              <FormGroup label="Columns to drop" labelFor="drop-cols">
                <MultiColumnInput
                  id="drop-cols"
                  value={String(config.columns ?? '')}
                  onChange={(v) => update('columns', v)}
                  columns={cols}
                  placeholder="e.g. temp_col, debug_flag"
                />
              </FormGroup>
            )}

            {/* Union config */}
            {data.transformType === 'union' && (
              <FormGroup label="Schema Mode" labelFor="union-mode">
                <HTMLSelect
                  id="union-mode"
                  value={String(config.union_mode ?? 'strict')}
                  onChange={(e) => update('union_mode', e.target.value)}
                  fill
                  options={[
                    { value: 'strict', label: 'Strict (schemas must match)' },
                    { value: 'common_only', label: 'Common Only (intersect columns)' },
                    { value: 'pad_missing_nulls', label: 'Pad Missing (null-fill)' },
                  ]}
                />
              </FormGroup>
            )}

            {/* Limit config */}
            {data.transformType === 'limit' && (
              <FormGroup label="Row Limit" labelFor="limit-count">
                <InputGroup
                  id="limit-count"
                  type="number"
                  value={String(config.limit ?? '')}
                  placeholder="e.g. 1000"
                  onChange={(e) => {
                    const v = e.target.value
                    update('limit', v ? parseInt(v) : '')
                  }}
                />
              </FormGroup>
            )}

            {/* Map / Rename config */}
            {data.transformType === 'map' && (
              <>
                <FormGroup label="Column Renames">
                  {Object.entries((config.rename as Record<string, string>) ?? {}).map(([oldName, newName]) => (
                    <div key={oldName || '__empty'} className="pipeline-rename-row">
                      <div style={{ flex: 1 }}>
                        <ColumnInput
                          id={`rename-old-${oldName}`}
                          value={oldName}
                          onChange={(v) => {
                            const rename = { ...((config.rename as Record<string, string>) ?? {}) }
                            const val = rename[oldName]
                            delete rename[oldName]
                            rename[v] = val
                            update('rename', rename)
                          }}
                          columns={cols}
                          placeholder="Old name"
                          small
                        />
                      </div>
                      <span className="pipeline-rename-arrow">→</span>
                      <InputGroup
                        small
                        value={newName}
                        placeholder="New name"
                        onChange={(e) => {
                          const rename = { ...((config.rename as Record<string, string>) ?? {}) }
                          rename[oldName] = e.target.value
                          update('rename', rename)
                        }}
                      />
                      <Button small minimal icon="cross" onClick={() => {
                        const rename = { ...((config.rename as Record<string, string>) ?? {}) }
                        delete rename[oldName]
                        update('rename', rename)
                      }} />
                    </div>
                  ))}
                </FormGroup>
                <Button small icon="plus" onClick={() => {
                  const rename = { ...((config.rename as Record<string, string>) ?? {}), '': '' }
                  update('rename', rename)
                }}>Add Rename</Button>
              </>
            )}

            {/* Normalize config */}
            {data.transformType === 'normalize' && (
              <>
                <FormGroup label="Columns" labelFor="norm-cols">
                  <MultiColumnInput
                    id="norm-cols"
                    value={String(config.columns ?? '')}
                    onChange={(v) => update('columns', v)}
                    columns={cols}
                    placeholder="e.g. name, email, address"
                  />
                </FormGroup>
                <FormGroup label="Operations">
                  <label className="pipeline-config-checkbox">
                    <input type="checkbox" checked={config.trim !== false} onChange={(e) => update('trim', e.target.checked)} />
                    Trim whitespace
                  </label>
                  <label className="pipeline-config-checkbox">
                    <input type="checkbox" checked={config.empty_to_null !== false} onChange={(e) => update('empty_to_null', e.target.checked)} />
                    Empty string → null
                  </label>
                  <label className="pipeline-config-checkbox">
                    <input type="checkbox" checked={config.whitespace_to_null !== false} onChange={(e) => update('whitespace_to_null', e.target.checked)} />
                    Whitespace-only → null
                  </label>
                  <label className="pipeline-config-checkbox">
                    <input type="checkbox" checked={config.lowercase === true} onChange={(e) => update('lowercase', e.target.checked)} />
                    Lowercase
                  </label>
                  <label className="pipeline-config-checkbox">
                    <input type="checkbox" checked={config.uppercase === true} onChange={(e) => update('uppercase', e.target.checked)} />
                    Uppercase
                  </label>
                </FormGroup>
              </>
            )}

            {/* Fill Null config */}
            {data.transformType === 'fill' && (
              <>
                <FormGroup label="Columns" labelFor="fill-cols">
                  <MultiColumnInput
                    id="fill-cols"
                    value={String(config.columns ?? '')}
                    onChange={(v) => update('columns', v)}
                    columns={cols}
                    placeholder="e.g. price, quantity"
                  />
                </FormGroup>
                <FormGroup label="Strategy" labelFor="fill-strategy">
                  <HTMLSelect
                    id="fill-strategy"
                    value={String(config.strategy ?? 'value')}
                    onChange={(e) => update('strategy', e.target.value)}
                    fill
                    options={[
                      { value: 'value', label: 'Fixed Value' },
                      { value: 'forward', label: 'Forward Fill' },
                      { value: 'backward', label: 'Backward Fill' },
                      { value: 'mean', label: 'Mean' },
                    ]}
                  />
                </FormGroup>
                {(!config.strategy || config.strategy === 'value') && (
                  <FormGroup label="Fill Value" labelFor="fill-val">
                    <InputGroup
                      id="fill-val"
                      value={String(config.fill_value ?? '')}
                      placeholder="e.g. 0 or N/A"
                      onChange={(e) => update('fill_value', e.target.value)}
                    />
                  </FormGroup>
                )}
              </>
            )}

            {/* Window Function config */}
            {data.transformType === 'window' && (
              <>
                <FormGroup label="Partition By" labelFor="win-partition">
                  <MultiColumnInput
                    id="win-partition"
                    value={String(config.partition_by ?? '')}
                    onChange={(v) => update('partition_by', v)}
                    columns={cols}
                    placeholder="e.g. department, region"
                  />
                </FormGroup>
                <FormGroup label="Order By" labelFor="win-order">
                  <MultiColumnInput
                    id="win-order"
                    value={String(config.order_by ?? '')}
                    onChange={(v) => update('order_by', v)}
                    columns={cols}
                    placeholder="e.g. created_at, -amount"
                  />
                </FormGroup>
                <FormGroup label="Function" labelFor="win-fn">
                  <HTMLSelect
                    id="win-fn"
                    value={String(config.window_function ?? 'row_number')}
                    onChange={(e) => update('window_function', e.target.value)}
                    fill
                    options={[
                      { value: 'row_number', label: 'row_number()' },
                      { value: 'rank', label: 'rank()' },
                      { value: 'dense_rank', label: 'dense_rank()' },
                      { value: 'custom', label: 'Custom Expression' },
                    ]}
                  />
                </FormGroup>
                <FormGroup label="Output Column" labelFor="win-output">
                  <InputGroup
                    id="win-output"
                    value={String(config.output_column ?? 'row_number')}
                    placeholder="row_number"
                    onChange={(e) => update('output_column', e.target.value)}
                  />
                </FormGroup>
                {config.window_function === 'custom' && (
                  <FormGroup label="Expression" labelFor="win-expr">
                    <InputGroup
                      id="win-expr"
                      value={String(config.expression ?? '')}
                      placeholder="e.g. sum(amount)"
                      onChange={(e) => update('expression', e.target.value)}
                    />
                  </FormGroup>
                )}
              </>
            )}

            {/* Pivot config */}
            {data.transformType === 'pivot' && (
              <>
                <FormGroup label="Index Columns" labelFor="pivot-index">
                  <MultiColumnInput
                    id="pivot-index"
                    value={String(config.index_columns ?? '')}
                    onChange={(v) => update('index_columns', v)}
                    columns={cols}
                    placeholder="e.g. region, year"
                  />
                </FormGroup>
                <FormGroup label="Pivot Column" labelFor="pivot-col">
                  <ColumnInput
                    id="pivot-col"
                    value={String(config.pivot_column ?? '')}
                    onChange={(v) => update('pivot_column', v)}
                    columns={cols}
                    placeholder="e.g. category"
                  />
                </FormGroup>
                <FormGroup label="Values Column" labelFor="pivot-val">
                  <ColumnInput
                    id="pivot-val"
                    value={String(config.values_column ?? '')}
                    onChange={(v) => update('values_column', v)}
                    columns={cols}
                    placeholder="e.g. amount"
                  />
                </FormGroup>
                <FormGroup label="Aggregation" labelFor="pivot-agg">
                  <HTMLSelect
                    id="pivot-agg"
                    value={String(config.agg_op ?? 'sum')}
                    onChange={(e) => update('agg_op', e.target.value)}
                    fill
                    options={[
                      { value: 'sum', label: 'SUM' },
                      { value: 'avg', label: 'AVG' },
                      { value: 'count', label: 'COUNT' },
                      { value: 'min', label: 'MIN' },
                      { value: 'max', label: 'MAX' },
                    ]}
                  />
                </FormGroup>
              </>
            )}

            {/* Unpivot config */}
            {data.transformType === 'unpivot' && (
              <>
                <FormGroup label="ID Columns (keep)" labelFor="unpivot-id">
                  <MultiColumnInput
                    id="unpivot-id"
                    value={String(config.id_columns ?? '')}
                    onChange={(v) => update('id_columns', v)}
                    columns={cols}
                    placeholder="e.g. region, year"
                  />
                </FormGroup>
                <FormGroup label="Value Columns (unpivot)" labelFor="unpivot-vals">
                  <MultiColumnInput
                    id="unpivot-vals"
                    value={String(config.value_columns ?? '')}
                    onChange={(v) => update('value_columns', v)}
                    columns={cols}
                    placeholder="e.g. q1, q2, q3, q4"
                  />
                </FormGroup>
                <FormGroup label="Variable Name" labelFor="unpivot-var">
                  <InputGroup
                    id="unpivot-var"
                    value={String(config.var_name ?? 'variable')}
                    placeholder="variable"
                    onChange={(e) => update('var_name', e.target.value)}
                  />
                </FormGroup>
                <FormGroup label="Value Name" labelFor="unpivot-value">
                  <InputGroup
                    id="unpivot-value"
                    value={String(config.value_name ?? 'value')}
                    placeholder="value"
                    onChange={(e) => update('value_name', e.target.value)}
                  />
                </FormGroup>
              </>
            )}

            {/* Split / Branch config */}
            {data.transformType === 'split' && (
              <>
                <FormGroup label="Condition Expression" labelFor="split-condition">
                  <InputGroup
                    id="split-condition"
                    value={String(config.condition ?? '')}
                    placeholder="e.g. amount > 100"
                    onChange={(e) => update('condition', e.target.value)}
                  />
                </FormGroup>
                <p style={{ fontSize: 11, color: 'var(--foundry-text-muted)', margin: '4px 0 0' }}>
                  Rows matching the condition pass through. Connect a second branch for the complement.
                </p>
              </>
            )}

            {/* Flatten / Explode config */}
            {data.transformType === 'flatten' && (
              <>
                <FormGroup label="Column to Flatten" labelFor="flatten-col">
                  <ColumnInput
                    id="flatten-col"
                    value={String(config.column ?? '')}
                    onChange={(v) => update('column', v)}
                    columns={cols}
                    placeholder="e.g. tags or nested_array"
                  />
                </FormGroup>
                <FormGroup label="Separator (for string split)" labelFor="flatten-sep">
                  <InputGroup
                    id="flatten-sep"
                    value={String(config.separator ?? '')}
                    placeholder="Leave empty for array explode"
                    onChange={(e) => update('separator', e.target.value)}
                  />
                </FormGroup>
              </>
            )}

            {/* UDF config */}
            {data.transformType === 'udf' && (
              <>
                <FormGroup label="UDF ID" labelFor="udf-id">
                  <InputGroup
                    id="udf-id"
                    value={String(config.udf_id ?? '')}
                    placeholder="e.g. my_custom_function"
                    onChange={(e) => update('udf_id', e.target.value)}
                  />
                </FormGroup>
                <FormGroup label="UDF Version (optional)" labelFor="udf-version">
                  <InputGroup
                    id="udf-version"
                    type="number"
                    value={String(config.udf_version ?? '')}
                    placeholder="latest"
                    onChange={(e) => update('udf_version', e.target.value ? parseInt(e.target.value) : '')}
                  />
                </FormGroup>
                <p style={{ fontSize: 11, color: 'var(--foundry-text-muted)', margin: '4px 0 0' }}>
                  Runs a sandboxed row-level Python function. Code is resolved at build time.
                </p>
              </>
            )}

            {/* Lookup config */}
            {data.transformType === 'lookup' && (
              <>
                <FormGroup label="Lookup Dataset" labelFor="lookup-dataset">
                  {datasets && datasets.length > 0 ? (
                    <HTMLSelect
                      id="lookup-dataset"
                      value={String(config.lookup_dataset_id ?? '')}
                      onChange={(e) => {
                        const ds = datasets.find((d) => d.dataset_id === e.target.value)
                        update('lookup_dataset_id', e.target.value)
                        if (ds) update('lookup_dataset_name', ds.name)
                      }}
                      fill
                    >
                      <option value="">Select a dataset...</option>
                      {datasets.map((ds) => (
                        <option key={ds.dataset_id} value={ds.dataset_id}>{ds.name}</option>
                      ))}
                    </HTMLSelect>
                  ) : (
                    <InputGroup
                      id="lookup-dataset"
                      value={String(config.lookup_dataset_id ?? '')}
                      placeholder="dataset_id"
                      onChange={(e) => update('lookup_dataset_id', e.target.value)}
                    />
                  )}
                </FormGroup>
                <FormGroup label="Key Column (source)" labelFor="lookup-key">
                  <ColumnInput
                    id="lookup-key"
                    value={String(config.key_column ?? '')}
                    onChange={(v) => update('key_column', v)}
                    columns={cols}
                    placeholder="e.g. customer_id"
                  />
                </FormGroup>
                <FormGroup label="Key Column (lookup)" labelFor="lookup-key-right">
                  <InputGroup
                    id="lookup-key-right"
                    value={String(config.lookup_key_column ?? '')}
                    placeholder="e.g. id"
                    onChange={(e) => update('lookup_key_column', e.target.value)}
                  />
                </FormGroup>
                <FormGroup label="Return Columns (comma-separated)" labelFor="lookup-return">
                  <InputGroup
                    id="lookup-return"
                    value={String(config.return_columns ?? '')}
                    placeholder="e.g. name, category, price"
                    onChange={(e) => update('return_columns', e.target.value)}
                  />
                </FormGroup>
              </>
            )}

            {/* Output config */}
            {data.transformType === 'output' && (
              <>
                <FormGroup label="Output Name" labelFor="output-name">
                  <InputGroup
                    id="output-name"
                    value={String(config.output_name ?? '')}
                    placeholder="e.g. cleaned_dataset"
                    onChange={(e) => update('output_name', e.target.value)}
                  />
                </FormGroup>

                {/* Data Expectations */}
                <ExpectationEditor
                  expectations={(config.expectations as Expectation[]) ?? []}
                  onChange={(exps) => update('expectations', exps)}
                />
              </>
            )}

            {/* Raw JSON fallback */}
            <details style={{ marginTop: 16 }}>
              <summary style={{ fontSize: 12, color: 'var(--foundry-text-muted)', cursor: 'pointer' }}>
                Advanced: Raw Config JSON
              </summary>
              <div style={{ marginTop: 8 }}>
                <JsonViewer value={config} />
              </div>
            </details>

            {/* Delete button */}
            <div style={{ marginTop: 24, borderTop: '1px solid var(--foundry-border)', paddingTop: 16 }}>
              <Button
                icon="trash"
                intent="danger"
                minimal
                small
                onClick={() => onDeleteNode(node.id)}
              >
                Delete Node
              </Button>
            </div>
          </>
        )}

        {activeTab === 'outputs' && (
          <div className="pipeline-right-panel-outputs">
            {outputSchema && outputSchema.length > 0 ? (
              <div className="pipeline-right-panel-schema">
                <div className="pipeline-right-panel-schema-header">
                  <span>Column</span>
                  <span>Type</span>
                </div>
                {outputSchema.map((col) => (
                  <div key={col.name} className="pipeline-right-panel-schema-row">
                    <span className="pipeline-right-panel-schema-name">{col.name}</span>
                    <Tag minimal style={{ fontSize: 10 }}>{col.type ?? 'unknown'}</Tag>
                  </div>
                ))}
              </div>
            ) : (
              <div className="pipeline-right-panel-outputs-empty">
                <Icon icon="th-list" size={24} style={{ opacity: 0.3 }} />
                <p>No schema available yet.</p>
                <p style={{ fontSize: 11, color: 'var(--foundry-text-muted)' }}>
                  Connect this node to a data source, or run preview to see output columns.
                </p>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
