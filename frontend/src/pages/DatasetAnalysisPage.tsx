import { useState, useMemo, useCallback } from 'react'
import {
  Button,
  Card,
  Callout,
  Drawer,
  FormGroup,
  HTMLSelect,
  HTMLTable,
  InputGroup,
  Intent,
  NumericInput,
  Spinner,
  Tab,
  Tabs,
  Tag,
  Tooltip,
} from '@blueprintjs/core'
import { useQuery } from '@tanstack/react-query'
import {
  BarChart, Bar, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid,
  Tooltip as RTooltip, ResponsiveContainer, Legend,
} from 'recharts'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { useRequestContext } from '../api/useRequestContext'
import { useAppStore } from '../store/useAppStore'
import {
  readDatasetTableV2,
  listObjectTypesV2,
  searchObjectsV2,
  listPipelineDatasets,
} from '../api/bff'

/* ── types ──────────────────────────────────────────── */
type BoardType = 'filter' | 'groupby' | 'table' | 'chart'

type FilterCondition = {
  field: string
  op: 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte' | 'contains'
  value: string
}

type BoardConfig = {
  id: string
  type: BoardType
  label: string
  filter?: FilterCondition
  groupByField?: string
  aggregation?: 'count' | 'sum' | 'avg' | 'min' | 'max'
  aggregateField?: string
  chartType?: 'bar' | 'pie'
}

/* ── query keys ─────────────────────────────────────── */
const analysisKeys = {
  datasets: (db: string, branch: string) => ['analysis', 'datasets', db, branch] as const,
  types: (db: string, branch: string) => ['analysis', 'types', db, branch] as const,
}

/* ── page ────────────────────────────────────────────── */
export const DatasetAnalysisPage = ({ dbName }: { dbName: string }) => {
  const ctx = useRequestContext()
  const branch = useAppStore((s) => s.context.branch)

  /* state */
  const [selectedDataset, setSelectedDataset] = useState('')
  const [boards, setBoards] = useState<BoardConfig[]>([])
  const [dataLoaded, setDataLoaded] = useState(false)
  const [rawData, setRawData] = useState<{
    columns: Array<{ name?: string; type?: string } | string>
    rows: unknown[]
    totalRowCount: number
  } | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  /* datasets */
  const datasetsQ = useQuery({
    queryKey: analysisKeys.datasets(dbName, branch),
    queryFn: () => listPipelineDatasets(ctx, { db_name: dbName, branch }),
  })
  const datasets = useMemo(() => {
    const d = datasetsQ.data
    if (Array.isArray(d)) return d as Record<string, unknown>[]
    if (d && typeof d === 'object' && 'data' in d) {
      const inner = (d as { data?: unknown }).data
      if (Array.isArray(inner)) return inner as Record<string, unknown>[]
    }
    return []
  }, [datasetsQ.data])

  /* load data */
  const loadData = useCallback(async () => {
    if (!selectedDataset) return
    setLoading(true)
    setError(null)
    try {
      const result = await readDatasetTableV2(ctx, selectedDataset, { rowLimit: 500 }, { dbName })
      setRawData(result)
      setDataLoaded(true)
    } catch (err) {
      setError(String(err instanceof Error ? err.message : 'Failed to load data'))
    } finally {
      setLoading(false)
    }
  }, [ctx, selectedDataset, dbName])

  /* columns list */
  const columns = useMemo(() => rawData?.columns?.map((c) => typeof c === 'string' ? c : (c.name ?? '')) ?? [], [rawData])

  /* normalize rows to string[][] */
  const normalizedRows = useMemo((): string[][] => {
    if (!rawData?.rows) return []
    return rawData.rows.map((row) => {
      if (Array.isArray(row)) return row.map((cell) => String(cell ?? ''))
      return []
    })
  }, [rawData])

  /* apply boards pipeline */
  const processedData = useMemo(() => {
    if (!normalizedRows.length) return { rows: normalizedRows, columns }

    let currentRows = normalizedRows
    let currentColumns = columns
    const groupResults: { field: string; groups: Record<string, number> }[] = []

    for (const board of boards) {
      if (board.type === 'filter' && board.filter) {
        const colIdx = currentColumns.indexOf(board.filter.field)
        if (colIdx >= 0) {
          currentRows = currentRows.filter((row) => {
            const cell = row[colIdx] ?? ''
            const val = board.filter!.value
            switch (board.filter!.op) {
              case 'eq': return cell === val
              case 'neq': return cell !== val
              case 'gt': return Number(cell) > Number(val)
              case 'gte': return Number(cell) >= Number(val)
              case 'lt': return Number(cell) < Number(val)
              case 'lte': return Number(cell) <= Number(val)
              case 'contains': return cell.toLowerCase().includes(val.toLowerCase())
              default: return true
            }
          })
        }
      }
      if (board.type === 'groupby' && board.groupByField) {
        const colIdx = currentColumns.indexOf(board.groupByField)
        if (colIdx >= 0) {
          const groups: Record<string, number[]> = {}
          for (const row of currentRows) {
            const key = row[colIdx] ?? '(empty)'
            if (!groups[key]) groups[key] = []
            if (board.aggregateField) {
              const aggIdx = currentColumns.indexOf(board.aggregateField)
              if (aggIdx >= 0) groups[key].push(Number(row[aggIdx]) || 0)
            } else {
              groups[key].push(1)
            }
          }
          const agg = board.aggregation ?? 'count'
          const computed: Record<string, number> = {}
          for (const [key, values] of Object.entries(groups)) {
            switch (agg) {
              case 'count': computed[key] = values.length; break
              case 'sum': computed[key] = values.reduce((a, b) => a + b, 0); break
              case 'avg': computed[key] = values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0; break
              case 'min': computed[key] = Math.min(...values); break
              case 'max': computed[key] = Math.max(...values); break
            }
          }
          groupResults.push({ field: board.groupByField, groups: computed })
        }
      }
    }

    return { rows: currentRows, columns: currentColumns, groupResults }
  }, [normalizedRows, boards, columns])

  /* board CRUD */
  const addBoard = (type: BoardType) => {
    const id = `board_${Date.now()}`
    const labels: Record<BoardType, string> = {
      filter: 'Filter',
      groupby: 'Group By',
      table: 'Table',
      chart: 'Chart',
    }
    setBoards((prev) => [...prev, {
      id,
      type,
      label: labels[type],
      filter: type === 'filter' ? { field: columns[0] ?? '', op: 'eq', value: '' } : undefined,
      groupByField: type === 'groupby' ? columns[0] ?? '' : undefined,
      aggregation: type === 'groupby' ? 'count' : undefined,
      chartType: type === 'chart' ? 'bar' : undefined,
    }])
  }

  const updateBoard = (id: string, updates: Partial<BoardConfig>) => {
    setBoards((prev) => prev.map((b) => b.id === id ? { ...b, ...updates } : b))
  }

  const removeBoard = (id: string) => {
    setBoards((prev) => prev.filter((b) => b.id !== id))
  }

  return (
    <div>
      <PageHeader
        title="Data Analysis"
        subtitle={`Contour-style data exploration · ${dbName}`}
        actions={
          <div className="form-row">
            <Button icon="export" minimal disabled={!dataLoaded}>
              Export CSV
            </Button>
          </div>
        }
      />

      {/* Dataset Selector */}
      <Card style={{ marginBottom: 12 }}>
        <div className="form-row" style={{ alignItems: 'flex-end', gap: 12 }}>
          <FormGroup label={
            <Tooltip content="Choose a dataset to load and analyze" placement="top">
              <span className="tooltip-label">Dataset</span>
            </Tooltip>
          } style={{ flex: 2, marginBottom: 0 }}>
            <HTMLSelect
              value={selectedDataset}
              onChange={(e) => { setSelectedDataset(e.target.value); setDataLoaded(false) }}
              fill
              options={[
                { value: '', label: '-- Select a dataset --' },
                ...datasets.map((ds) => ({
                  value: String(ds.dataset_id ?? ds.rid ?? ''),
                  label: String(ds.name ?? ds.displayName ?? ds.dataset_id ?? 'Unknown'),
                })),
              ]}
            />
          </FormGroup>
          <Button
            intent={Intent.PRIMARY}
            icon="data-lineage"
            loading={loading}
            disabled={!selectedDataset}
            onClick={loadData}
          >
            Load Data
          </Button>
        </div>
      </Card>

      {error && (
        <Callout intent={Intent.DANGER} style={{ marginBottom: 12 }}>{error}</Callout>
      )}

      {loading && (
        <Card style={{ textAlign: 'center', padding: 40 }}>
          <Spinner size={30} />
          <p style={{ marginTop: 12 }}>Loading dataset...</p>
        </Card>
      )}

      {dataLoaded && rawData && (
        <>
          {/* Data Summary */}
          <Card style={{ marginBottom: 12 }}>
            <div className="form-row" style={{ gap: 12 }}>
              <Tag icon="th">{columns.length} columns</Tag>
              <Tag icon="th-list">{rawData.totalRowCount} rows loaded</Tag>
              <Tag icon="filter">{processedData.rows.length} rows after filters</Tag>
              <Tag icon="layers">{boards.length} boards</Tag>
            </div>
          </Card>

          {/* Board Stack */}
          {boards.map((board, idx) => (
            <Card key={board.id} style={{ marginBottom: 12 }}>
              <div className="form-row" style={{ justifyContent: 'space-between', marginBottom: 8 }}>
                <div className="form-row" style={{ gap: 8 }}>
                  <Tag intent={
                    board.type === 'filter' ? Intent.WARNING :
                    board.type === 'groupby' ? Intent.PRIMARY :
                    board.type === 'chart' ? Intent.SUCCESS :
                    Intent.NONE
                  }>
                    Board {idx + 1}: {board.label}
                  </Tag>
                </div>
                <Button
                  minimal
                  icon="cross"
                  onClick={() => removeBoard(board.id)}
                />
              </div>

              {board.type === 'filter' && (
                <FilterBoardEditor
                  board={board}
                  columns={columns}
                  onChange={(updates) => updateBoard(board.id, updates)}
                />
              )}

              {board.type === 'groupby' && (
                <GroupByBoardEditor
                  board={board}
                  columns={columns}
                  onChange={(updates) => updateBoard(board.id, updates)}
                  results={(processedData as { groupResults?: { field: string; groups: Record<string, number> }[] }).groupResults?.find(
                    (g) => g.field === board.groupByField
                  )}
                />
              )}

              {board.type === 'chart' && (
                <ChartBoardView
                  groupResults={(processedData as { groupResults?: { field: string; groups: Record<string, number> }[] }).groupResults ?? []}
                  chartType={board.chartType ?? 'bar'}
                />
              )}

              {board.type === 'table' && (
                <TableBoardView
                  columns={processedData.columns}
                  rows={processedData.rows}
                />
              )}
            </Card>
          ))}

          {/* Add Board Buttons */}
          <Card style={{ marginBottom: 12 }}>
            <div className="form-row" style={{ gap: 8 }}>
              <span style={{ fontSize: 13, fontWeight: 600, marginRight: 8 }}>+ Add Board:</span>
              <Tooltip content="Filter rows by a column value condition" placement="top">
                <Button icon="filter" onClick={() => addBoard('filter')}>Filter</Button>
              </Tooltip>
              <Tooltip content="Aggregate data by grouping on a column" placement="top">
                <Button icon="group-objects" onClick={() => addBoard('groupby')}>Group By</Button>
              </Tooltip>
              <Tooltip content="Visualize grouped data as a bar or pie chart" placement="top">
                <Button icon="chart" onClick={() => addBoard('chart')}>Chart</Button>
              </Tooltip>
              <Tooltip content="Display raw data in a sortable table" placement="top">
                <Button icon="th" onClick={() => addBoard('table')}>Table</Button>
              </Tooltip>
            </div>
          </Card>

          {/* Default data table if no table board */}
          {!boards.some((b) => b.type === 'table') && (
            <Card>
              <div className="card-title" style={{ marginBottom: 8 }}>Data Preview</div>
              <TableBoardView
                columns={processedData.columns}
                rows={processedData.rows.slice(0, 100)}
              />
              {processedData.rows.length > 100 && (
                <div style={{ padding: 8, fontSize: 12, opacity: 0.6 }}>
                  Showing 100 of {processedData.rows.length} rows. Add a Table board for full data.
                </div>
              )}
            </Card>
          )}
        </>
      )}

      {!dataLoaded && !loading && !error && (
        <Card>
          <Callout icon="info-sign">
            Select a dataset and click "Load Data" to begin analysis. Add Filter, Group By,
            Chart, and Table boards to build your analysis pipeline.
          </Callout>
        </Card>
      )}
    </div>
  )
}

/* ── Filter Board Editor ────────────────────────────── */
const FilterBoardEditor = ({
  board,
  columns,
  onChange,
}: {
  board: BoardConfig
  columns: string[]
  onChange: (updates: Partial<BoardConfig>) => void
}) => {
  const filter = board.filter ?? { field: '', op: 'eq' as const, value: '' }

  return (
    <div className="form-row" style={{ gap: 8, alignItems: 'flex-end' }}>
      <FormGroup label="Field" style={{ flex: 1, marginBottom: 0 }}>
        <HTMLSelect
          value={filter.field}
          onChange={(e) => onChange({ filter: { ...filter, field: e.target.value } })}
          fill
          options={columns.map((c) => ({ value: c, label: c }))}
        />
      </FormGroup>
      <FormGroup label="Operator" style={{ flex: 1, marginBottom: 0 }}>
        <HTMLSelect
          value={filter.op}
          onChange={(e) => onChange({ filter: { ...filter, op: e.target.value as FilterCondition['op'] } })}
          fill
          options={[
            { value: 'eq', label: '= equals' },
            { value: 'neq', label: '≠ not equals' },
            { value: 'gt', label: '> greater' },
            { value: 'gte', label: '≥ greater or equal' },
            { value: 'lt', label: '< less' },
            { value: 'lte', label: '≤ less or equal' },
            { value: 'contains', label: '∋ contains' },
          ]}
        />
      </FormGroup>
      <FormGroup label="Value" style={{ flex: 1, marginBottom: 0 }}>
        <InputGroup
          value={filter.value}
          onChange={(e) => onChange({ filter: { ...filter, value: e.target.value } })}
          placeholder="Filter value..."
        />
      </FormGroup>
    </div>
  )
}

/* ── Group By Board Editor ──────────────────────────── */
const GroupByBoardEditor = ({
  board,
  columns,
  onChange,
  results,
}: {
  board: BoardConfig
  columns: string[]
  onChange: (updates: Partial<BoardConfig>) => void
  results?: { field: string; groups: Record<string, number> }
}) => {
  const sortedGroups = useMemo(() => {
    if (!results) return []
    return Object.entries(results.groups)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 20)
  }, [results])

  const maxVal = sortedGroups.length ? Math.max(...sortedGroups.map(([, v]) => v)) : 1

  return (
    <div>
      <div className="form-row" style={{ gap: 8, alignItems: 'flex-end', marginBottom: 12 }}>
        <FormGroup label="Group By Field" style={{ flex: 1, marginBottom: 0 }}>
          <HTMLSelect
            value={board.groupByField ?? ''}
            onChange={(e) => onChange({ groupByField: e.target.value })}
            fill
            options={columns.map((c) => ({ value: c, label: c }))}
          />
        </FormGroup>
        <FormGroup label="Aggregation" style={{ flex: 1, marginBottom: 0 }}>
          <HTMLSelect
            value={board.aggregation ?? 'count'}
            onChange={(e) => onChange({ aggregation: e.target.value as BoardConfig['aggregation'] })}
            fill
            options={[
              { value: 'count', label: 'Count' },
              { value: 'sum', label: 'Sum' },
              { value: 'avg', label: 'Average' },
              { value: 'min', label: 'Min' },
              { value: 'max', label: 'Max' },
            ]}
          />
        </FormGroup>
        {board.aggregation !== 'count' && (
          <FormGroup label="Aggregate Field" style={{ flex: 1, marginBottom: 0 }}>
            <HTMLSelect
              value={board.aggregateField ?? ''}
              onChange={(e) => onChange({ aggregateField: e.target.value })}
              fill
              options={[
                { value: '', label: '-- Select field --' },
                ...columns.map((c) => ({ value: c, label: c })),
              ]}
            />
          </FormGroup>
        )}
      </div>

      {sortedGroups.length > 0 && (
        <div>
          <div style={{ fontSize: 12, fontWeight: 600, marginBottom: 8, opacity: 0.7 }}>
            {sortedGroups.length} groups · {board.aggregation ?? 'count'}({board.aggregateField || '*'})
          </div>
          {sortedGroups.map(([key, val]) => (
            <div key={key} style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
              <div style={{ minWidth: 120, fontSize: 12, textAlign: 'right', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {key}
              </div>
              <div style={{ flex: 1, background: '#e8ecf0', borderRadius: 3, height: 20, overflow: 'hidden' }}>
                <div
                  style={{
                    width: `${(val / maxVal) * 100}%`,
                    height: '100%',
                    background: '#2b95d6',
                    borderRadius: 3,
                    minWidth: 2,
                    transition: 'width 0.3s',
                  }}
                />
              </div>
              <div style={{ minWidth: 60, fontSize: 12, fontFamily: 'monospace' }}>
                {typeof val === 'number' && val % 1 !== 0 ? val.toFixed(2) : val}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

/* ── Chart Board View (Recharts) ───────────────────── */
const CHART_COLORS = ['#2b95d6', '#29a634', '#d9822b', '#db3737', '#8f398f', '#00b3a4', '#f5498b', '#ffc940', '#634dbf', '#738091']

const ChartBoardView = ({
  groupResults,
  chartType,
}: {
  groupResults: { field: string; groups: Record<string, number> }[]
  chartType: 'bar' | 'pie'
}) => {
  if (groupResults.length === 0) {
    return <Callout>Add a Group By board above to generate chart data.</Callout>
  }

  const lastGroup = groupResults[groupResults.length - 1]
  const chartData = Object.entries(lastGroup.groups)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15)
    .map(([name, value]) => ({ name, value }))

  if (chartType === 'pie') {
    return (
      <div>
        <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 12 }}>
          Distribution: {lastGroup.field}
        </div>
        <ResponsiveContainer width="100%" height={260}>
          <PieChart>
            <Pie
              data={chartData}
              cx="50%"
              cy="50%"
              outerRadius={90}
              dataKey="value"
              label={({ name, percent }) => `${name} (${((percent ?? 0) * 100).toFixed(0)}%)`}
              labelLine={false}
            >
              {chartData.map((_, i) => (
                <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
              ))}
            </Pie>
            <RTooltip />
            <Legend />
          </PieChart>
        </ResponsiveContainer>
      </div>
    )
  }

  /* bar chart (default) */
  return (
    <div>
      <div style={{ fontSize: 13, fontWeight: 600, marginBottom: 12 }}>
        Bar Chart: {lastGroup.field}
      </div>
      <ResponsiveContainer width="100%" height={240}>
        <BarChart data={chartData} margin={{ top: 5, right: 20, bottom: 40, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="name" angle={-35} textAnchor="end" fontSize={11} interval={0} />
          <YAxis fontSize={11} />
          <RTooltip />
          <Bar dataKey="value" radius={[4, 4, 0, 0]}>
            {chartData.map((_, i) => (
              <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

/* ── Table Board View ───────────────────────────────── */
const TableBoardView = ({
  columns,
  rows,
}: {
  columns: string[]
  rows: string[][]
}) => {
  const [sortCol, setSortCol] = useState<number | null>(null)
  const [sortAsc, setSortAsc] = useState(true)

  const sortedRows = useMemo(() => {
    if (sortCol === null) return rows
    return [...rows].sort((a, b) => {
      const va = a[sortCol] ?? ''
      const vb = b[sortCol] ?? ''
      const na = Number(va)
      const nb = Number(vb)
      if (!isNaN(na) && !isNaN(nb)) {
        return sortAsc ? na - nb : nb - na
      }
      return sortAsc ? va.localeCompare(vb) : vb.localeCompare(va)
    })
  }, [rows, sortCol, sortAsc])

  const handleSort = (idx: number) => {
    if (sortCol === idx) {
      setSortAsc(!sortAsc)
    } else {
      setSortCol(idx)
      setSortAsc(true)
    }
  }

  return (
    <div style={{ overflow: 'auto', maxHeight: 400 }}>
      <HTMLTable compact striped interactive style={{ width: '100%' }}>
        <thead>
          <tr>
            {columns.map((col, i) => (
              <th
                key={col}
                style={{ cursor: 'pointer', userSelect: 'none' }}
                onClick={() => handleSort(i)}
              >
                {col}
                {sortCol === i && (sortAsc ? ' ↑' : ' ↓')}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sortedRows.slice(0, 200).map((row, i) => (
            <tr key={i}>
              {columns.map((_, j) => (
                <td key={j} style={{ maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', fontSize: 12 }}>
                  {row[j] ?? ''}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </HTMLTable>
      {sortedRows.length > 200 && (
        <div style={{ padding: 8, fontSize: 12, opacity: 0.6, textAlign: 'center' }}>
          Showing 200 of {sortedRows.length} rows
        </div>
      )}
    </div>
  )
}
