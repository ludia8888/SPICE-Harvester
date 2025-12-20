import { useMemo, useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import {
  Button,
  Card,
  FormGroup,
  HTMLTable,
  HTMLSelect,
  InputGroup,
  Intent,
  Text,
} from '@blueprintjs/core'
import { queryBuilderInfo, runQuery } from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { useOntologyRegistry } from '../query/useOntologyRegistry'
import { useAppStore } from '../store/useAppStore'

type FilterRow = { field: string; operator: string; value: string }

const operatorMap: Record<string, string> = {
  '=': 'eq',
  '!=': 'ne',
  '>': 'gt',
  '>=': 'ge',
  '<': 'lt',
  '<=': 'le',
  LIKE: 'like',
  NOT_LIKE: 'like',
  STARTS_WITH: 'like',
  ENDS_WITH: 'like',
  CONTAINS: 'like',
  IN: 'in',
  NOT_IN: 'not_in',
  IS_NULL: 'is_null',
  IS_NOT_NULL: 'is_not_null',
}

const parseValue = (raw: string) => {
  if (!raw) return raw
  try {
    return JSON.parse(raw)
  } catch {
    return raw
  }
}

const buildLikeValue = (operator: string, value: string) => {
  if (operator === 'STARTS_WITH') return `${value}%`
  if (operator === 'ENDS_WITH') return `%${value}`
  if (operator === 'CONTAINS') return `%${value}%`
  return value
}

export const QueryBuilderPage = ({ dbName }: { dbName: string }) => {
  const requestContext = useRequestContext()
  const language = useAppStore((state) => state.context.language)
  const branch = useAppStore((state) => state.context.branch)
  const setInspector = useAppStore((state) => state.setInspector)
  const registry = useOntologyRegistry(dbName, branch)

  const [classId, setClassId] = useState('')
  const [filters, setFilters] = useState<FilterRow[]>([{ field: '', operator: '=', value: '' }])
  const [selectFields, setSelectFields] = useState('')
  const [orderBy, setOrderBy] = useState('')
  const [orderDirection, setOrderDirection] = useState<'asc' | 'desc'>('asc')
  const [limit, setLimit] = useState('20')

  const selectedClass = useMemo(
    () => registry.classes.find((item) => item.id === classId) ?? null,
    [classId, registry.classes],
  )

  const classOptions = useMemo(
    () => [{ label: 'Select class', value: '' }, ...registry.classes.map((item) => ({
      label: item.label ? `${item.label} (${item.id})` : item.id,
      value: item.id,
    }))],
    [registry.classes],
  )

  const propertyOptions = useMemo(() => {
    if (!selectedClass) {
      return []
    }
    return selectedClass.properties.map((prop) => ({
      label: prop.label ? `${prop.label} (${prop.id})` : prop.id,
      value: prop.label || prop.id,
    }))
  }, [selectedClass])

  const classLabel = selectedClass?.label ?? classId

  const builderQuery = useQuery({
    queryKey: qk.queryBuilder(dbName, requestContext.language),
    queryFn: () => queryBuilderInfo(requestContext, dbName),
  })

  const operatorOptions = useMemo(() => {
    const payload = builderQuery.data as { operators?: Record<string, string[]> } | undefined
    const operators = payload?.operators ?? {}
    return Object.values(operators).flat().filter((op) => op !== 'NOT_LIKE')
  }, [builderQuery.data])

  const runMutation = useMutation({
    mutationFn: () => {
      const preparedFilters = filters
        .filter((f) => f.field && f.operator)
        .map((filter) => {
          const mappedOperator = operatorMap[filter.operator] ?? filter.operator
          const rawValue = parseValue(filter.value)
          const value =
            mappedOperator === 'like' && typeof rawValue === 'string'
              ? buildLikeValue(filter.operator, rawValue)
              : rawValue
          return { field: filter.field, operator: mappedOperator, value }
        })
      return runQuery(requestContext, dbName, {
        class_label: classLabel,
        filters: preparedFilters,
        select: selectFields ? selectFields.split(',').map((s) => s.trim()).filter(Boolean) : undefined,
        order_by: orderBy || undefined,
        order_direction: orderDirection,
        limit: Number(limit) || undefined,
      })
    },
    onError: (error) => toastApiError(error, language),
  })

  const resultPayload = runMutation.data as
    | { results?: Array<Record<string, unknown>>; total?: number }
    | undefined
  const results = resultPayload?.results ?? []
  const columns = useMemo(() => {
    const keys = new Set<string>()
    results.forEach((row) => {
      Object.keys(row).forEach((key) => keys.add(key))
    })
    return Array.from(keys)
  }, [results])
  const formatCell = (value: unknown) => {
    if (value === null || value === undefined) return ''
    if (typeof value === 'string') return value
    if (typeof value === 'number' || typeof value === 'boolean') return String(value)
    try {
      return JSON.stringify(value)
    } catch {
      return String(value)
    }
  }
  const total = typeof resultPayload?.total === 'number' ? resultPayload.total : results.length

  return (
    <div>
      <PageHeader title="Query Builder" subtitle={`Label-based query (branch context: ${branch})`} />

      <div className="page-grid two-col">
        <Card className="card-stack">
          <FormGroup label="Class">
            <HTMLSelect value={classId} options={classOptions} onChange={(event) => setClassId(event.currentTarget.value)} />
          </FormGroup>
          {propertyOptions.length > 0 ? (
            <datalist id="query-field-options">
              {propertyOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </datalist>
          ) : null}
          {filters.map((filter, index) => (
            <div key={index} className="form-row">
              <InputGroup
                placeholder="Field label"
                list={propertyOptions.length > 0 ? 'query-field-options' : undefined}
                value={filter.field}
                onChange={(event) => {
                  const next = [...filters]
                  next[index] = { ...filter, field: event.currentTarget.value }
                  setFilters(next)
                }}
              />
              <HTMLSelect
                value={filter.operator}
                options={operatorOptions.length ? operatorOptions : ['=', '!=', '>', '>=', '<', '<=']}
                onChange={(event) => {
                  const next = [...filters]
                  next[index] = { ...filter, operator: event.currentTarget.value }
                  setFilters(next)
                }}
              />
              <InputGroup
                placeholder="Value"
                value={filter.value}
                onChange={(event) => {
                  const next = [...filters]
                  next[index] = { ...filter, value: event.currentTarget.value }
                  setFilters(next)
                }}
              />
            </div>
          ))}
          <div className="form-row">
            <Button icon="plus" onClick={() => setFilters([...filters, { field: '', operator: '=', value: '' }])}>
              Add filter
            </Button>
            <Button
              icon="cross"
              intent={Intent.WARNING}
              disabled={filters.length <= 1}
              onClick={() => setFilters(filters.slice(0, -1))}
            >
              Remove filter
            </Button>
          </div>
          <FormGroup label="Select fields (comma-separated)">
            <InputGroup value={selectFields} onChange={(event) => setSelectFields(event.currentTarget.value)} />
          </FormGroup>
          {propertyOptions.length > 0 ? (
            <div className="form-row">
              {propertyOptions.slice(0, 8).map((option) => (
                <Button
                  key={option.value}
                  small
                  minimal
                  icon="plus"
                  onClick={() => {
                    const current = selectFields
                      .split(',')
                      .map((item) => item.trim())
                      .filter(Boolean)
                    if (!current.includes(option.value)) {
                      const next = [...current, option.value].join(', ')
                      setSelectFields(next)
                    }
                  }}
                >
                  {option.label}
                </Button>
              ))}
            </div>
          ) : null}
          <FormGroup label="Order by">
            {propertyOptions.length > 0 ? (
              <HTMLSelect
                value={orderBy}
                options={[{ label: 'Select field', value: '' }, ...propertyOptions]}
                onChange={(event) => setOrderBy(event.currentTarget.value)}
              />
            ) : (
              <InputGroup value={orderBy} onChange={(event) => setOrderBy(event.currentTarget.value)} />
            )}
          </FormGroup>
          <FormGroup label="Order direction">
            <HTMLSelect
              value={orderDirection}
              options={[
                { label: 'asc', value: 'asc' },
                { label: 'desc', value: 'desc' },
              ]}
              onChange={(event) => setOrderDirection(event.currentTarget.value as 'asc' | 'desc')}
            />
          </FormGroup>
          <FormGroup label="Limit">
            <InputGroup value={limit} onChange={(event) => setLimit(event.currentTarget.value)} />
          </FormGroup>
          <Button intent={Intent.PRIMARY} onClick={() => runMutation.mutate()} disabled={!classLabel} loading={runMutation.isPending}>
            Run query
          </Button>
        </Card>

        <Card className="card-stack">
          <Text className="muted small">Operator catalog</Text>
          <JsonViewer value={builderQuery.data} empty="Query builder info will appear here." />
          <Text className="muted small">Results</Text>
          {results.length === 0 ? (
            <Text className="muted">No results yet.</Text>
          ) : (
            <>
              <Text className="muted small">Total: {total}</Text>
              <HTMLTable striped interactive className="command-table">
                <thead>
                  <tr>
                    {columns.map((col) => (
                      <th key={col}>{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {results.map((row, index) => (
                    <tr
                      key={index}
                      onClick={() =>
                        setInspector({
                          title: `Row ${index + 1}`,
                          kind: 'Query result',
                          data: row,
                        })
                      }
                    >
                      {columns.map((col) => (
                        <td key={`${index}-${col}`}>{formatCell(row[col])}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </HTMLTable>
            </>
          )}
          <JsonViewer value={runMutation.data} empty="Run a query to see results." />
        </Card>
      </div>
    </div>
  )
}
