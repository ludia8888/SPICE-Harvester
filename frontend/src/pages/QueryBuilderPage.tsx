import { useMemo, useState } from 'react'
import { useParams } from 'react-router-dom'
import { useMutation, useQuery } from '@tanstack/react-query'
import {
  Button,
  Card,
  FormGroup,
  H5,
  HTMLSelect,
  HTMLTable,
  InputGroup,
  Intent,
  NumericInput,
  TextArea,
} from '@blueprintjs/core'
import { queryBuilderInfo, runQuery, runRawQuery } from '../api/bff'
import { ApiErrorCallout } from '../components/ApiErrorCallout'
import { PageHeader } from '../components/PageHeader'
import { JsonView } from '../components/JsonView'
import { useOntologyRegistry } from '../hooks/useOntologyRegistry'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { useAppStore } from '../store/useAppStore'
import { formatLabel } from '../utils/labels'

type FilterRow = { field: string; operator: string; value: string }

const OPERATOR_LABELS: Record<string, string> = {
  eq: '=',
  ne: '!=',
  gt: '>',
  ge: '>=',
  lt: '<',
  le: '<=',
  like: 'like',
  in: 'in',
  not_in: 'not in',
  is_null: 'is null',
  is_not_null: 'is not null',
}

const mapOperator = (op: string): string | null => {
  const normalized = op.trim().toLowerCase()
  if (normalized === '=') return 'eq'
  if (normalized === '!=') return 'ne'
  if (normalized === '>') return 'gt'
  if (normalized === '>=') return 'ge'
  if (normalized === '<') return 'lt'
  if (normalized === '<=') return 'le'
  if (['like', 'contains', 'starts_with', 'ends_with'].includes(normalized)) return 'like'
  if (normalized === 'in') return 'in'
  if (normalized === 'not_in') return 'not_in'
  if (normalized === 'is_null') return 'is_null'
  if (normalized === 'is_not_null') return 'is_not_null'
  return null
}

export const QueryBuilderPage = () => {
  const { db } = useParams()
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)

  const registry = useOntologyRegistry(db, context.branch)

  const [classId, setClassId] = useState('')
  const [limit, setLimit] = useState(20)
  const [filters, setFilters] = useState<FilterRow[]>([])
  const [selectFields, setSelectFields] = useState<string[]>([])
  const [orderBy, setOrderBy] = useState('')
  const [orderDirection, setOrderDirection] = useState('asc')
  const [result, setResult] = useState<any>(null)
  const [lastError, setLastError] = useState<unknown>(null)
  const [rawQueryJson, setRawQueryJson] = useState('')
  const [rawResult, setRawResult] = useState<any>(null)
  const [rawError, setRawError] = useState<unknown>(null)

  const requestContext = useMemo(
    () => ({ language: context.language, authToken, adminToken }),
    [adminToken, authToken, context.language],
  )

  const builderQuery = useQuery({
    queryKey: db ? qk.queryBuilder({ dbName: db, language: context.language }) : ['bff', 'query-builder', 'empty'],
    queryFn: () => queryBuilderInfo(requestContext, db ?? ''),
    enabled: Boolean(db),
  })

  const runMutation = useMutation({
    mutationFn: () =>
      runQuery(requestContext, db ?? '', {
        ...(registry.classMap.get(classId)?.label
          ? { class_label: formatLabel(registry.classMap.get(classId)?.label, context.language, '') }
          : { class_id: classId }),
        filters: filters.map((filter) => ({
          field: filter.field,
          operator: filter.operator,
          value: filter.value,
        })),
        select: selectFields.length ? selectFields : undefined,
        limit,
        order_by: orderBy || undefined,
        order_direction: orderDirection,
      }),
    onSuccess: (payload) => {
      setLastError(null)
      setResult(payload)
    },
    onError: (error) => {
      setLastError(error)
      toastApiError(error, context.language)
    },
  })

  const rawMutation = useMutation({
    mutationFn: () => runRawQuery(requestContext, db ?? '', JSON.parse(rawQueryJson || '{}')),
    onSuccess: (payload) => {
      setRawError(null)
      setRawResult(payload)
    },
    onError: (error) => {
      setRawError(error)
      toastApiError(error, context.language)
    },
  })

  const propertyOptions = useMemo(() => {
    const item = registry.classMap.get(classId)
    const props = item?.properties ?? []
    return props.map((prop: any) => ({
      id: prop.name ?? '',
      label: String(prop.label?.ko ?? prop.label?.en ?? prop.name ?? ''),
    }))
  }, [classId, registry.classMap])

  const operators = useMemo<string[]>(() => {
    const data = builderQuery.data as any
    if (!data?.operators) {
      return ['eq', 'ne', 'gt', 'ge', 'lt', 'le', 'like', 'in', 'not_in', 'is_null', 'is_not_null']
    }
    const raw = (Object.values(data.operators).flat() as string[]).filter((value) => typeof value === 'string')
    const mapped = raw.map((value) => mapOperator(value)).filter((value): value is string => Boolean(value))
    return mapped.length ? Array.from(new Set(mapped)) : ['eq', 'ne', 'gt', 'ge', 'lt', 'le', 'like', 'in', 'not_in', 'is_null', 'is_not_null']
  }, [builderQuery.data])

  return (
    <div>
      <PageHeader title="Query Builder" subtitle="라벨 기반 테이블형 조회" />

      {lastError ? (
        <ApiErrorCallout
          error={lastError}
          language={context.language}
          mappingsUrl={db ? `/db/${encodeURIComponent(db)}/mappings` : undefined}
        />
      ) : null}

      <Card elevation={1} className="section-card">
        <div className="form-grid">
          <FormGroup label="Class label">
            <HTMLSelect
              value={classId}
              onChange={(event) => setClassId(event.currentTarget.value)}
              options={[{ label: 'Select class', value: '' }, ...registry.classOptions.map((item) => ({ label: item.label, value: item.value }))]}
            />
          </FormGroup>
          <FormGroup label="Limit">
            <NumericInput value={limit} min={1} max={500} onValueChange={(value) => setLimit(value)} />
          </FormGroup>
          <FormGroup label="Order by">
            <HTMLSelect
              value={orderBy}
              onChange={(event) => setOrderBy(event.currentTarget.value)}
              options={[{ label: 'None', value: '' }, ...propertyOptions.map((prop) => ({ label: prop.label, value: prop.label }))]}
            />
          </FormGroup>
          <FormGroup label="Direction">
            <HTMLSelect
              value={orderDirection}
              onChange={(event) => setOrderDirection(event.currentTarget.value)}
              options={['asc', 'desc']}
            />
          </FormGroup>
        </div>

        <div className="form-row">
          <Button minimal icon="add" onClick={() => setFilters((prev) => [...prev, { field: '', operator: 'eq', value: '' }])}>
            Add filter
          </Button>
        </div>
        {filters.map((filter, index) => (
          <div key={index} className="row-grid">
            <HTMLSelect
              value={filter.field}
              onChange={(event) => {
                const value = event.currentTarget.value
                setFilters((prev) => {
                  const next = [...prev]
                  next[index] = { ...next[index], field: value }
                  return next
                })
              }}
              options={[{ label: 'Field', value: '' }, ...propertyOptions.map((prop) => ({ label: prop.label, value: prop.label }))]}
            />
            <HTMLSelect
              value={filter.operator}
              onChange={(event) => {
                const value = event.currentTarget.value
                setFilters((prev) => {
                  const next = [...prev]
                  next[index] = { ...next[index], operator: value }
                  return next
                })
              }}
              options={operators.map((op) => ({ label: OPERATOR_LABELS[op] ?? op, value: op }))}
            />
            <InputGroup
              placeholder="value"
              value={filter.value}
              onChange={(event) => {
                const value = event.currentTarget.value
                setFilters((prev) => {
                  const next = [...prev]
                  next[index] = { ...next[index], value }
                  return next
                })
              }}
            />
            <Button
              minimal
              icon="trash"
              onClick={() => setFilters((prev) => prev.filter((_, i) => i !== index))}
            />
          </div>
        ))}

        <FormGroup label="Select fields">
          <select
            className="bp6-html-select"
            multiple
            value={selectFields}
            onChange={(event) => {
              const options = Array.from(event.currentTarget.selectedOptions)
              setSelectFields(options.map((opt) => opt.value))
            }}
          >
            {propertyOptions.map((prop) => (
              <option key={prop.id} value={prop.label}>
                {prop.label}
              </option>
            ))}
          </select>
        </FormGroup>

        <Button intent={Intent.PRIMARY} onClick={() => runMutation.mutate()} disabled={!classId}>
          Run Query
        </Button>
      </Card>

      <Card elevation={1} className="section-card">
        <H5>Results</H5>
        {Array.isArray(result?.results) ? (
          <HTMLTable striped interactive className="full-width">
            <thead>
              <tr>
                {Object.keys(result.results[0] ?? {}).map((key) => (
                  <th key={key}>{key}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.results.map((row: any, index: number) => (
                <tr key={index}>
                  {Object.values(row).map((cell: any, idx: number) => (
                    <td key={idx}>{String(cell)}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </HTMLTable>
        ) : (
          <JsonView value={result} />
        )}
      </Card>

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>Raw Query</H5>
          <Button intent={Intent.PRIMARY} onClick={() => rawMutation.mutate()} disabled={!rawQueryJson.trim()}>
            Run Raw Query
          </Button>
        </div>
        <TextArea
          rows={6}
          value={rawQueryJson}
          onChange={(event) => setRawQueryJson(event.currentTarget.value)}
          placeholder='{\"class_id\":\"...\",\"filters\":[]}'
        />
        {rawError ? (
          <ApiErrorCallout
            error={rawError}
            language={context.language}
            mappingsUrl={db ? `/db/${encodeURIComponent(db)}/mappings` : undefined}
          />
        ) : null}
        <JsonView value={rawResult} />
      </Card>
    </div>
  )
}
