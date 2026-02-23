import { useState, useMemo } from 'react'
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
  Spinner,
  Tab,
  Tabs,
  Tag,
  Tooltip,
} from '@blueprintjs/core'
import { useQuery, useMutation } from '@tanstack/react-query'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { useRequestContext } from '../api/useRequestContext'
import { useAppStore } from '../store/useAppStore'
import {
  listObjectTypesV2,
  searchObjectsV2,
  listInstancesCtx,
  getInstanceCtx,
} from '../api/bff'

/* ── query keys ─────────────────────────────────────── */
const exKeys = {
  types: (db: string, branch: string) => ['explorer', 'types', db, branch] as const,
  search: (db: string, type: string, branch: string) => ['explorer', 'search', db, type, branch] as const,
}

/* ── page ────────────────────────────────────────────── */
export const ObjectExplorerPage = ({ dbName }: { dbName: string }) => {
  const ctx = useRequestContext()
  const branch = useAppStore((s) => s.context.branch)

  const [selectedType, setSelectedType] = useState<string>('')
  const [searchTerm, setSearchTerm] = useState('')
  const [detailObject, setDetailObject] = useState<Record<string, unknown> | null>(null)
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [viewMode, setViewMode] = useState<'table' | 'cards'>('table')

  /* object types */
  const typesQ = useQuery({
    queryKey: exKeys.types(dbName, branch),
    queryFn: () => listObjectTypesV2(ctx, dbName, { branch }),
  })

  const objectTypes = useMemo(() => {
    const d = typesQ.data
    if (Array.isArray(d)) return d as Record<string, unknown>[]
    if (d && typeof d === 'object' && 'data' in d) {
      const inner = (d as { data?: unknown }).data
      if (Array.isArray(inner)) return inner as Record<string, unknown>[]
    }
    return []
  }, [typesQ.data])

  /* search objects */
  const searchMut = useMutation({
    mutationFn: async () => {
      if (!selectedType) throw new Error('Select an object type')

      // Try v2 search first, fallback to v1 list
      try {
        const query: Record<string, unknown> = searchTerm
          ? { type: 'containsAnyTerm', field: '__primaryKey', value: searchTerm }
          : {}
        const result = await searchObjectsV2(ctx, dbName, selectedType, {
          where: query,
          pageSize: 100,
        }, { branch })
        return result
      } catch {
        // Fallback to v1 instances
        const result = await listInstancesCtx(ctx, dbName, selectedType, {
          limit: 100,
          search: searchTerm || undefined,
        })
        return result
      }
    },
  })

  const searchResults = useMemo(() => {
    if (!searchMut.data) return []
    const d = searchMut.data as Record<string, unknown>
    if (Array.isArray(d)) return d
    if ('data' in d && Array.isArray(d.data)) return d.data as Record<string, unknown>[]
    if ('instances' in d && Array.isArray(d.instances)) return d.instances as Record<string, unknown>[]
    if ('objects' in d && Array.isArray(d.objects)) return d.objects as Record<string, unknown>[]
    return []
  }, [searchMut.data])

  /* extract column names from results */
  const columns = useMemo(() => {
    if (searchResults.length === 0) return []
    const first = searchResults[0] as Record<string, unknown>
    const props = first.properties ?? first
    if (typeof props === 'object' && props !== null && !Array.isArray(props)) {
      return Object.keys(props as Record<string, unknown>).slice(0, 8)
    }
    return Object.keys(first).filter((k) => !k.startsWith('_')).slice(0, 8)
  }, [searchResults])

  const handleSearch = () => {
    if (selectedType) searchMut.mutate()
  }

  const openDetail = (obj: Record<string, unknown>) => {
    setDetailObject(obj)
    setDrawerOpen(true)
  }

  return (
    <div>
      <PageHeader
        title="Object Explorer"
        subtitle={`Search and explore ontology objects in ${dbName}`}
      />

      {/* Search Bar */}
      <Card style={{ marginBottom: 12 }}>
        <div className="form-row" style={{ alignItems: 'flex-end', gap: 12 }}>
          <FormGroup label={
            <Tooltip content="Select the ontology object type to search (e.g. Employee, Order)" placement="top">
              <span className="tooltip-label">Object Type</span>
            </Tooltip>
          } style={{ flex: 1, marginBottom: 0 }}>
            <HTMLSelect
              value={selectedType}
              onChange={(e) => setSelectedType(e.target.value)}
              fill
              options={[
                { value: '', label: '-- Select type --' },
                ...objectTypes.map((ot) => ({
                  value: String(ot.apiName ?? ot.name ?? ot.class_id ?? ''),
                  label: String(ot.apiName ?? ot.name ?? ot.class_id ?? 'Unknown'),
                })),
              ]}
            />
          </FormGroup>
          <FormGroup label={
            <Tooltip content="Search objects by their primary key or text fields" placement="top">
              <span className="tooltip-label">Search</span>
            </Tooltip>
          } style={{ flex: 2, marginBottom: 0 }}>
            <InputGroup
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              placeholder="Search objects..."
              leftIcon="search"
              onKeyDown={(e) => { if (e.key === 'Enter') handleSearch() }}
            />
          </FormGroup>
          <Button
            intent={Intent.PRIMARY}
            icon="search"
            loading={searchMut.isPending}
            disabled={!selectedType}
            onClick={handleSearch}
          >
            Search
          </Button>
          <Button
            minimal
            icon={viewMode === 'table' ? 'grid-view' : 'th'}
            onClick={() => setViewMode(viewMode === 'table' ? 'cards' : 'table')}
          >
            {viewMode === 'table' ? 'Cards' : 'Table'}
          </Button>
        </div>
      </Card>

      {/* Results */}
      {searchMut.error && (
        <Callout intent={Intent.DANGER} style={{ marginBottom: 12 }}>Search failed.</Callout>
      )}

      {!searchMut.data && !searchMut.isPending && (
        <Card>
          <Callout icon="info-sign">
            Select an object type and click Search to explore objects.
          </Callout>
        </Card>
      )}

      {searchMut.isPending && (
        <Card style={{ textAlign: 'center', padding: 40 }}>
          <Spinner size={30} />
          <p style={{ marginTop: 12 }}>Searching...</p>
        </Card>
      )}

      {searchResults.length > 0 && (
        <Card>
          <div className="form-row" style={{ marginBottom: 8 }}>
            <Tag minimal>{searchResults.length} results</Tag>
            <Tag minimal icon="cube">{selectedType}</Tag>
          </div>

          {viewMode === 'table' ? (
            /* Table View */
            <div style={{ overflow: 'auto' }}>
              <HTMLTable compact striped interactive style={{ width: '100%' }}>
                <thead>
                  <tr>
                    {columns.map((col) => (
                      <th key={col}>{col}</th>
                    ))}
                    <th style={{ width: 60 }}>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {searchResults.map((obj, i) => {
                    const row = (obj as Record<string, unknown>).properties ?? obj
                    const rowData = row as Record<string, unknown>
                    return (
                      <tr key={i} onClick={() => openDetail(obj as Record<string, unknown>)} style={{ cursor: 'pointer' }}>
                        {columns.map((col) => (
                          <td key={col} style={{ maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                            {formatCellValue(rowData[col])}
                          </td>
                        ))}
                        <td>
                          <Button small minimal icon="eye-open" onClick={(e) => {
                            e.stopPropagation()
                            openDetail(obj as Record<string, unknown>)
                          }} />
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </HTMLTable>
            </div>
          ) : (
            /* Card View */
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))', gap: 12 }}>
              {searchResults.map((obj, i) => {
                const row = (obj as Record<string, unknown>).properties ?? obj
                const rowData = row as Record<string, unknown>
                const pk = (obj as Record<string, unknown>).__primaryKey ?? (obj as Record<string, unknown>).id ?? i
                return (
                  <Card key={i} interactive onClick={() => openDetail(obj as Record<string, unknown>)}>
                    <div style={{ fontWeight: 600, marginBottom: 8 }}>
                      <Tag icon="cube">{String(pk)}</Tag>
                    </div>
                    {columns.slice(0, 4).map((col) => (
                      <div key={col} style={{ fontSize: 12, marginBottom: 2 }}>
                        <span style={{ opacity: 0.6 }}>{col}: </span>
                        {formatCellValue(rowData[col])}
                      </div>
                    ))}
                  </Card>
                )
              })}
            </div>
          )}
        </Card>
      )}

      {searchResults.length === 0 && searchMut.data && !searchMut.isPending && (
        <Card>
          <Callout>No objects found matching your criteria.</Callout>
        </Card>
      )}

      {/* Detail Drawer */}
      <Drawer
        isOpen={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        title="Object Detail"
        size="50%"
      >
        {detailObject && (
          <div style={{ padding: 20 }}>
            <ObjectDetailView object={detailObject} />
          </div>
        )}
      </Drawer>
    </div>
  )
}

/* ── Object Detail View ──────────────────────────────── */
const ObjectDetailView = ({ object }: { object: Record<string, unknown> }) => {
  const [tab, setTab] = useState('properties')

  const props = (object.properties ?? object) as Record<string, unknown>
  const propEntries = typeof props === 'object' && !Array.isArray(props) ? Object.entries(props) : []
  const pk = object.__primaryKey ?? object.id ?? ''

  return (
    <div>
      <div className="form-row" style={{ marginBottom: 16 }}>
        {pk && <Tag icon="key" intent={Intent.PRIMARY}>{String(pk)}</Tag>}
        {typeof object.__objectType === 'string' && <Tag icon="cube">{object.__objectType}</Tag>}
      </div>

      <Tabs selectedTabId={tab} onChange={(id) => setTab(id as string)}>
        <Tab id="properties" title="Properties" panel={
          <HTMLTable compact striped style={{ width: '100%' }}>
            <thead>
              <tr>
                <th>Property</th>
                <th>Value</th>
              </tr>
            </thead>
            <tbody>
              {propEntries.map(([key, val]) => (
                <tr key={key}>
                  <td style={{ fontFamily: 'monospace', fontSize: 12, fontWeight: 500 }}>{key}</td>
                  <td style={{ fontSize: 12 }}>{formatCellValue(val)}</td>
                </tr>
              ))}
            </tbody>
          </HTMLTable>
        } />
        <Tab id="raw" title="Raw JSON" panel={<JsonViewer value={object} />} />
      </Tabs>
    </div>
  )
}

/* ── helpers ─────────────────────────────────────────── */
function formatCellValue(val: unknown): string {
  if (val === null || val === undefined) return '—'
  if (typeof val === 'boolean') return val ? 'true' : 'false'
  if (typeof val === 'object') return JSON.stringify(val).slice(0, 50)
  return String(val)
}
