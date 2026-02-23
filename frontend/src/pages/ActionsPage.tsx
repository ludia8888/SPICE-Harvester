import { useState, useMemo } from 'react'
import {
  Button,
  Card,
  Callout,
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
import { StatusBadge } from '../components/ux/StatusBadge'
import { KeyValueEditor } from '../components/ux/KeyValueEditor'
import { useRequestContext } from '../api/useRequestContext'
import { useAppStore } from '../store/useAppStore'
import type { RequestContext, FoundryActionTypeRecordV2 } from '../api/bff'
import {
  listActionTypesV2,
  getActionTypeV2,
  applyActionV2,
} from '../api/bff'

/* ── query keys ─────────────────────────────────────── */
const actKeys = {
  list: (db: string, branch: string) => ['actions', db, branch] as const,
  detail: (db: string, name: string, branch: string) => ['actions', 'detail', db, name, branch] as const,
}

/* ── page ────────────────────────────────────────────── */
export const ActionsPage = ({ dbName }: { dbName: string }) => {
  const ctx = useRequestContext()
  const branch = useAppStore((s) => s.context.branch)

  const [selected, setSelected] = useState<string | null>(null)
  const [detailTab, setDetailTab] = useState<string>('execute')

  /* list action types */
  const listQ = useQuery({
    queryKey: actKeys.list(dbName, branch),
    queryFn: () => listActionTypesV2(ctx, dbName, { branch }),
  })

  const actionTypes: FoundryActionTypeRecordV2[] = useMemo(() => {
    const d = listQ.data
    if (Array.isArray(d)) return d
    if (d && typeof d === 'object' && 'data' in d) {
      const inner = (d as { data?: unknown }).data
      if (Array.isArray(inner)) return inner as FoundryActionTypeRecordV2[]
    }
    return []
  }, [listQ.data])

  /* detail */
  const detailQ = useQuery({
    queryKey: actKeys.detail(dbName, selected ?? '', branch),
    queryFn: () => getActionTypeV2(ctx, dbName, selected!, { branch }),
    enabled: !!selected,
  })

  const selectedAction = detailQ.data as FoundryActionTypeRecordV2 | undefined

  return (
    <div>
      <PageHeader
        title="Actions"
        subtitle={`${actionTypes.length} action types · ${dbName}`}
        actions={
          <Button icon="refresh" minimal loading={listQ.isFetching} onClick={() => listQ.refetch()}>
            Refresh
          </Button>
        }
      />

      <div className="two-col-grid">
        {/* Left: Action Type List */}
        <div className="card-stack">
          <Card>
            <div className="card-title">Action Types</div>
            {listQ.isLoading && <Spinner size={20} />}
            {listQ.error && <Callout intent={Intent.DANGER}>Failed to load action types.</Callout>}
            {actionTypes.length === 0 && !listQ.isLoading && (
              <Callout intent={Intent.NONE}>
                No action types defined. Define actions in the Ontology Manager.
              </Callout>
            )}
            <div className="nav-list">
              {actionTypes.map((at, i) => {
                const name = at.apiName ?? `action_${i}`
                const isActive = selected === name
                return (
                  <button
                    key={name}
                    className={`nav-item ${isActive ? 'is-active' : ''}`}
                    onClick={() => { setSelected(name); setDetailTab('execute') }}
                  >
                    <Tag icon="play" minimal>{name}</Tag>
                    {at.status && (
                      <Tag
                        minimal
                        intent={at.status === 'ACTIVE' ? Intent.SUCCESS : Intent.NONE}
                        style={{ marginLeft: 8 }}
                      >
                        {at.status}
                      </Tag>
                    )}
                  </button>
                )
              })}
            </div>
          </Card>
        </div>

        {/* Right: Detail */}
        <div className="card-stack">
          {!selected ? (
            <Card>
              <Callout icon="info-sign">Select an action type from the list to view details and execute.</Callout>
            </Card>
          ) : (
            <Card>
              <div className="card-title">{selected}</div>
              {detailQ.isLoading && <Spinner size={20} />}
              {selectedAction && (
                <div className="form-row" style={{ marginBottom: 12 }}>
                  {selectedAction.displayName && (
                    <Tag icon="tag">{selectedAction.displayName}</Tag>
                  )}
                  {selectedAction.targetObjectType && (
                    <Tag icon="cube">{selectedAction.targetObjectType}</Tag>
                  )}
                  {selectedAction.status && (
                    <Tag
                      intent={selectedAction.status === 'ACTIVE' ? Intent.SUCCESS : Intent.NONE}
                      minimal
                    >
                      {selectedAction.status}
                    </Tag>
                  )}
                </div>
              )}
              {typeof selectedAction?.description === 'string' && selectedAction.description && (
                <p style={{ fontSize: 13, opacity: 0.8, marginBottom: 12 }}>
                  {selectedAction.description}
                </p>
              )}

              <Tabs selectedTabId={detailTab} onChange={(id) => setDetailTab(id as string)}>
                <Tab id="execute" title="Execute" panel={
                  <ActionExecutePanel
                    dbName={dbName}
                    branch={branch}
                    actionType={selected}
                    actionDef={selectedAction}
                    ctx={ctx}
                  />
                } />
                <Tab id="definition" title="Definition" panel={
                  <div>
                    {selectedAction?.parameters && (
                      <>
                        <div className="card-title" style={{ fontSize: 13, marginBottom: 8 }}>Parameters</div>
                        <ParameterTable parameters={selectedAction.parameters} />
                      </>
                    )}
                    {selectedAction?.operations && (
                      <>
                        <div className="card-title" style={{ fontSize: 13, marginTop: 16, marginBottom: 8 }}>Operations</div>
                        <JsonViewer value={selectedAction.operations} />
                      </>
                    )}
                  </div>
                } />
                <Tab id="raw" title="Raw JSON" panel={<JsonViewer value={selectedAction} />} />
              </Tabs>
            </Card>
          )}
        </div>
      </div>
    </div>
  )
}

/* ── Parameter Table ─────────────────────────────────── */
const ParameterTable = ({ parameters }: { parameters: Record<string, unknown> }) => {
  const paramEntries = useMemo(() => {
    if (Array.isArray(parameters)) {
      return parameters.map((p: Record<string, unknown>) => ({
        id: String(p.parameterId ?? p.name ?? p.id ?? ''),
        type: String(p.dataType ?? p.type ?? '?'),
        required: Boolean(p.required),
        description: String(p.description ?? ''),
      }))
    }
    return Object.entries(parameters).map(([key, val]) => {
      const v = val as Record<string, unknown> | string
      if (typeof v === 'string') return { id: key, type: v, required: false, description: '' }
      return {
        id: key,
        type: String((v as Record<string, unknown>).dataType ?? (v as Record<string, unknown>).type ?? '?'),
        required: Boolean((v as Record<string, unknown>).required),
        description: String((v as Record<string, unknown>).description ?? ''),
      }
    })
  }, [parameters])

  if (paramEntries.length === 0) return <Callout>No parameters defined.</Callout>

  return (
    <HTMLTable compact striped style={{ width: '100%' }}>
      <thead>
        <tr>
          <th>Parameter</th>
          <th>Type</th>
          <th>Required</th>
          <th>Description</th>
        </tr>
      </thead>
      <tbody>
        {paramEntries.map((p, i) => (
          <tr key={i}>
            <td style={{ fontFamily: 'monospace', fontSize: 12 }}>{p.id}</td>
            <td><Tag minimal>{p.type}</Tag></td>
            <td>{p.required ? <Tag intent={Intent.WARNING} minimal>Yes</Tag> : 'No'}</td>
            <td style={{ fontSize: 12 }}>{p.description}</td>
          </tr>
        ))}
      </tbody>
    </HTMLTable>
  )
}

/* ── Action Execute Panel ────────────────────────────── */
const ActionExecutePanel = ({
  dbName,
  branch,
  actionType,
  actionDef,
  ctx,
}: {
  dbName: string
  branch: string
  actionType: string
  actionDef: FoundryActionTypeRecordV2 | undefined
  ctx: RequestContext
}) => {
  const [fallbackKv, setFallbackKv] = useState<Record<string, string>>({})

  const applyMut = useMutation({
    mutationFn: () => {
      const params = paramFields.length > 0
        ? { parameters: paramValues }
        : { parameters: fallbackKv as Record<string, unknown> }
      return applyActionV2(ctx, dbName, actionType, params, { branch })
    },
  })

  /* dynamic form fields from action definition */
  const paramFields = useMemo(() => {
    if (!actionDef?.parameters || typeof actionDef.parameters !== 'object') return []
    return Object.entries(actionDef.parameters as Record<string, unknown>).map(([name, spec]) => {
      const s = spec as Record<string, unknown> | undefined
      const type = String(s?.type ?? s?.baseType ?? 'string').toLowerCase()
      const paramType = type.includes('int') || type.includes('double') || type.includes('float') || type.includes('number')
        ? 'number' as const
        : type.includes('bool')
          ? 'boolean' as const
          : type.includes('date') || type.includes('timestamp')
            ? 'date' as const
            : 'string' as const
      return {
        name,
        type: paramType,
        required: Boolean(s?.required),
        description: String(s?.description ?? ''),
      }
    })
  }, [actionDef])

  const [paramValues, setParamValues] = useState<Record<string, unknown>>({})

  return (
    <div>
      <FormGroup label={
        <Tooltip content="Fill in the parameters to execute this action on an object" placement="top">
          <span className="tooltip-label">Action Parameters</span>
        </Tooltip>
      }>
        {paramFields.length > 0 ? (
          <div className="action-param-form">
            {paramFields.map((p) => (
              <FormGroup key={p.name} label={
                <span className="tooltip-label">
                  {p.name}{p.required && <span style={{ color: '#db3737' }}> *</span>}
                  {p.description && (
                    <Tooltip content={p.description} placement="top">
                      <span style={{ cursor: 'help', opacity: 0.5, fontSize: 11 }}>?</span>
                    </Tooltip>
                  )}
                </span>
              }>
                {p.type === 'number' ? (
                  <InputGroup
                    type="number"
                    value={String(paramValues[p.name] ?? '')}
                    onChange={(e) => setParamValues({ ...paramValues, [p.name]: e.target.value ? Number(e.target.value) : '' })}
                    placeholder={`Enter ${p.name}`}
                  />
                ) : p.type === 'boolean' ? (
                  <HTMLSelect
                    value={String(paramValues[p.name] ?? '')}
                    onChange={(e) => setParamValues({ ...paramValues, [p.name]: e.target.value === 'true' })}
                    options={['', 'true', 'false']}
                    fill
                  />
                ) : p.type === 'date' ? (
                  <InputGroup
                    type="date"
                    value={String(paramValues[p.name] ?? '')}
                    onChange={(e) => setParamValues({ ...paramValues, [p.name]: e.target.value })}
                  />
                ) : (
                  <InputGroup
                    value={String(paramValues[p.name] ?? '')}
                    onChange={(e) => setParamValues({ ...paramValues, [p.name]: e.target.value })}
                    placeholder={`Enter ${p.name}`}
                  />
                )}
              </FormGroup>
            ))}
          </div>
        ) : (
          <KeyValueEditor
            value={fallbackKv}
            onChange={setFallbackKv}
            keyPlaceholder="Parameter name"
            valuePlaceholder="Parameter value"
            addLabel="Add parameter"
          />
        )}
      </FormGroup>
      <div className="form-row" style={{ marginBottom: 12 }}>
        <Button
          intent={Intent.SUCCESS}
          icon="play"
          loading={applyMut.isPending}
          onClick={() => applyMut.mutate()}
        >
          Apply Action
        </Button>
        <Button
          minimal
          icon="reset"
          onClick={() => { setParamValues({}); setFallbackKv({}) }}
        >
          Reset
        </Button>
      </div>

      {applyMut.error && (
        <Callout intent={Intent.DANGER} style={{ marginBottom: 8 }}>
          Action execution failed.
        </Callout>
      )}

      {applyMut.data && (
        <div>
          <div className="card-title" style={{ fontSize: 13, marginBottom: 8 }}>Result</div>
          <div className="form-row" style={{ marginBottom: 8 }}>
            <StatusBadge
              status={applyMut.data.writebackStatus === 'confirmed' ? 'success' : 'warning'}
              label={`Writeback: ${applyMut.data.writebackStatus}`}
            />
            {applyMut.data.auditLogId && (
              <Tag minimal icon="history">
                Audit: {String(applyMut.data.auditLogId).slice(0, 12)}...
              </Tag>
            )}
          </div>
          <JsonViewer value={applyMut.data} />
        </div>
      )}
    </div>
  )
}
