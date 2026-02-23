import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Button,
  Callout,
  Card,
  Checkbox,
  Dialog,
  DialogBody,
  DialogFooter,
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
import MonacoEditor from '@monaco-editor/react'
import { useAppStore as useAppStoreNew } from '../store/useAppStore'
import {
  createObjectTypeV2,
  getObjectTypeFullMetadataV2,
  getOntology,
  getSummary,
  listObjectTypesV2,
  listOntology,
  listActionTypesV2,
  updateObjectTypeV2,
} from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { DangerConfirmDialog } from '../components/DangerConfirmDialog'
import { JsonViewer } from '../components/JsonViewer'
import { PageHeader } from '../components/layout/PageHeader'
import { showAppToast } from '../app/AppToaster'
import { useCommandRegistration } from '../commands/useCommandRegistration'
import { extractCommandId } from '../commands/extractCommandId'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'

/* ═══════════════════════════════════════════════════════════════════════════
   Ontology Manager (v2 – visual property editor + JSON fallback)
   ═══════════════════════════════════════════════════════════════════════════ */

type OntologyItem = Record<string, unknown>

/* ── data type options ──────────────────────────────── */
const DATA_TYPES = [
  'string', 'integer', 'long', 'double', 'boolean',
  'date', 'timestamp', 'geopoint', 'geoshape', 'array',
  'xsd:string', 'xsd:integer', 'xsd:decimal', 'xsd:boolean', 'xsd:dateTime',
] as const

/* ── helpers ─────────────────────────────────────────── */
const asRecord = (value: unknown): Record<string, unknown> | null =>
  value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : null

const toStringValue = (value: unknown) =>
  typeof value === 'string' ? value.trim() : (typeof value === 'number' ? String(value) : '')

const normalizeApiName = (value: string) =>
  value.trim().replace(/[^a-zA-Z0-9_]/g, '_').replace(/_+/g, '_').replace(/^_+|_+$/g, '')

const toObjectArray = (value: unknown) =>
  Array.isArray(value)
    ? value.map((item) => asRecord(item)).filter((item): item is Record<string, unknown> => Boolean(item))
    : []

/* ── contract builder ────────────────────────────────── */
type ObjectTypeContractDraft = {
  apiName: string
  createInput: Parameters<typeof createObjectTypeV2>[2]
  updateInput: Parameters<typeof updateObjectTypeV2>[3]
}

const buildNewOntologyDraft = (lang: string) => ({
  id: '',
  label: lang === 'ko' ? '새 클래스' : 'NewClass',
  description: '',
  properties: [],
  metadata: {},
})

const buildObjectTypeContractDraft = (
  payload: Record<string, unknown>,
  fallbackClassId?: string | null,
): ObjectTypeContractDraft => {
  const draftApiName = normalizeApiName(
    toStringValue(payload.id) || toStringValue(payload.apiName) ||
    toStringValue(payload['@id']) || toStringValue(payload.label) ||
    toStringValue(fallbackClassId),
  )
  if (!draftApiName) throw new Error('Class ID is required.')

  const draftLabel = toStringValue(payload.label) || draftApiName
  const draftDescription = toStringValue(payload.description)
  const draftProperties = toObjectArray(payload.properties).map((property) => ({
    name: toStringValue(property.name || property.id || property.apiName),
    label: toStringValue(property.label) || toStringValue(property.name || property.id || property.apiName),
    type: toStringValue(property.type) || 'xsd:string',
    required: Boolean(property.required),
    primaryKey: Boolean(property.primaryKey),
    titleKey: Boolean(property.titleKey),
  }))

  const primaryProperty =
    draftProperties.find((p) => p.primaryKey && p.name) ??
    draftProperties.find((p) => p.required && p.name) ??
    draftProperties.find((p) => p.name)
  const titleProperty =
    draftProperties.find((p) => p.titleKey && p.name) ?? primaryProperty

  const draftMetadata = asRecord(payload.metadata) ?? {}
  const metadata: Record<string, unknown> = {
    ...draftMetadata, displayName: draftLabel, description: draftDescription,
    properties: draftProperties, source: 'frontend_ontology_editor_v2',
  }

  const createInput: Parameters<typeof createObjectTypeV2>[2] = {
    apiName: draftApiName,
    status: toStringValue(payload.status) || 'ACTIVE',
    primaryKey: primaryProperty?.name || undefined,
    titleProperty: titleProperty?.name || undefined,
    metadata,
  }
  const updateInput: Parameters<typeof updateObjectTypeV2>[3] = {
    status: toStringValue(payload.status) || 'ACTIVE',
    primaryKey: primaryProperty?.name || undefined,
    titleProperty: titleProperty?.name || undefined,
    metadata,
  }

  for (const field of ['pkSpec', 'backingSource', 'backingSources', 'backingDatasetId', 'backingDatasourceId', 'backingDatasourceVersionId', 'datasetVersionId', 'mappingSpecId', 'mappingSpecVersion'] as const) {
    const val = payload[field]
    if (val !== undefined && val !== null && val !== '') {
      ;(createInput as Record<string, unknown>)[field] = val
      ;(updateInput as Record<string, unknown>)[field] = val
    }
  }

  return { apiName: draftApiName, createInput, updateInput }
}

/* ── property row type ───────────────────────────────── */
type PropertyRow = {
  name: string
  label: string
  type: string
  required: boolean
  primaryKey: boolean
  titleKey: boolean
}

/* ═══════════════════════════════════════════════════════
   Page Component
   ═══════════════════════════════════════════════════════ */
export const OntologyPage = ({ dbName }: { dbName: string }) => {
  const queryClient = useQueryClient()
  const requestContext = useRequestContext()
  const language = useAppStoreNew((state) => state.context.language)
  const branch = useAppStoreNew((state) => state.context.branch)
  const adminToken = useAppStoreNew((state) => state.adminToken)
  const adminMode = useAppStoreNew((state) => state.adminMode)
  const registerCommand = useCommandRegistration()

  const [selectedClass, setSelectedClass] = useState<string | null>(null)
  const [draft, setDraft] = useState(() => JSON.stringify(buildNewOntologyDraft(language), null, 2))
  const [draftError, setDraftError] = useState<string | null>(null)
  const [validationResult, setValidationResult] = useState<unknown>(null)
  const [confirmAction, setConfirmAction] = useState<'apply' | null>(null)
  const [detailTab, setDetailTab] = useState<string>('properties')
  const [createDialogOpen, setCreateDialogOpen] = useState(false)

  /* ── queries ──────────────────────────────────────── */
  const listQuery = useQuery({
    queryKey: qk.ontologyList(dbName, branch, requestContext.language),
    queryFn: () => listOntology(requestContext, dbName, branch),
  })

  const typesV2Query = useQuery({
    queryKey: ['ontology', 'v2-types', dbName, branch],
    queryFn: () => listObjectTypesV2(requestContext, dbName, { branch }),
  })

  const summaryQuery = useQuery({
    queryKey: qk.summary({ dbName, branch, language: requestContext.language }),
    queryFn: () => getSummary(requestContext, { dbName, branch }),
  })

  const detailQuery = useQuery({
    queryKey: selectedClass
      ? qk.ontology(dbName, selectedClass, branch, requestContext.language)
      : ['ontology', 'none'],
    queryFn: () => getOntology(requestContext, dbName, selectedClass ?? '', branch),
    enabled: Boolean(selectedClass),
  })

  const metadataQuery = useQuery({
    queryKey: selectedClass
      ? qk.ontologySchema(dbName, selectedClass, branch, 'fullMetadata', requestContext.language)
      : ['ontology-schema', 'none'],
    queryFn: () => getObjectTypeFullMetadataV2(requestContext, dbName, selectedClass ?? '', { branch }),
    enabled: Boolean(selectedClass),
  })

  const actionsQuery = useQuery({
    queryKey: ['ontology', 'actions', dbName, branch],
    queryFn: () => listActionTypesV2(requestContext, dbName, { branch }),
  })

  const isProtected = Boolean(
    (summaryQuery.data as { data?: { policy?: { is_protected_branch?: boolean } } } | undefined)?.data
      ?.policy?.is_protected_branch,
  )

  /* ── sync draft with selected class ────────────────── */
  useEffect(() => {
    if (!selectedClass) {
      setDraft(JSON.stringify(buildNewOntologyDraft(language), null, 2))
      return
    }
    if (detailQuery.data) {
      setDraft(JSON.stringify(detailQuery.data, null, 2))
    }
  }, [detailQuery.data, language, selectedClass])

  /* ── ontology list extraction ──────────────────────── */
  const ontologies = useMemo(() => {
    const payload = listQuery.data ?? {}
    const list = payload as { ontologies?: OntologyItem[]; data?: { ontologies?: OntologyItem[] } }
    return list.ontologies ?? list.data?.ontologies ?? []
  }, [listQuery.data])

  /* ── v2 types for richer info ──────────────────────── */
  const v2Types = useMemo(() => {
    const d = typesV2Query.data
    if (Array.isArray(d)) return d as Record<string, unknown>[]
    if (d && typeof d === 'object' && 'data' in d) {
      const inner = (d as { data?: unknown }).data
      if (Array.isArray(inner)) return inner as Record<string, unknown>[]
    }
    return []
  }, [typesV2Query.data])

  /* ── action types ──────────────────────────────────── */
  const actionTypes = useMemo(() => {
    const d = actionsQuery.data
    if (Array.isArray(d)) return d as Record<string, unknown>[]
    if (d && typeof d === 'object' && 'data' in d) {
      const inner = (d as { data?: unknown }).data
      if (Array.isArray(inner)) return inner as Record<string, unknown>[]
    }
    return []
  }, [actionsQuery.data])

  /* ── extract properties from draft ─────────────────── */
  const properties: PropertyRow[] = useMemo(() => {
    try {
      const parsed = JSON.parse(draft) as Record<string, unknown>
      return toObjectArray(parsed.properties).map((p) => ({
        name: toStringValue(p.name || p.id || p.apiName),
        label: toStringValue(p.label) || toStringValue(p.name || p.id || p.apiName),
        type: toStringValue(p.type) || 'string',
        required: Boolean(p.required),
        primaryKey: Boolean(p.primaryKey),
        titleKey: Boolean(p.titleKey),
      }))
    } catch {
      return []
    }
  }, [draft])

  /* ── property mutations ────────────────────────────── */
  const updateProperties = (newProps: PropertyRow[]) => {
    try {
      const parsed = JSON.parse(draft) as Record<string, unknown>
      parsed.properties = newProps
      setDraft(JSON.stringify(parsed, null, 2))
    } catch { /* ignore */ }
  }

  const addProperty = () => {
    updateProperties([
      ...properties,
      { name: '', label: '', type: 'string', required: false, primaryKey: false, titleKey: false },
    ])
  }

  const removeProperty = (idx: number) => {
    updateProperties(properties.filter((_, i) => i !== idx))
  }

  const updateProperty = (idx: number, field: keyof PropertyRow, value: unknown) => {
    const next = properties.map((p, i) => i === idx ? { ...p, [field]: value } : p)
    updateProperties(next)
  }

  /* ── validate + apply (preserved from original) ────── */
  const validateMutation = useMutation({
    mutationFn: async () => {
      const parsed = JSON.parse(draft) as Record<string, unknown>
      const normalized = buildObjectTypeContractDraft(parsed, selectedClass)
      return {
        valid: true,
        mode: selectedClass ? 'update' : 'create',
        branch,
        objectType: normalized.apiName,
        createPayload: normalized.createInput,
        updatePayload: normalized.updateInput,
      }
    },
    onSuccess: (result) => setValidationResult(result),
    onError: (error) => { setValidationResult(null); toastApiError(error, language) },
  })

  const applyMutation = useMutation({
    mutationFn: async () => {
      const parsed = JSON.parse(draft) as Record<string, unknown>
      const normalized = buildObjectTypeContractDraft(parsed, selectedClass)
      if (!selectedClass) {
        return createObjectTypeV2(requestContext, dbName, normalized.createInput, { branch })
      }
      if (normalized.apiName !== selectedClass) {
        throw new Error('Object type rename is not supported.')
      }
      return updateObjectTypeV2(requestContext, dbName, selectedClass, normalized.updateInput, { branch })
    },
    onSuccess: (payload) => {
      const commandId = extractCommandId(payload)
      let targetClassId = selectedClass ?? undefined
      if (!targetClassId) {
        try {
          const parsed = JSON.parse(draft) as { id?: string; '@id'?: string }
          targetClassId = String(parsed.id ?? parsed['@id'] ?? '').trim() || undefined
        } catch { targetClassId = undefined }
      }
      if (commandId) {
        registerCommand({
          commandId,
          kind: selectedClass ? 'UPDATE_ONTOLOGY' : 'CREATE_ONTOLOGY',
          targetClassId,
          title: selectedClass ? `Update ontology: ${selectedClass}` : 'Create ontology',
        })
      }
      void showAppToast({ intent: Intent.SUCCESS, message: 'Ontology update submitted.' })
      void queryClient.invalidateQueries({ queryKey: qk.ontologyList(dbName, branch, requestContext.language) })
      void queryClient.invalidateQueries({ queryKey: ['ontology', 'v2-types', dbName, branch] })
      if (selectedClass) {
        void queryClient.invalidateQueries({
          queryKey: qk.ontology(dbName, selectedClass, branch, requestContext.language),
        })
      }
    },
    onError: (error) => toastApiError(error, language),
    onSettled: () => setConfirmAction(null),
  })

  const handleValidate = () => {
    setDraftError(null)
    try { JSON.parse(draft); validateMutation.mutate(undefined) }
    catch { setDraftError('Invalid JSON') }
  }

  const handleApply = () => {
    setDraftError(null)
    try { JSON.parse(draft) } catch { setDraftError('Invalid JSON'); return }
    if (isProtected) { setConfirmAction('apply'); return }
    applyMutation.mutate(undefined)
  }

  /* ── extract metadata for selected type ────────────── */
  const selectedV2Type = v2Types.find(
    (t) => t.apiName === selectedClass || t.name === selectedClass || t.class_id === selectedClass,
  )
  const selectedMetadata = asRecord(selectedV2Type?.metadata) ?? asRecord(metadataQuery.data) ?? {}
  const linkTypes = toObjectArray(
    selectedMetadata.outgoingLinkTypes ?? selectedMetadata.linkTypes ?? selectedMetadata.links,
  )

  return (
    <div>
      <PageHeader
        title="Ontology Manager"
        subtitle={`${ontologies.length} object types · Branch: ${branch}`}
        actions={
          <div className="form-row">
            <Button icon="plus" intent={Intent.PRIMARY} onClick={() => { setSelectedClass(null); setDetailTab('properties') }}>
              New Type
            </Button>
            <Button icon="refresh" minimal loading={listQuery.isFetching} onClick={() => { listQuery.refetch(); typesV2Query.refetch() }}>
              Refresh
            </Button>
          </div>
        }
      />

      {isProtected && (
        <Callout intent={Intent.WARNING} style={{ marginBottom: 12 }}>
          Protected branch. Admin token + Admin mode required for apply/delete.
        </Callout>
      )}

      <div className="two-col-grid">
        {/* ── Left: Type List ──────────────────────── */}
        <div className="card-stack">
          <Card>
            <div className="card-title">
              <Tooltip content="Object types define the schema for entities in your ontology (e.g. Employee, Order)" placement="top">
                <span className="tooltip-label">Object Types</span>
              </Tooltip>
            </div>
            {listQuery.isFetching && <Spinner size={16} />}
            {ontologies.length === 0 && !listQuery.isFetching && (
              <Callout intent={Intent.NONE}>No object types defined yet.</Callout>
            )}
            <div className="nav-list">
              {ontologies.map((item) => {
                const id = (item.id as string) || (item['@id'] as string) || ''
                const label = (item.label as string) || id || 'unknown'
                const active = id === selectedClass
                const propCount = Array.isArray(item.properties) ? item.properties.length : undefined
                return (
                  <button
                    key={id || label}
                    className={`nav-item ${active ? 'is-active' : ''}`}
                    onClick={() => { setSelectedClass(id || label); setDetailTab('properties') }}
                    style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}
                  >
                    <span>{label}</span>
                    {propCount != null && (
                      <Tag minimal style={{ marginLeft: 8 }}>{propCount} props</Tag>
                    )}
                  </button>
                )
              })}
            </div>
          </Card>

          {/* Action Types Summary */}
          <Card>
            <div className="card-title">
              <Tooltip content="Actions define operations that can be performed on objects (e.g. Apply, Simulate)" placement="top">
                <span className="tooltip-label">Action Types</span>
              </Tooltip>
            </div>
            {actionsQuery.isLoading && <Spinner size={16} />}
            {actionTypes.length === 0 && !actionsQuery.isLoading && (
              <Callout intent={Intent.NONE}>No action types defined.</Callout>
            )}
            {actionTypes.length > 0 && (
              <div className="nav-list">
                {actionTypes.map((at, i) => {
                  const name = String(at.apiName ?? at.name ?? `action_${i}`)
                  return (
                    <div key={i} className="nav-item" style={{ cursor: 'default' }}>
                      <Tag icon="play" minimal>{name}</Tag>
                    </div>
                  )
                })}
              </div>
            )}
          </Card>
        </div>

        {/* ── Right: Detail ────────────────────────── */}
        <div className="card-stack">
          <Card>
            <div className="card-title" style={{ marginBottom: 4 }}>
              {selectedClass ?? 'New Object Type'}
            </div>
            <div className="form-row" style={{ marginBottom: 12 }}>
              <Button
                small icon="tick-circle"
                onClick={handleValidate}
                loading={validateMutation.isPending}
              >
                Validate
              </Button>
              <Button
                small intent={Intent.PRIMARY} icon="cloud-upload"
                onClick={handleApply}
                loading={applyMutation.isPending}
                disabled={isProtected && (!adminMode || !adminToken)}
              >
                Apply
              </Button>
            </div>

            {draftError && <Callout intent={Intent.DANGER} style={{ marginBottom: 8 }}>{draftError}</Callout>}

            <Tabs selectedTabId={detailTab} onChange={(id) => setDetailTab(id as string)}>
              {/* ── Properties Tab ──────────────────── */}
              <Tab id="properties" title="Properties" panel={
                <div>
                  <div className="form-row" style={{ marginBottom: 8 }}>
                    <Button small icon="plus" intent={Intent.SUCCESS} onClick={addProperty}>
                      Add Property
                    </Button>
                  </div>
                  {properties.length === 0 && (
                    <Callout>No properties yet. Add properties to define the object type schema.</Callout>
                  )}
                  {properties.length > 0 && (
                    <HTMLTable compact striped style={{ width: '100%' }}>
                      <thead>
                        <tr>
                          <th>Name</th>
                          <th>Type</th>
                          <th style={{ width: 50 }}>
                            <Tooltip content="Primary Key — unique identifier for each object instance" placement="top">
                              <span className="tooltip-label">PK</span>
                            </Tooltip>
                          </th>
                          <th style={{ width: 50 }}>
                            <Tooltip content="Required — this property must have a value" placement="top">
                              <span className="tooltip-label">Req</span>
                            </Tooltip>
                          </th>
                          <th style={{ width: 40 }}></th>
                        </tr>
                      </thead>
                      <tbody>
                        {properties.map((prop, i) => (
                          <tr key={i}>
                            <td>
                              <InputGroup
                                small
                                value={prop.name}
                                onChange={(e) => updateProperty(i, 'name', e.target.value)}
                                placeholder="property_name"
                              />
                            </td>
                            <td>
                              <HTMLSelect
                                value={prop.type}
                                onChange={(e) => updateProperty(i, 'type', e.target.value)}
                                options={DATA_TYPES.map((t) => ({ value: t, label: t }))}
                                fill
                                minimal
                              />
                            </td>
                            <td style={{ textAlign: 'center' }}>
                              <Checkbox
                                checked={prop.primaryKey}
                                onChange={(e) => updateProperty(i, 'primaryKey', (e.target as HTMLInputElement).checked)}
                                style={{ marginBottom: 0 }}
                              />
                            </td>
                            <td style={{ textAlign: 'center' }}>
                              <Checkbox
                                checked={prop.required}
                                onChange={(e) => updateProperty(i, 'required', (e.target as HTMLInputElement).checked)}
                                style={{ marginBottom: 0 }}
                              />
                            </td>
                            <td>
                              <Button
                                small minimal icon="cross" intent={Intent.DANGER}
                                onClick={() => removeProperty(i)}
                              />
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </HTMLTable>
                  )}
                </div>
              } />

              {/* ── Links Tab ──────────────────────── */}
              <Tab id="links" title="Links" panel={
                <div>
                  {!selectedClass && <Callout>Select an object type to see its relationships.</Callout>}
                  {selectedClass && linkTypes.length === 0 && (
                    <Callout>No link types defined for this object type.</Callout>
                  )}
                  {linkTypes.length > 0 && (
                    <HTMLTable compact striped style={{ width: '100%' }}>
                      <thead>
                        <tr>
                          <th>Link Name</th>
                          <th>Target</th>
                          <th>Cardinality</th>
                        </tr>
                      </thead>
                      <tbody>
                        {linkTypes.map((lt, i) => (
                          <tr key={i}>
                            <td><Tag minimal>{String(lt.apiName ?? lt.name ?? `link_${i}`)}</Tag></td>
                            <td>{String(lt.objectTypeApiName ?? lt.target ?? lt.targetType ?? '—')}</td>
                            <td><Tag minimal>{String(lt.cardinality ?? '—')}</Tag></td>
                          </tr>
                        ))}
                      </tbody>
                    </HTMLTable>
                  )}

                  {/* Mini relationship map */}
                  {selectedClass && ontologies.length > 1 && (
                    <div style={{ marginTop: 16 }}>
                      <div className="card-title" style={{ fontSize: 13, marginBottom: 8 }}>Relationship Map</div>
                      <RelationshipDiagram
                        selectedType={selectedClass}
                        allTypes={ontologies}
                        linkTypes={linkTypes}
                      />
                    </div>
                  )}
                </div>
              } />

              {/* ── Actions Tab ─────────────────────── */}
              <Tab id="actions" title="Actions" panel={
                <div>
                  {actionTypes.length === 0 && (
                    <Callout>No action types defined in this ontology.</Callout>
                  )}
                  {actionTypes.map((at, i) => {
                    const name = String(at.apiName ?? at.name ?? `action_${i}`)
                    const params = toObjectArray(at.parameters ?? at.params)
                    return (
                      <Card key={i} style={{ marginBottom: 8, padding: 12 }}>
                        <div style={{ fontWeight: 600, marginBottom: 4 }}>
                          <Tag icon="play" intent={Intent.PRIMARY}>{name}</Tag>
                        </div>
                        {typeof at.description === 'string' && at.description && (
                          <div style={{ fontSize: 12, opacity: 0.7, marginBottom: 4 }}>
                            {at.description}
                          </div>
                        )}
                        {params.length > 0 && (
                          <HTMLTable compact style={{ width: '100%', fontSize: 12 }}>
                            <thead>
                              <tr><th>Parameter</th><th>Type</th><th>Required</th></tr>
                            </thead>
                            <tbody>
                              {params.map((p, j) => (
                                <tr key={j}>
                                  <td>{String(p.name ?? p.parameterId ?? `param_${j}`)}</td>
                                  <td><Tag minimal>{String(p.dataType ?? p.type ?? '?')}</Tag></td>
                                  <td>{p.required ? 'Yes' : 'No'}</td>
                                </tr>
                              ))}
                            </tbody>
                          </HTMLTable>
                        )}
                      </Card>
                    )
                  })}
                </div>
              } />

              {/* ── JSON Editor Tab (preserved) ─────── */}
              <Tab id="json" title="JSON Editor" panel={
                <div>
                  <Callout intent={Intent.PRIMARY} style={{ marginBottom: 8 }}>
                    Advanced: edit the full JSON payload. Changes sync with the Properties tab.
                  </Callout>
                  <FormGroup label="Selected class">
                    <InputGroup value={selectedClass ?? '(new class)'} disabled />
                  </FormGroup>
                  <FormGroup label="JSON payload">
                    <div style={{ border: '1px solid #d8e1e8', borderRadius: 4, overflow: 'hidden' }}>
                      <MonacoEditor
                        height="340px"
                        language="json"
                        theme="vs-light"
                        value={draft}
                        onChange={(val) => setDraft(val ?? '')}
                        options={{
                          minimap: { enabled: false },
                          fontSize: 13,
                          lineNumbers: 'on',
                          scrollBeyondLastLine: false,
                          automaticLayout: true,
                          tabSize: 2,
                          wordWrap: 'on',
                          formatOnPaste: true,
                        }}
                      />
                    </div>
                  </FormGroup>
                </div>
              } />

              {/* ── Metadata Tab ────────────────────── */}
              <Tab id="metadata" title="Metadata" panel={
                <div>
                  {!selectedClass && <Callout>Select a class to view metadata.</Callout>}
                  {selectedClass && metadataQuery.isLoading && <Spinner size={20} />}
                  {selectedClass && !metadataQuery.isLoading && (
                    <>
                      <div className="form-row" style={{ marginBottom: 8 }}>
                        <Button small icon="refresh" onClick={() => metadataQuery.refetch()}>
                          Reload Metadata
                        </Button>
                      </div>
                      <JsonViewer value={metadataQuery.data ?? selectedV2Type} empty="No metadata available." />
                    </>
                  )}
                </div>
              } />

              {/* ── Validation Tab ──────────────────── */}
              <Tab id="validation" title="Validation" panel={
                <JsonViewer value={validationResult} empty="Run validation to see results." />
              } />
            </Tabs>
          </Card>
        </div>
      </div>

      {/* ── Danger Confirm Dialog (preserved) ──────── */}
      <DangerConfirmDialog
        isOpen={Boolean(confirmAction)}
        title="Apply ontology changes"
        description="This will apply object type contract changes on the protected branch."
        confirmLabel="Apply"
        cancelLabel="Cancel"
        confirmTextToType={selectedClass ?? 'class'}
        reasonLabel="Change reason"
        reasonPlaceholder="Why are you making this change?"
        typedLabel="Type class id to confirm"
        typedPlaceholder={selectedClass ?? 'class'}
        onCancel={() => setConfirmAction(null)}
        onConfirm={() => { if (confirmAction === 'apply') applyMutation.mutate(undefined) }}
        loading={applyMutation.isPending}
        footerHint="Protected branches require audit context."
      />
    </div>
  )
}

/* ══════════════════════════════════════════════════════════
   Relationship Diagram (simple SVG-based)
   ══════════════════════════════════════════════════════════ */
const RelationshipDiagram = ({
  selectedType,
  allTypes,
  linkTypes,
}: {
  selectedType: string
  allTypes: OntologyItem[]
  linkTypes: Record<string, unknown>[]
}) => {
  const targets = linkTypes
    .map((lt) => String(lt.objectTypeApiName ?? lt.target ?? lt.targetType ?? ''))
    .filter(Boolean)

  const relatedTypes = Array.from(new Set([selectedType, ...targets]))
  const nodeRadius = 40
  const centerX = 200
  const centerY = 100
  const orbitRadius = 80

  return (
    <svg
      width="100%"
      height={220}
      viewBox="0 0 400 220"
      style={{ background: '#f8f9fa', borderRadius: 6, border: '1px solid #e1e8ed' }}
    >
      {/* Center node */}
      <circle cx={centerX} cy={centerY} r={nodeRadius} fill="#137CBD" opacity={0.15} />
      <circle cx={centerX} cy={centerY} r={nodeRadius} fill="none" stroke="#137CBD" strokeWidth={2} />
      <text x={centerX} y={centerY + 4} textAnchor="middle" fontSize={11} fontWeight={600} fill="#137CBD">
        {selectedType.length > 12 ? selectedType.slice(0, 12) + '…' : selectedType}
      </text>

      {/* Related nodes */}
      {targets.map((target, i) => {
        const angle = (2 * Math.PI * i) / Math.max(targets.length, 1) - Math.PI / 2
        const tx = centerX + orbitRadius * 1.5 * Math.cos(angle)
        const ty = centerY + orbitRadius * Math.sin(angle)
        const linkName = String(linkTypes[i]?.apiName ?? linkTypes[i]?.name ?? '')

        // Edge line
        const dx = tx - centerX
        const dy = ty - centerY
        const dist = Math.sqrt(dx * dx + dy * dy)
        const sx = centerX + (dx / dist) * nodeRadius
        const sy = centerY + (dy / dist) * nodeRadius
        const ex = tx - (dx / dist) * (nodeRadius - 5)
        const ey = ty - (dy / dist) * (nodeRadius - 5)

        return (
          <g key={i}>
            <line x1={sx} y1={sy} x2={ex} y2={ey} stroke="#5C7080" strokeWidth={1.5} markerEnd="url(#arrow)" />
            <text
              x={(sx + ex) / 2}
              y={(sy + ey) / 2 - 6}
              textAnchor="middle"
              fontSize={9}
              fill="#5C7080"
            >
              {linkName.length > 15 ? linkName.slice(0, 15) + '…' : linkName}
            </text>
            <circle cx={tx} cy={ty} r={nodeRadius - 5} fill="#0D8050" opacity={0.1} />
            <circle cx={tx} cy={ty} r={nodeRadius - 5} fill="none" stroke="#0D8050" strokeWidth={1.5} />
            <text x={tx} y={ty + 4} textAnchor="middle" fontSize={10} fontWeight={500} fill="#0D8050">
              {target.length > 10 ? target.slice(0, 10) + '…' : target}
            </text>
          </g>
        )
      })}

      {/* Arrow marker */}
      <defs>
        <marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
          <path d="M 0 0 L 10 5 L 0 10 z" fill="#5C7080" />
        </marker>
      </defs>

      {targets.length === 0 && (
        <text x={centerX} y={centerY + nodeRadius + 25} textAnchor="middle" fontSize={11} fill="#8a9ba8">
          No outgoing links defined
        </text>
      )}
    </svg>
  )
}
