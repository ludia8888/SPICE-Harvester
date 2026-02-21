import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Button,
  Callout,
  Card,
  FormGroup,
  InputGroup,
  Intent,
  Text,
  TextArea,
} from '@blueprintjs/core'
import { useAppStore as useAppStoreNew } from '../store/useAppStore'
import {
  createObjectTypeV2,
  getObjectTypeFullMetadataV2,
  getOntology,
  getSummary,
  listOntology,
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
   BFF-aligned Ontology page (new – RequestContext based)
   ═══════════════════════════════════════════════════════════════════════════ */

type OntologyItem = Record<string, unknown>

const buildNewOntologyDraft = (lang: string) => ({
  id: '',
  label: lang === 'ko' ? '새 클래스' : 'NewClass',
  description: '',
  properties: [],
  metadata: {},
})

const asRecord = (value: unknown): Record<string, unknown> | null =>
  value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : null

const toStringValue = (value: unknown) =>
  typeof value === 'string' ? value.trim() : (typeof value === 'number' ? String(value) : '')

const normalizeApiName = (value: string) =>
  value
    .trim()
    .replace(/[^a-zA-Z0-9_]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '')

const toObjectArray = (value: unknown) =>
  Array.isArray(value)
    ? value
        .map((item) => asRecord(item))
        .filter((item): item is Record<string, unknown> => Boolean(item))
    : []

type ObjectTypeContractDraft = {
  apiName: string
  createInput: Parameters<typeof createObjectTypeV2>[2]
  updateInput: Parameters<typeof updateObjectTypeV2>[3]
}

const buildObjectTypeContractDraft = (
  payload: Record<string, unknown>,
  fallbackClassId?: string | null,
): ObjectTypeContractDraft => {
  const draftApiName = normalizeApiName(
    toStringValue(payload.id) ||
      toStringValue(payload.apiName) ||
      toStringValue(payload['@id']) ||
      toStringValue(payload.label) ||
      toStringValue(fallbackClassId),
  )
  if (!draftApiName) {
    throw new Error('Class ID is required. Set "id" (or "apiName") in the draft.')
  }

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
    draftProperties.find((property) => property.primaryKey && property.name) ??
    draftProperties.find((property) => property.required && property.name) ??
    draftProperties.find((property) => property.name)
  const titleProperty =
    draftProperties.find((property) => property.titleKey && property.name) ??
    primaryProperty
  const draftMetadata = asRecord(payload.metadata) ?? {}

  const metadata: Record<string, unknown> = {
    ...draftMetadata,
    displayName: draftLabel,
    description: draftDescription,
    properties: draftProperties,
    source: 'frontend_ontology_editor_v2',
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

  const explicitPkSpec = asRecord(payload.pkSpec)
  if (explicitPkSpec) {
    createInput.pkSpec = explicitPkSpec
    updateInput.pkSpec = explicitPkSpec
  }

  const backingSource = asRecord(payload.backingSource)
  if (backingSource) {
    createInput.backingSource = backingSource
    updateInput.backingSource = backingSource
  }

  const backingSources = toObjectArray(payload.backingSources)
  if (backingSources.length > 0) {
    createInput.backingSources = backingSources
    updateInput.backingSources = backingSources
  }

  const mappingSpecId = toStringValue(payload.mappingSpecId)
  if (mappingSpecId) {
    createInput.mappingSpecId = mappingSpecId
    updateInput.mappingSpecId = mappingSpecId
  }

  const mappingSpecVersion = Number(payload.mappingSpecVersion)
  if (Number.isFinite(mappingSpecVersion)) {
    createInput.mappingSpecVersion = mappingSpecVersion
    updateInput.mappingSpecVersion = mappingSpecVersion
  }

  const backingDatasetId = toStringValue(payload.backingDatasetId)
  if (backingDatasetId) {
    createInput.backingDatasetId = backingDatasetId
    updateInput.backingDatasetId = backingDatasetId
  }

  const backingDatasourceId = toStringValue(payload.backingDatasourceId)
  if (backingDatasourceId) {
    createInput.backingDatasourceId = backingDatasourceId
    updateInput.backingDatasourceId = backingDatasourceId
  }

  const backingDatasourceVersionId = toStringValue(payload.backingDatasourceVersionId)
  if (backingDatasourceVersionId) {
    createInput.backingDatasourceVersionId = backingDatasourceVersionId
    updateInput.backingDatasourceVersionId = backingDatasourceVersionId
  }

  const datasetVersionId = toStringValue(payload.datasetVersionId)
  if (datasetVersionId) {
    createInput.datasetVersionId = datasetVersionId
    updateInput.datasetVersionId = datasetVersionId
  }

  const requiresDatasetVersion = Boolean(
    mappingSpecId ||
      backingDatasetId ||
      backingDatasourceId ||
      backingDatasourceVersionId ||
      backingSource ||
      backingSources.length > 0,
  )
  if (requiresDatasetVersion && !datasetVersionId) {
    throw new Error(
      'datasetVersionId is required when mapping/backing source metadata is provided.',
    )
  }

  return {
    apiName: draftApiName,
    createInput,
    updateInput,
  }
}

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

  const listQuery = useQuery({
    queryKey: qk.ontologyList(dbName, branch, requestContext.language),
    queryFn: () => listOntology(requestContext, dbName, branch),
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

  const schemaQuery = useQuery({
    queryKey: selectedClass
      ? qk.ontologySchema(dbName, selectedClass, branch, 'fullMetadata', requestContext.language)
      : ['ontology-schema', 'none'],
    queryFn: () =>
      getObjectTypeFullMetadataV2(requestContext, dbName, selectedClass ?? '', { branch }),
    enabled: false,
  })

  const isProtected = Boolean(
    (summaryQuery.data as { data?: { policy?: { is_protected_branch?: boolean } } } | undefined)?.data
      ?.policy?.is_protected_branch,
  )

  useEffect(() => {
    if (!selectedClass) {
      const next = buildNewOntologyDraft(language)
      setDraft(JSON.stringify(next, null, 2))
      return
    }
    if (detailQuery.data) {
      setDraft(JSON.stringify(detailQuery.data, null, 2))
    }
  }, [detailQuery.data, language, selectedClass])

  const ontologies = useMemo(() => {
    const payload = listQuery.data ?? {}
    const list = (payload as { ontologies?: OntologyItem[]; data?: { ontologies?: OntologyItem[] } })
    return list.ontologies ?? list.data?.ontologies ?? []
  }, [listQuery.data])

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
    onError: (error) => {
      setValidationResult(null)
      toastApiError(error, language)
    },
  })

  const applyMutation = useMutation({
    mutationFn: async () => {
      const parsed = JSON.parse(draft) as Record<string, unknown>
      const normalized = buildObjectTypeContractDraft(parsed, selectedClass)
      if (!selectedClass) {
        return createObjectTypeV2(requestContext, dbName, normalized.createInput, { branch })
      }
      if (normalized.apiName !== selectedClass) {
        throw new Error('Object type rename is not supported. Keep "id" equal to selected class.')
      }
      return updateObjectTypeV2(
        requestContext,
        dbName,
        selectedClass,
        normalized.updateInput,
        { branch },
      )
    },
    onSuccess: (payload) => {
      const commandId = extractCommandId(payload)
      let targetClassId = selectedClass ?? undefined
      if (!targetClassId) {
        try {
          const parsed = JSON.parse(draft) as { id?: string; '@id'?: string; label?: unknown }
          targetClassId = String(parsed.id ?? parsed['@id'] ?? '').trim() || undefined
        } catch {
          targetClassId = undefined
        }
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
    try {
      JSON.parse(draft)
      validateMutation.mutate(undefined)
    } catch (error) {
      setDraftError('Invalid JSON')
    }
  }

  const handleApply = () => {
    setDraftError(null)
    try {
      JSON.parse(draft)
    } catch {
      setDraftError('Invalid JSON')
      return
    }
    if (isProtected) {
      setConfirmAction('apply')
      return
    }
    applyMutation.mutate(undefined)
  }

  return (
    <div>
      <PageHeader title="Ontology" subtitle={`Branch: ${branch}`} />

      {isProtected ? (
        <Callout intent={Intent.WARNING} style={{ marginBottom: 12 }}>
          Protected branch. Admin token + Admin mode required for apply/delete.
        </Callout>
      ) : null}

      <div className="page-grid two-col">
        <Card>
          <div className="card-title">
            <Text>Classes</Text>
            <Button
              small
              icon="plus"
              onClick={() => setSelectedClass(null)}
            >
              New
            </Button>
          </div>
          {listQuery.isFetching ? <Text className="muted small">Loading...</Text> : null}
          {ontologies.length === 0 ? (
            <Text className="muted">No classes found.</Text>
          ) : (
            <div className="nav-list">
              {ontologies.map((item) => {
                const id = (item.id as string | undefined) || (item['@id'] as string | undefined)
                const label = (item.label as string | undefined) || id || 'unknown'
                const active = id && id === selectedClass
                return (
                  <button
                    key={id ?? label}
                    className={`nav-item ${active ? 'is-active' : ''}`}
                    onClick={() => setSelectedClass(id ?? label)}
                  >
                    {label}
                  </button>
                )
              })}
            </div>
          )}
        </Card>

        <div className="card-stack">
          <Card>
            <div className="card-title">
              <Text>Editor</Text>
              <div className="form-row">
                <Button
                  small
                  icon="tick-circle"
                  onClick={handleValidate}
                  loading={validateMutation.isPending}
                >
                  Validate
                </Button>
                <Button
                  small
                  intent={Intent.PRIMARY}
                  icon="cloud-upload"
                  onClick={handleApply}
                  loading={applyMutation.isPending}
                  disabled={isProtected && (!adminMode || !adminToken)}
                >
                  Apply
                </Button>
                <Button
                  small
                  intent={Intent.DANGER}
                  icon="trash"
                  disabled
                >
                  Delete
                </Button>
              </div>
            </div>
            <Callout intent={Intent.PRIMARY} style={{ marginBottom: 12 }}>
              Create/update writes are handled by Foundry v2 object type contract APIs. Delete remains disabled until v2 delete endpoint is available.
            </Callout>
            <FormGroup label="Selected class">
              <InputGroup value={selectedClass ?? '(new class)'} disabled />
            </FormGroup>
            <FormGroup label="JSON payload">
              <TextArea
                value={draft}
                onChange={(event) => setDraft(event.currentTarget.value)}
                rows={14}
              />
            </FormGroup>
            {draftError ? <Callout intent={Intent.DANGER}>{draftError}</Callout> : null}
          </Card>

          <Card>
            <div className="card-title">Validation</div>
            <JsonViewer value={validationResult} empty="Run validation to see results." />
          </Card>

          <Card>
            <div className="card-title">
              <Text>Object Type Metadata</Text>
              <div className="form-row">
                <Button
                  icon="download"
                  onClick={() => void schemaQuery.refetch()}
                  disabled={!selectedClass}
                >
                  Load metadata
                </Button>
              </div>
            </div>
            <JsonViewer value={schemaQuery.data} empty="Select a class to load full metadata." />
          </Card>
        </div>
      </div>

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
        onConfirm={() => {
          if (confirmAction === 'apply') {
            applyMutation.mutate(undefined)
          }
        }}
        loading={applyMutation.isPending}
        footerHint="Protected branches require audit context."
      />
    </div>
  )
}
