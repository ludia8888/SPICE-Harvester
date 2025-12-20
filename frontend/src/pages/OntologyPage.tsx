import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Button,
  Callout,
  Card,
  FormGroup,
  HTMLSelect,
  InputGroup,
  Intent,
  Text,
  TextArea,
} from '@blueprintjs/core'
import {
  createOntology,
  deleteOntology,
  getOntology,
  getOntologySchema,
  getSummary,
  listOntology,
  updateOntology,
  validateOntologyCreate,
  validateOntologyUpdate,
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
import { useAppStore } from '../store/useAppStore'

type OntologyItem = Record<string, unknown>

const buildNewOntologyDraft = (lang: string) => ({
  label: lang === 'ko' ? '새 클래스' : 'NewClass',
  description: '',
  properties: [],
  relationships: [],
  metadata: {},
})

const sanitizeUpdatePayload = (payload: Record<string, unknown>) => {
  const clone = { ...payload }
  delete clone.id
  delete clone.version
  delete clone.created_at
  delete clone.updated_at
  return clone
}

const coerceExpectedSeq = (value: unknown) =>
  typeof value === 'number' && Number.isFinite(value) ? value : null

export const OntologyPage = ({ dbName }: { dbName: string }) => {
  const queryClient = useQueryClient()
  const requestContext = useRequestContext()
  const language = useAppStore((state) => state.context.language)
  const branch = useAppStore((state) => state.context.branch)
  const adminToken = useAppStore((state) => state.adminToken)
  const adminMode = useAppStore((state) => state.adminMode)
  const registerCommand = useCommandRegistration()

  const [selectedClass, setSelectedClass] = useState<string | null>(null)
  const [draft, setDraft] = useState(() => JSON.stringify(buildNewOntologyDraft(language), null, 2))
  const [draftError, setDraftError] = useState<string | null>(null)
  const [validationResult, setValidationResult] = useState<unknown>(null)
  const [confirmAction, setConfirmAction] = useState<'apply' | 'delete' | null>(null)
  const [schemaFormat, setSchemaFormat] = useState<'json' | 'jsonld' | 'owl'>('json')

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
      ? qk.ontologySchema(dbName, selectedClass, branch, schemaFormat, requestContext.language)
      : ['ontology-schema', 'none'],
    queryFn: () =>
      getOntologySchema(requestContext, dbName, selectedClass ?? '', branch, schemaFormat),
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

  const selectedExpectedSeq = useMemo(() => {
    if (!detailQuery.data || typeof detailQuery.data !== 'object') {
      return null
    }
    return coerceExpectedSeq((detailQuery.data as { version?: number }).version)
  }, [detailQuery.data])

  const validateMutation = useMutation({
    mutationFn: async () => {
      if (!selectedClass) {
        return validateOntologyCreate(requestContext, dbName, branch, JSON.parse(draft))
      }
      const payload = sanitizeUpdatePayload(JSON.parse(draft))
      return validateOntologyUpdate(requestContext, dbName, selectedClass, branch, payload)
    },
    onSuccess: (result) => setValidationResult(result),
    onError: (error) => {
      setValidationResult(null)
      toastApiError(error, language)
    },
  })

  const applyMutation = useMutation({
    mutationFn: async (reason?: string) => {
      const parsed = JSON.parse(draft)
      if (!selectedClass) {
        return createOntology(requestContext, dbName, branch, parsed, reason ? { 'X-Change-Reason': reason } : undefined)
      }
      if (selectedExpectedSeq === null) {
        throw new Error('expected_seq missing')
      }
      const payload = sanitizeUpdatePayload(parsed)
      return updateOntology(
        requestContext,
        dbName,
        selectedClass,
        branch,
        selectedExpectedSeq,
        payload,
        reason ? { 'X-Change-Reason': reason } : undefined,
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

  const deleteMutation = useMutation({
    mutationFn: async (reason?: string) => {
      if (!selectedClass || selectedExpectedSeq === null) {
        throw new Error('Select a class first')
      }
      return deleteOntology(
        requestContext,
        dbName,
        selectedClass,
        branch,
        selectedExpectedSeq,
        reason ? { 'X-Change-Reason': reason } : undefined,
      )
    },
    onSuccess: (payload) => {
      const commandId = extractCommandId(payload)
      if (commandId) {
        registerCommand({
          commandId,
          kind: 'DELETE_ONTOLOGY',
          targetClassId: selectedClass ?? undefined,
          title: selectedClass ? `Delete ontology: ${selectedClass}` : 'Delete ontology',
        })
      }
      void showAppToast({ intent: Intent.WARNING, message: 'Delete submitted.' })
      void queryClient.invalidateQueries({ queryKey: qk.ontologyList(dbName, branch, requestContext.language) })
      setSelectedClass(null)
    },
    onError: (error) => toastApiError(error, language),
    onSettled: () => setConfirmAction(null),
  })

  const handleValidate = () => {
    setDraftError(null)
    try {
      JSON.parse(draft)
      validateMutation.mutate()
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
    applyMutation.mutate()
  }

  const handleDelete = () => {
    if (!selectedClass) {
      return
    }
    if (isProtected) {
      setConfirmAction('delete')
      return
    }
    deleteMutation.mutate()
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
                <Button small icon="tick-circle" onClick={handleValidate} loading={validateMutation.isPending}>
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
                  onClick={handleDelete}
                  loading={deleteMutation.isPending}
                  disabled={!selectedClass || (isProtected && (!adminMode || !adminToken))}
                >
                  Delete
                </Button>
              </div>
            </div>
            <FormGroup label="Class ID (optional)">
              <InputGroup value={selectedClass ?? ''} disabled placeholder="Auto-generated for new classes" />
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
              <Text>Schema export</Text>
              <div className="form-row">
                <HTMLSelect
                  value={schemaFormat}
                  options={[
                    { label: 'json', value: 'json' },
                    { label: 'jsonld', value: 'jsonld' },
                    { label: 'owl', value: 'owl' },
                  ]}
                  onChange={(event) => setSchemaFormat(event.currentTarget.value as typeof schemaFormat)}
                />
                <Button
                  icon="download"
                  onClick={() => void schemaQuery.refetch()}
                  disabled={!selectedClass}
                >
                  Load schema
                </Button>
              </div>
            </div>
            <JsonViewer value={schemaQuery.data} empty="Select a class to export schema." />
          </Card>
        </div>
      </div>

      <DangerConfirmDialog
        isOpen={Boolean(confirmAction)}
        title={confirmAction === 'delete' ? 'Delete class' : 'Apply ontology changes'}
        description={
          confirmAction === 'delete'
            ? 'This will delete the class on the protected branch.'
            : 'This will apply changes on the protected branch.'
        }
        confirmLabel={confirmAction === 'delete' ? 'Delete' : 'Apply'}
        cancelLabel="Cancel"
        confirmTextToType={selectedClass ?? 'class'}
        reasonLabel="Change reason"
        reasonPlaceholder="Why are you making this change?"
        typedLabel="Type class id to confirm"
        typedPlaceholder={selectedClass ?? 'class'}
        onCancel={() => setConfirmAction(null)}
        onConfirm={({ reason }) => {
          if (confirmAction === 'delete') {
            deleteMutation.mutate(reason)
          } else if (confirmAction === 'apply') {
            applyMutation.mutate(reason)
          }
        }}
        loading={applyMutation.isPending || deleteMutation.isPending}
        footerHint="Protected branches require audit context."
      />
    </div>
  )
}
