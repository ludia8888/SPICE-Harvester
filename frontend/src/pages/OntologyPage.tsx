import { useEffect, useMemo, useState } from 'react'
import { useParams } from 'react-router-dom'
import { useMutation, useQuery } from '@tanstack/react-query'
import {
  Button,
  Callout,
  Card,
  FormGroup,
  H5,
  HTMLSelect,
  InputGroup,
  Intent,
  Tab,
  Tabs,
  TextArea,
} from '@blueprintjs/core'
import {
  createOntology,
  deleteOntology,
  getOntology,
  getOntologySchema,
  getSummary,
  updateOntology,
  validateOntology,
  validateOntologyUpdate,
} from '../api/bff'
import { PageHeader } from '../components/PageHeader'
import { JsonView } from '../components/JsonView'
import { useOntologyRegistry, type OntologyClass } from '../hooks/useOntologyRegistry'
import { showAppToast } from '../app/AppToaster'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { useAppStore } from '../store/useAppStore'
import { formatLabel } from '../utils/labels'
import { extractOccActualSeq } from '../utils/occ'

type DraftProperty = {
  name: string
  type: string
  label: string
  required: boolean
  primary_key: boolean
}

type DraftRelationship = {
  predicate: string
  target: string
  label: string
  cardinality: string
  inverse_predicate?: string
  inverse_label?: string
}

type OntologyDraft = {
  id: string
  label: string
  description?: string
  parent_class?: string
  abstract?: boolean
  properties: DraftProperty[]
  relationships: DraftRelationship[]
}

const buildDraftFromClass = (klass: OntologyClass, lang: string): OntologyDraft => ({
  id: klass.id ?? '',
  label: formatLabel(klass.label, lang, klass.id ?? ''),
  description: formatLabel(klass.description, lang, ''),
  parent_class: (klass.parent_class as string) ?? undefined,
  abstract: Boolean(klass.abstract),
  properties:
    klass.properties?.map((prop) => ({
      name: prop.name ?? '',
      type: prop.type ?? 'xsd:string',
      label: formatLabel(prop.label, lang, prop.name ?? ''),
      required: Boolean(prop.required),
      primary_key: Boolean((prop as any).primary_key || (prop as any).primaryKey),
    })) ?? [],
  relationships:
    klass.relationships?.map((rel) => ({
      predicate: rel.predicate ?? '',
      target: rel.target ?? '',
      label: formatLabel(rel.label, lang, rel.predicate ?? ''),
      cardinality: rel.cardinality ?? '1:n',
      inverse_predicate: (rel as any).inverse_predicate ?? undefined,
      inverse_label: formatLabel((rel as any).inverse_label, lang, ''),
    })) ?? [],
})

const emptyDraft = (): OntologyDraft => ({
  id: '',
  label: '',
  description: '',
  parent_class: '',
  abstract: false,
  properties: [],
  relationships: [],
})

export const OntologyPage = () => {
  const { db } = useParams()
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)
  const trackCommand = useAppStore((state) => state.trackCommand)

  const requestContext = useMemo(
    () => ({ language: context.language, authToken, adminToken }),
    [adminToken, authToken, context.language],
  )

  const summaryQuery = useQuery({
    queryKey: qk.summary({ dbName: db ?? null, branch: context.branch, language: context.language }),
    queryFn: () => getSummary(requestContext, { dbName: db ?? null, branch: context.branch }),
    enabled: Boolean(db),
  })

  const isProtected = Boolean((summaryQuery.data as any)?.data?.policy?.is_protected_branch)

  const registry = useOntologyRegistry(db, context.branch)
  const [selectedClassId, setSelectedClassId] = useState<string>('')
  const [search, setSearch] = useState('')
  const [draft, setDraft] = useState<OntologyDraft>(emptyDraft())
  const [tab, setTab] = useState('properties')
  const [validationResult, setValidationResult] = useState<unknown>(null)
  const [schemaFormat, setSchemaFormat] = useState('json')
  const [schemaResult, setSchemaResult] = useState<unknown>(null)
  const [changeReason, setChangeReason] = useState('')
  const [adminActor, setAdminActor] = useState('')
  const [actualSeqHint, setActualSeqHint] = useState<number | null>(null)

  const detailQuery = useQuery({
    queryKey:
      db && selectedClassId
        ? qk.ontologyDetail({ dbName: db, classLabel: selectedClassId, branch: context.branch, language: context.language })
        : ['bff', 'ontology-detail', 'empty'],
    queryFn: () => getOntology(requestContext, db ?? '', selectedClassId, context.branch),
    enabled: Boolean(db && selectedClassId),
  })

  useEffect(() => {
    if (detailQuery.data && selectedClassId) {
      setDraft(buildDraftFromClass(detailQuery.data as OntologyClass, context.language))
      setSchemaResult(null)
      setValidationResult(null)
      setActualSeqHint(null)
      return
    }
    if (!selectedClassId) {
      setDraft(emptyDraft())
    }
  }, [context.language, detailQuery.data, selectedClassId])

  const expectedSeq = (detailQuery.data as any)?.version ?? (detailQuery.data as any)?.expected_seq

  const buildPayload = (): unknown => ({
    id: draft.id,
    label: draft.label,
    description: draft.description || undefined,
    parent_class: draft.parent_class || undefined,
    abstract: Boolean(draft.abstract),
    properties: draft.properties.map((prop) => ({
      name: prop.name,
      type: prop.type,
      label: prop.label,
      required: Boolean(prop.required),
      primary_key: Boolean(prop.primary_key),
    })),
    relationships: draft.relationships.map((rel) => ({
      predicate: rel.predicate,
      target: rel.target,
      label: rel.label,
      cardinality: rel.cardinality,
      inverse_predicate: rel.inverse_predicate || undefined,
      inverse_label: rel.inverse_label || undefined,
    })),
  })

  const adminActorValue = adminActor.trim()
  const extraHeaders = isProtected
    ? {
        'X-Change-Reason': changeReason.trim(),
        ...(adminActorValue ? { 'X-Admin-Actor': adminActorValue } : {}),
      }
    : undefined

  const canApplyProtected = !isProtected || (changeReason.trim() && adminToken)

  const applyMutation = useMutation({
    mutationFn: async (params?: { expectedSeqOverride?: number }) => {
      const payload = buildPayload()
      if (!db) {
        throw new Error('Missing db')
      }
      if (selectedClassId) {
        const seq = params?.expectedSeqOverride ?? (typeof expectedSeq === 'number' ? expectedSeq : actualSeqHint)
        if (typeof seq !== 'number') {
          throw new Error('expected_seq missing')
        }
        return updateOntology(requestContext, db, selectedClassId, context.branch, seq, payload, extraHeaders)
      }
      return createOntology(requestContext, db, context.branch, payload, extraHeaders)
    },
    onSuccess: (result) => {
      if (!db) {
        return
      }
      const commandId = (result as any)?.commandId
      if (commandId) {
        trackCommand({
          id: commandId,
          kind: 'ONTOLOGY_APPLY',
          title: selectedClassId ? `Update ${selectedClassId}` : `Create ${draft.id}`,
          target: { dbName: db },
          context: { project: db, branch: context.branch },
          submittedAt: new Date().toISOString(),
          writePhase: 'SUBMITTED',
          indexPhase: 'UNKNOWN',
        })
      }
      void registry.refetch()
      void showAppToast({ intent: Intent.SUCCESS, message: '온톨로지 적용 요청 완료' })
    },
    onError: (error) => {
      const actualSeq = extractOccActualSeq(error)
      if (actualSeq !== null) {
        setActualSeqHint(actualSeq)
      }
      toastApiError(error, context.language)
    },
  })

  const deleteMutation = useMutation({
    mutationFn: async (params?: { expectedSeqOverride?: number }) => {
      if (!db || !selectedClassId) {
        throw new Error('Missing class')
      }
      const seq = params?.expectedSeqOverride ?? (typeof expectedSeq === 'number' ? expectedSeq : actualSeqHint)
      if (typeof seq !== 'number') {
        throw new Error('expected_seq missing')
      }
      return deleteOntology(requestContext, db, selectedClassId, context.branch, seq, extraHeaders)
    },
    onSuccess: (result) => {
      if (!db) {
        return
      }
      const commandId = (result as any)?.commandId
      if (commandId) {
        trackCommand({
          id: commandId,
          kind: 'ONTOLOGY_DELETE',
          title: `Delete ${selectedClassId}`,
          target: { dbName: db },
          context: { project: db, branch: context.branch },
          submittedAt: new Date().toISOString(),
          writePhase: 'SUBMITTED',
          indexPhase: 'UNKNOWN',
        })
      }
      setSelectedClassId('')
      void registry.refetch()
      void showAppToast({ intent: Intent.SUCCESS, message: '삭제 요청 전송 완료' })
    },
    onError: (error) => {
      const actualSeq = extractOccActualSeq(error)
      if (actualSeq !== null) {
        setActualSeqHint(actualSeq)
      }
      toastApiError(error, context.language)
    },
  })

  const validateMutation = useMutation({
    mutationFn: async () => {
      const payload = buildPayload()
      if (!db) {
        throw new Error('Missing db')
      }
      if (selectedClassId) {
        return validateOntologyUpdate(requestContext, db, selectedClassId, context.branch, payload)
      }
      return validateOntology(requestContext, db, context.branch, payload)
    },
    onSuccess: (result) => setValidationResult(result),
    onError: (error) => toastApiError(error, context.language),
  })

  const schemaMutation = useMutation({
    mutationFn: async () => {
      if (!db || !selectedClassId) {
        throw new Error('Missing class')
      }
      return getOntologySchema(requestContext, db, selectedClassId, context.branch, schemaFormat)
    },
    onSuccess: (result) => setSchemaResult(result),
    onError: (error) => toastApiError(error, context.language),
  })

  const filteredClasses = useMemo(() => {
    const term = search.trim().toLowerCase()
    if (!term) {
      return registry.classOptions
    }
    return registry.classOptions.filter(
      (option) =>
        option.label.toLowerCase().includes(term) || option.value.toLowerCase().includes(term),
    )
  }, [registry.classOptions, search])

  return (
    <div>
      <PageHeader title="Ontology" subtitle="클래스, 속성, 관계를 정의합니다." />
      {isProtected ? (
        <Callout intent={Intent.WARNING} title="Protected branch">
          보호 브랜치에서 변경하려면 Admin token과 변경 사유가 필요합니다.
        </Callout>
      ) : null}

      <div className="ontology-layout">
        <Card elevation={1} className="section-card">
          <div className="card-title">
            <H5>Classes</H5>
            <Button minimal icon="add" onClick={() => setSelectedClassId('')}>
              New
            </Button>
          </div>
          <InputGroup
            placeholder="Search class"
            value={search}
            onChange={(event) => setSearch(event.currentTarget.value)}
          />
          <div className="list-scroll">
            {filteredClasses.map((option) => (
              <Button
                key={option.value}
                minimal
                className={selectedClassId === option.value ? 'list-item active' : 'list-item'}
                onClick={() => setSelectedClassId(option.value)}
              >
                {option.label}
              </Button>
            ))}
          </div>
        </Card>

        <Card elevation={1} className="section-card">
          <div className="card-title">
            <H5>Editor</H5>
            <div className="muted small">expected_seq: {expectedSeq ?? 'n/a'}</div>
          </div>
          <div className="form-grid">
            <FormGroup label="Class ID">
              <InputGroup
                value={draft.id}
                onChange={(event) => setDraft((prev) => ({ ...prev, id: event.currentTarget.value }))}
              />
            </FormGroup>
            <FormGroup label="Label">
              <InputGroup
                value={draft.label}
                onChange={(event) => setDraft((prev) => ({ ...prev, label: event.currentTarget.value }))}
              />
            </FormGroup>
            <FormGroup label="Description">
              <TextArea
                value={draft.description ?? ''}
                onChange={(event) => setDraft((prev) => ({ ...prev, description: event.currentTarget.value }))}
              />
            </FormGroup>
            <FormGroup label="Parent class">
              <HTMLSelect
                value={draft.parent_class ?? ''}
                onChange={(event) => setDraft((prev) => ({ ...prev, parent_class: event.currentTarget.value }))}
                options={[{ label: 'None', value: '' }, ...registry.classOptions.map((item) => ({ label: item.label, value: item.value }))]}
              />
            </FormGroup>
            <FormGroup label="Abstract">
              <HTMLSelect
                value={draft.abstract ? 'true' : 'false'}
                onChange={(event) =>
                  setDraft((prev) => ({ ...prev, abstract: event.currentTarget.value === 'true' }))
                }
                options={[
                  { label: 'false', value: 'false' },
                  { label: 'true', value: 'true' },
                ]}
              />
            </FormGroup>
          </div>

          <Tabs id="ontology-tabs" selectedTabId={tab} onChange={(value) => setTab(value as string)}>
            <Tab id="properties" title="Properties" />
            <Tab id="relationships" title="Relationships" />
            <Tab id="validate" title="Validate" />
            <Tab id="export" title="Export" />
          </Tabs>

          {tab === 'properties' ? (
            <div className="table-editor">
              <Button
                minimal
                icon="add"
                onClick={() =>
                  setDraft((prev) => ({
                    ...prev,
                    properties: [
                      ...prev.properties,
                      { name: '', type: 'xsd:string', label: '', required: false, primary_key: false },
                    ],
                  }))
                }
              >
                Add property
              </Button>
              {draft.properties.map((prop, index) => (
                <div key={`${prop.name}-${index}`} className="row-grid">
                  <InputGroup
                    placeholder="name"
                    value={prop.name}
                    onChange={(event) => {
                      const value = event.currentTarget.value
                      setDraft((prev) => {
                        const next = [...prev.properties]
                        next[index] = { ...next[index], name: value }
                        return { ...prev, properties: next }
                      })
                    }}
                  />
                  <InputGroup
                    placeholder="type"
                    value={prop.type}
                    onChange={(event) => {
                      const value = event.currentTarget.value
                      setDraft((prev) => {
                        const next = [...prev.properties]
                        next[index] = { ...next[index], type: value }
                        return { ...prev, properties: next }
                      })
                    }}
                  />
                  <InputGroup
                    placeholder="label"
                    value={prop.label}
                    onChange={(event) => {
                      const value = event.currentTarget.value
                      setDraft((prev) => {
                        const next = [...prev.properties]
                        next[index] = { ...next[index], label: value }
                        return { ...prev, properties: next }
                      })
                    }}
                  />
                  <HTMLSelect
                    value={prop.required ? 'true' : 'false'}
                    onChange={(event) => {
                      const value = event.currentTarget.value === 'true'
                      setDraft((prev) => {
                        const next = [...prev.properties]
                        next[index] = { ...next[index], required: value }
                        return { ...prev, properties: next }
                      })
                    }}
                    options={[
                      { label: 'required: false', value: 'false' },
                      { label: 'required: true', value: 'true' },
                    ]}
                  />
                  <HTMLSelect
                    value={prop.primary_key ? 'true' : 'false'}
                    onChange={(event) => {
                      const value = event.currentTarget.value === 'true'
                      setDraft((prev) => {
                        const next = [...prev.properties]
                        next[index] = { ...next[index], primary_key: value }
                        return { ...prev, properties: next }
                      })
                    }}
                    options={[
                      { label: 'pk: false', value: 'false' },
                      { label: 'pk: true', value: 'true' },
                    ]}
                  />
                  <Button
                    minimal
                    icon="trash"
                    intent={Intent.DANGER}
                    onClick={() =>
                      setDraft((prev) => ({
                        ...prev,
                        properties: prev.properties.filter((_, i) => i !== index),
                      }))
                    }
                  />
                </div>
              ))}
            </div>
          ) : null}

          {tab === 'relationships' ? (
            <div className="table-editor">
              <Button
                minimal
                icon="add"
                onClick={() =>
                  setDraft((prev) => ({
                    ...prev,
                    relationships: [
                      ...prev.relationships,
                      { predicate: '', target: '', label: '', cardinality: '1:n' },
                    ],
                  }))
                }
              >
                Add relationship
              </Button>
              {draft.relationships.map((rel, index) => (
                <div key={`${rel.predicate}-${index}`} className="row-grid">
                  <InputGroup
                    placeholder="predicate"
                    value={rel.predicate}
                    onChange={(event) => {
                      const value = event.currentTarget.value
                      setDraft((prev) => {
                        const next = [...prev.relationships]
                        next[index] = { ...next[index], predicate: value }
                        return { ...prev, relationships: next }
                      })
                    }}
                  />
                  <InputGroup
                    placeholder="target"
                    value={rel.target}
                    onChange={(event) => {
                      const value = event.currentTarget.value
                      setDraft((prev) => {
                        const next = [...prev.relationships]
                        next[index] = { ...next[index], target: value }
                        return { ...prev, relationships: next }
                      })
                    }}
                  />
                  <InputGroup
                    placeholder="label"
                    value={rel.label}
                    onChange={(event) => {
                      const value = event.currentTarget.value
                      setDraft((prev) => {
                        const next = [...prev.relationships]
                        next[index] = { ...next[index], label: value }
                        return { ...prev, relationships: next }
                      })
                    }}
                  />
                  <HTMLSelect
                    value={rel.cardinality}
                    onChange={(event) => {
                      const value = event.currentTarget.value
                      setDraft((prev) => {
                        const next = [...prev.relationships]
                        next[index] = { ...next[index], cardinality: value }
                        return { ...prev, relationships: next }
                      })
                    }}
                    options={['1:1', '1:n', 'n:1', 'n:m']}
                  />
                  <Button
                    minimal
                    icon="trash"
                    intent={Intent.DANGER}
                    onClick={() =>
                      setDraft((prev) => ({
                        ...prev,
                        relationships: prev.relationships.filter((_, i) => i !== index),
                      }))
                    }
                  />
                </div>
              ))}
            </div>
          ) : null}

          {tab === 'validate' ? (
            <div>
              <Button
                icon="endorsed"
                intent={Intent.PRIMARY}
                onClick={() => validateMutation.mutate()}
                loading={validateMutation.isPending}
              >
                Validate
              </Button>
              <JsonView value={validationResult} />
            </div>
          ) : null}

          {tab === 'export' ? (
            <div>
              <div className="form-row">
                <HTMLSelect
                  value={schemaFormat}
                  onChange={(event) => setSchemaFormat(event.currentTarget.value)}
                  options={[
                    { label: 'json', value: 'json' },
                    { label: 'jsonld', value: 'jsonld' },
                    { label: 'owl', value: 'owl' },
                  ]}
                />
                <Button
                  icon="download"
                  onClick={() => schemaMutation.mutate()}
                  disabled={!selectedClassId}
                  loading={schemaMutation.isPending}
                >
                  Export Schema
                </Button>
              </div>
              <JsonView value={schemaResult} />
            </div>
          ) : null}

          <div className="action-row">
            {isProtected ? (
              <div className="protected-form">
                <FormGroup label="Change reason">
                  <InputGroup value={changeReason} onChange={(event) => setChangeReason(event.currentTarget.value)} />
                </FormGroup>
                <FormGroup label="Admin actor (optional)">
                  <InputGroup value={adminActor} onChange={(event) => setAdminActor(event.currentTarget.value)} />
                </FormGroup>
              </div>
            ) : null}

            {actualSeqHint !== null ? (
              <Callout intent={Intent.WARNING} title="OCC Conflict">
                최신 seq: {actualSeqHint} — 재시도 시 적용됩니다.
                <div className="button-row">
                  <Button
                    minimal
                    icon="refresh"
                    onClick={() => applyMutation.mutate({ expectedSeqOverride: actualSeqHint })}
                    disabled={!selectedClassId || !canApplyProtected}
                  >
                    Use actual_seq and retry apply
                  </Button>
                  <Button
                    minimal
                    icon="refresh"
                    intent={Intent.DANGER}
                    onClick={() => deleteMutation.mutate({ expectedSeqOverride: actualSeqHint })}
                    disabled={!selectedClassId || !canApplyProtected}
                  >
                    Use actual_seq and retry delete
                  </Button>
                </div>
              </Callout>
            ) : null}

            <div className="button-row">
              <Button
                intent={Intent.PRIMARY}
                icon="floppy-disk"
                onClick={() => applyMutation.mutate(undefined)}
                loading={applyMutation.isPending}
                disabled={!draft.id || !draft.label || !canApplyProtected}
              >
                Apply (202)
              </Button>
              <Button
                intent={Intent.DANGER}
                icon="trash"
                onClick={() => deleteMutation.mutate(undefined)}
                loading={deleteMutation.isPending}
                disabled={!selectedClassId || !canApplyProtected}
              >
                Delete (202)
              </Button>
            </div>
          </div>
        </Card>

        <Card elevation={1} className="section-card">
          <div className="card-title">
            <H5>Policy</H5>
          </div>
          <JsonView value={(summaryQuery.data as any)?.data?.policy} />
        </Card>
      </div>
    </div>
  )
}
