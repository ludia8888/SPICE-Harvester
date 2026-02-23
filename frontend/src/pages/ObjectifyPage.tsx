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
  ProgressBar,
  Spinner,
  Tab,
  Tabs,
  Tag,
  Tooltip,
} from '@blueprintjs/core'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { StepperBar } from '../components/StepperBar'
import { KeyValueEditor } from '../components/ux/KeyValueEditor'
import { StatusBadge } from '../components/ux/StatusBadge'
import { useRequestContext } from '../api/useRequestContext'
import { useAppStore } from '../store/useAppStore'
import type { RequestContext } from '../api/bff'
import {
  listPipelineDatasets,
  listObjectTypesV2,
  listObjectifyMappingSpecs,
  createObjectifyMappingSpec,
  runObjectifyDataset,
  runObjectifyDag,
  detectObjectifyRelationships,
  triggerIncrementalObjectify,
  getObjectifyWatermark,
  getTaskStatus,
} from '../api/bff'

/* ── query keys ─────────────────────────────────────── */
const objKeys = {
  datasets: (db: string, branch: string) => ['objectify', 'datasets', db, branch] as const,
  types: (db: string, branch: string) => ['objectify', 'types', db, branch] as const,
  specs: (db: string) => ['objectify', 'specs', db] as const,
}

/* ── page ────────────────────────────────────────────── */
export const ObjectifyPage = ({ dbName }: { dbName: string }) => {
  const ctx = useRequestContext()
  const branch = useAppStore((s) => s.context.branch)
  const queryClient = useQueryClient()

  /* state */
  const [selectedDataset, setSelectedDataset] = useState<string>('')
  const [targetType, setTargetType] = useState<string>('')
  const [runMode, setRunMode] = useState<'full' | 'incremental'>('full')
  const [activeTab, setActiveTab] = useState<string>('mapping')

  /* datasets */
  const datasetsQ = useQuery({
    queryKey: objKeys.datasets(dbName, branch),
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

  /* object types */
  const typesQ = useQuery({
    queryKey: objKeys.types(dbName, branch),
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

  /* mapping specs */
  const specsQ = useQuery({
    queryKey: objKeys.specs(dbName),
    queryFn: () => listObjectifyMappingSpecs(ctx, { db_name: dbName }),
  })
  const specs = (Array.isArray(specsQ.data) ? specsQ.data : []) as Record<string, unknown>[]

  /* mutations */
  const createSpecMut = useMutation({
    mutationFn: (input: Record<string, unknown>) => createObjectifyMappingSpec(ctx, input),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: objKeys.specs(dbName) }),
  })

  const runDatasetMut = useMutation({
    mutationFn: () => {
      if (!selectedDataset) throw new Error('Select a dataset')
      return runObjectifyDataset(ctx, selectedDataset, { dataset_version_id: 'latest' })
    },
  })

  const runDagMut = useMutation({
    mutationFn: () => runObjectifyDag(ctx, dbName),
  })

  const detectRelMut = useMutation({
    mutationFn: () => {
      if (!selectedDataset) throw new Error('Select a dataset')
      return detectObjectifyRelationships(ctx, dbName, selectedDataset)
    },
  })

  return (
    <div>
      <PageHeader
        title="Objectify"
        subtitle={`Transform datasets into ontology instances · ${dbName}`}
        actions={
          <div className="form-row">
            <Button
              icon="play"
              intent={Intent.SUCCESS}
              loading={runDagMut.isPending}
              onClick={() => runDagMut.mutate()}
            >
              Run DAG
            </Button>
            <Button
              icon="refresh"
              minimal
              onClick={() => {
                datasetsQ.refetch()
                typesQ.refetch()
                specsQ.refetch()
              }}
            >
              Refresh
            </Button>
          </div>
        }
      />

      {runDagMut.isSuccess && (
        <Callout intent={Intent.SUCCESS} icon="tick" style={{ marginBottom: 12 }}>
          DAG objectification started. {runDagMut.data?.task_id ? `Task: ${String(runDagMut.data.task_id)}` : ''}
        </Callout>
      )}
      {runDagMut.error && (
        <Callout intent={Intent.DANGER} style={{ marginBottom: 12 }}>DAG run failed.</Callout>
      )}

      <div className="two-col-grid">
        {/* Left: Steps */}
        <div className="card-stack">
          {/* Step 1: Select Dataset */}
          <Card>
            <div className="card-title">Step 1: Select Dataset</div>
            {datasetsQ.isLoading && <Spinner size={20} />}
            <FormGroup label="Source Dataset">
              <HTMLSelect
                value={selectedDataset}
                onChange={(e) => setSelectedDataset(e.target.value)}
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
          </Card>

          {/* Step 2: Target Type */}
          <Card>
            <div className="card-title">Step 2: Target Object Type</div>
            {typesQ.isLoading && <Spinner size={20} />}
            <FormGroup label="Object Type">
              <HTMLSelect
                value={targetType}
                onChange={(e) => setTargetType(e.target.value)}
                fill
                options={[
                  { value: '', label: '-- Select object type --' },
                  ...objectTypes.map((ot) => ({
                    value: String(ot.apiName ?? ot.name ?? ot.class_id ?? ''),
                    label: String(ot.apiName ?? ot.name ?? ot.class_id ?? 'Unknown'),
                  })),
                ]}
              />
            </FormGroup>
            <div className="form-row">
              <Button
                small
                icon="automatic-updates"
                loading={detectRelMut.isPending}
                disabled={!selectedDataset}
                onClick={() => detectRelMut.mutate()}
              >
                Detect Relationships
              </Button>
            </div>
            {detectRelMut.data && (
              <div style={{ marginTop: 8 }}>
                <JsonViewer value={detectRelMut.data} />
              </div>
            )}
          </Card>

          {/* Step 3: Execution */}
          <Card>
            <div className="card-title">Step 3: Execute</div>
            <FormGroup label="Mode">
              <HTMLSelect
                value={runMode}
                onChange={(e) => setRunMode(e.target.value as 'full' | 'incremental')}
                fill
                options={[
                  { value: 'full', label: 'Full (re-process all rows)' },
                  { value: 'incremental', label: 'Incremental (new rows only)' },
                ]}
              />
            </FormGroup>
            <div className="form-row">
              <Button
                intent={Intent.SUCCESS}
                icon="play"
                loading={runDatasetMut.isPending}
                disabled={!selectedDataset}
                onClick={() => runDatasetMut.mutate()}
              >
                Run Objectify
              </Button>
            </div>
            {runDatasetMut.isPending && (
              <div style={{ marginTop: 8 }}>
                <ProgressBar intent={Intent.SUCCESS} />
              </div>
            )}
            {runDatasetMut.isSuccess && (
              <Callout intent={Intent.SUCCESS} icon="tick" style={{ marginTop: 8 }}>
                Objectification started successfully.
              </Callout>
            )}
            {runDatasetMut.error && (
              <Callout intent={Intent.DANGER} style={{ marginTop: 8 }}>
                Objectification failed.
              </Callout>
            )}
          </Card>
        </div>

        {/* Right: Mapping Specs & Results */}
        <div className="card-stack">
          <Card>
            <Tabs selectedTabId={activeTab} onChange={(id) => setActiveTab(id as string)}>
              <Tab id="mapping" title="Mapping Specs" panel={
                <div>
                  {specsQ.isLoading && <Spinner size={20} />}
                  {specs.length === 0 && !specsQ.isLoading && (
                    <Callout>No mapping specifications yet. Create one to start objectifying data.</Callout>
                  )}
                  {specs.length > 0 && (
                    <HTMLTable compact striped style={{ width: '100%' }}>
                      <thead>
                        <tr>
                          <th>Mapping</th>
                          <th>Target Type</th>
                          <th>Dataset</th>
                          <th>Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {specs.map((spec, i) => (
                          <tr key={i}>
                            <td>{String(spec.mapping_spec_id ?? spec.id ?? `spec_${i}`)}</td>
                            <td><Tag minimal>{String(spec.target_class_id ?? spec.target_type ?? '—')}</Tag></td>
                            <td>{String(spec.dataset_id ?? spec.source ?? '—')}</td>
                            <td>
                              <Tag
                                intent={spec.status === 'active' ? Intent.SUCCESS : Intent.NONE}
                                minimal
                              >
                                {String(spec.status ?? 'draft')}
                              </Tag>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </HTMLTable>
                  )}
                </div>
              } />
              <Tab id="create" title="Create Mapping" panel={
                <MappingSpecCreator
                  ctx={ctx}
                  dbName={dbName}
                  datasets={datasets}
                  objectTypes={objectTypes}
                  selectedDataset={selectedDataset}
                  targetType={targetType}
                  onCreated={() => queryClient.invalidateQueries({ queryKey: objKeys.specs(dbName) })}
                />
              } />
              <Tab id="results" title="Results" panel={
                <div>
                  {runDatasetMut.data ? (
                    <JsonViewer value={runDatasetMut.data} />
                  ) : (
                    <Callout>Run objectification to see results here.</Callout>
                  )}
                </div>
              } />
            </Tabs>
          </Card>
        </div>
      </div>
    </div>
  )
}

/* ── Mapping Spec Creator ────────────────────────────── */
const MappingSpecCreator = ({
  ctx,
  dbName,
  datasets,
  objectTypes,
  selectedDataset,
  targetType,
  onCreated,
}: {
  ctx: RequestContext
  dbName: string
  datasets: Record<string, unknown>[]
  objectTypes: Record<string, unknown>[]
  selectedDataset: string
  targetType: string
  onCreated: () => void
}) => {
  const [mappingKv, setMappingKv] = useState<Record<string, string>>({})

  const createMut = useMutation({
    mutationFn: () => {
      const columnMappings = Object.entries(mappingKv)
        .filter(([k]) => k.trim())
        .map(([source, target]) => ({ source, target }))
      return createObjectifyMappingSpec(ctx, {
        db_name: dbName,
        dataset_id: selectedDataset || undefined,
        target_class_id: targetType || undefined,
        column_mappings: columnMappings,
      })
    },
    onSuccess: onCreated,
  })

  return (
    <div>
      <FormGroup label="Source Dataset">
        <InputGroup value={selectedDataset || '(select from Step 1)'} disabled />
      </FormGroup>
      <FormGroup label="Target Object Type">
        <InputGroup value={targetType || '(select from Step 2)'} disabled />
      </FormGroup>
      <FormGroup label={
        <Tooltip content="Map source dataset columns to target object type properties" placement="top">
          <span className="tooltip-label">Column Mappings</span>
        </Tooltip>
      }>
        <KeyValueEditor
          value={mappingKv}
          onChange={setMappingKv}
          keyPlaceholder="Source column (e.g. order_id)"
          valuePlaceholder="Target property (e.g. orderId)"
          addLabel="Add column mapping"
        />
      </FormGroup>
      <Button
        intent={Intent.PRIMARY}
        icon="plus"
        loading={createMut.isPending}
        disabled={!selectedDataset || !targetType}
        onClick={() => createMut.mutate()}
      >
        Create Mapping Spec
      </Button>
      {createMut.error && <Callout intent={Intent.DANGER} style={{ marginTop: 8 }}>Failed to create mapping.</Callout>}
      {createMut.isSuccess && <Callout intent={Intent.SUCCESS} style={{ marginTop: 8 }}>Mapping created.</Callout>}
    </div>
  )
}
