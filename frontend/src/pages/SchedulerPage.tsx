import { useState, useMemo } from 'react'
import {
  Button,
  Card,
  Callout,
  Dialog,
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
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { CronBuilder } from '../components/ux/CronBuilder'
import { KeyValueEditor } from '../components/ux/KeyValueEditor'
import { StatusBadge } from '../components/ux/StatusBadge'
import { useRequestContext } from '../api/useRequestContext'
import {
  createOrchestrationBuild,
  getOrchestrationBuild,
  cancelOrchestrationBuild,
  listOrchestrationBuildJobs,
  createOrchestrationSchedule,
  getOrchestrationSchedule,
  deleteOrchestrationSchedule,
  pauseOrchestrationSchedule,
  unpauseOrchestrationSchedule,
  listOrchestrationScheduleRuns,
} from '../api/bff'

/* ── page ────────────────────────────────────────────── */
export const SchedulerPage = () => {
  const ctx = useRequestContext()
  const queryClient = useQueryClient()
  const [activeTab, setActiveTab] = useState('builds')

  return (
    <div>
      <PageHeader
        title="Scheduler"
        subtitle="Orchestration builds & schedule management"
      />

      <Tabs
        selectedTabId={activeTab}
        onChange={(id) => setActiveTab(id as string)}
        large
      >
        <Tab id="builds" title="Builds" panel={
          <BuildsTab ctx={ctx} queryClient={queryClient} />
        } />
        <Tab id="schedules" title="Schedules" panel={
          <SchedulesTab ctx={ctx} queryClient={queryClient} />
        } />
      </Tabs>
    </div>
  )
}

/* ── Builds Tab ─────────────────────────────────────── */
const BuildsTab = ({
  ctx,
  queryClient,
}: {
  ctx: ReturnType<typeof useRequestContext>
  queryClient: ReturnType<typeof useQueryClient>
}) => {
  const [createOpen, setCreateOpen] = useState(false)
  const [buildTarget, setBuildTarget] = useState('')
  const [buildType, setBuildType] = useState('INCREMENTAL')
  const [branchName, setBranchName] = useState('master')
  const [buildConfigKv, setBuildConfigKv] = useState<Record<string, string>>({})

  /* tracked builds */
  const [trackedBuilds, setTrackedBuilds] = useState<string[]>([])
  const [buildInput, setBuildInput] = useState('')

  const [selectedBuild, setSelectedBuild] = useState<string | null>(null)
  const [buildDetail, setBuildDetail] = useState<Record<string, unknown> | null>(null)
  const [buildJobs, setBuildJobs] = useState<Record<string, unknown>[]>([])
  const [detailLoading, setDetailLoading] = useState(false)

  /* create build mutation */
  const createMut = useMutation({
    mutationFn: () => {
      let config: Record<string, unknown> = {}
      config = buildConfigKv as Record<string, unknown>
      return createOrchestrationBuild(ctx, {
        target: { type: 'MANUAL', dataset: buildTarget || undefined },
        type: buildType,
        branch: branchName,
        ...config,
      })
    },
    onSuccess: (data) => {
      setCreateOpen(false)
      const rid = String(data.buildRid ?? data.rid ?? '')
      if (rid) {
        setTrackedBuilds((prev) => [rid, ...prev])
      }
    },
  })

  /* cancel build */
  const cancelMut = useMutation({
    mutationFn: (rid: string) => cancelOrchestrationBuild(ctx, rid),
  })

  /* load build detail */
  const loadBuildDetail = async (rid: string) => {
    setSelectedBuild(rid)
    setDetailLoading(true)
    try {
      const [detail, jobs] = await Promise.all([
        getOrchestrationBuild(ctx, rid),
        listOrchestrationBuildJobs(ctx, rid),
      ])
      setBuildDetail(detail)
      setBuildJobs(jobs)
    } catch {
      setBuildDetail(null)
      setBuildJobs([])
    } finally {
      setDetailLoading(false)
    }
  }

  const addTrackedBuild = () => {
    const rid = buildInput.trim()
    if (rid && !trackedBuilds.includes(rid)) {
      setTrackedBuilds((prev) => [rid, ...prev])
      setBuildInput('')
    }
  }

  return (
    <div style={{ marginTop: 12 }}>
      <div className="form-row" style={{ justifyContent: 'space-between', marginBottom: 12 }}>
        <div className="form-row" style={{ gap: 8 }}>
          <InputGroup
            value={buildInput}
            onChange={(e) => setBuildInput(e.target.value)}
            placeholder="Paste build RID to track..."
            onKeyDown={(e) => { if (e.key === 'Enter') addTrackedBuild() }}
            style={{ width: 300 }}
          />
          <Button icon="plus" onClick={addTrackedBuild} disabled={!buildInput.trim()}>
            Track
          </Button>
        </div>
        <Button icon="build" intent={Intent.PRIMARY} onClick={() => setCreateOpen(true)}>
          Create Build
        </Button>
      </div>

      <div className="two-col-grid">
        {/* Build List */}
        <Card>
          <div className="card-title">Tracked Builds</div>
          {trackedBuilds.length === 0 && (
            <Callout icon="info-sign">
              No builds tracked. Create a new build or paste a build RID above.
            </Callout>
          )}
          <div className="nav-list">
            {trackedBuilds.map((rid) => {
              const isActive = selectedBuild === rid
              return (
                <button
                  key={rid}
                  className={`nav-item ${isActive ? 'is-active' : ''}`}
                  onClick={() => loadBuildDetail(rid)}
                >
                  <Tag icon="build" minimal style={{ fontFamily: 'monospace', fontSize: 11 }}>
                    {rid.length > 30 ? `${rid.slice(0, 15)}...${rid.slice(-10)}` : rid}
                  </Tag>
                </button>
              )
            })}
          </div>
        </Card>

        {/* Build Detail */}
        <Card>
          <div className="card-title">Build Detail</div>
          {!selectedBuild && (
            <Callout icon="info-sign">Select a build from the list to view details.</Callout>
          )}
          {detailLoading && <Spinner size={20} />}
          {buildDetail && !detailLoading && (
            <div>
              <div className="form-row" style={{ gap: 8, marginBottom: 12 }}>
                <Tag intent={
                  String(buildDetail.status).toLowerCase() === 'running' ? Intent.PRIMARY :
                  String(buildDetail.status).toLowerCase() === 'succeeded' ? Intent.SUCCESS :
                  String(buildDetail.status).toLowerCase() === 'failed' ? Intent.DANGER :
                  Intent.NONE
                }>
                  {String(buildDetail.status ?? 'unknown')}
                </Tag>
                {String(buildDetail.status).toLowerCase() === 'running' && (
                  <Button
                    minimal
                    icon="stop"
                    intent={Intent.DANGER}
                    loading={cancelMut.isPending}
                    onClick={() => cancelMut.mutate(selectedBuild!)}
                  >
                    Cancel
                  </Button>
                )}
                <Button
                  minimal
                  icon="refresh"
                  onClick={() => loadBuildDetail(selectedBuild!)}
                >
                  Refresh
                </Button>
              </div>

              {/* Build Info */}
              <HTMLTable compact striped style={{ width: '100%', marginBottom: 12 }}>
                <tbody>
                  {Object.entries(buildDetail).filter(([k]) => typeof buildDetail[k] !== 'object').slice(0, 10).map(([key, val]) => (
                    <tr key={key}>
                      <td style={{ fontFamily: 'monospace', fontSize: 12, fontWeight: 500, width: '40%' }}>{key}</td>
                      <td style={{ fontSize: 12 }}>{String(val ?? '—')}</td>
                    </tr>
                  ))}
                </tbody>
              </HTMLTable>

              {/* Jobs */}
              {buildJobs.length > 0 && (
                <>
                  <div className="card-title" style={{ fontSize: 13, marginBottom: 8 }}>
                    Jobs ({buildJobs.length})
                  </div>
                  <HTMLTable compact striped style={{ width: '100%' }}>
                    <thead>
                      <tr>
                        <th>Job</th>
                        <th>Status</th>
                        <th>Duration</th>
                      </tr>
                    </thead>
                    <tbody>
                      {buildJobs.map((job, i) => (
                        <tr key={i}>
                          <td style={{ fontFamily: 'monospace', fontSize: 12 }}>
                            {String(job.jobId ?? job.rid ?? `job_${i}`)}
                          </td>
                          <td>
                            <Tag
                              minimal
                              intent={
                                String(job.status).toLowerCase() === 'succeeded' ? Intent.SUCCESS :
                                String(job.status).toLowerCase() === 'failed' ? Intent.DANGER :
                                String(job.status).toLowerCase() === 'running' ? Intent.PRIMARY :
                                Intent.NONE
                              }
                            >
                              {String(job.status ?? '—')}
                            </Tag>
                          </td>
                          <td style={{ fontSize: 12 }}>
                            {job.duration ? `${String(job.duration)}s` : '—'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </HTMLTable>
                </>
              )}

              <div style={{ marginTop: 12 }}>
                <JsonViewer value={buildDetail} />
              </div>
            </div>
          )}
        </Card>
      </div>

      {/* Create Build Dialog */}
      <Dialog
        isOpen={createOpen}
        onClose={() => setCreateOpen(false)}
        title="Create Build"
        style={{ width: 500 }}
      >
        <div style={{ padding: 20 }}>
          <FormGroup label="Target Dataset / Resource">
            <InputGroup
              value={buildTarget}
              onChange={(e) => setBuildTarget(e.target.value)}
              placeholder="e.g. ri.foundry.main.dataset.abc123"
            />
          </FormGroup>
          <FormGroup label="Build Type">
            <HTMLSelect
              value={buildType}
              onChange={(e) => setBuildType(e.target.value)}
              fill
              options={[
                { value: 'INCREMENTAL', label: 'Incremental' },
                { value: 'SNAPSHOT', label: 'Snapshot (Full)' },
                { value: 'FORCE_BUILD', label: 'Force Build' },
              ]}
            />
          </FormGroup>
          <FormGroup label="Branch">
            <InputGroup
              value={branchName}
              onChange={(e) => setBranchName(e.target.value)}
              placeholder="master"
            />
          </FormGroup>
          <FormGroup label={
            <Tooltip content="Additional build settings (e.g. retryCount, notificationsEnabled)" placement="top">
              <span className="tooltip-label">Build Configuration</span>
            </Tooltip>
          }>
            <KeyValueEditor
              value={buildConfigKv}
              onChange={setBuildConfigKv}
              keyPlaceholder="Config key"
              valuePlaceholder="Config value"
              addLabel="Add config"
            />
          </FormGroup>
          <div className="form-row" style={{ justifyContent: 'flex-end', gap: 8 }}>
            <Button onClick={() => setCreateOpen(false)}>Cancel</Button>
            <Button
              intent={Intent.PRIMARY}
              icon="build"
              loading={createMut.isPending}
              onClick={() => createMut.mutate()}
            >
              Create Build
            </Button>
          </div>
          {createMut.error && (
            <Callout intent={Intent.DANGER} style={{ marginTop: 8 }}>Failed to create build.</Callout>
          )}
          {createMut.isSuccess && (
            <Callout intent={Intent.SUCCESS} style={{ marginTop: 8 }}>Build created successfully.</Callout>
          )}
        </div>
      </Dialog>
    </div>
  )
}

/* ── Schedules Tab ──────────────────────────────────── */
const SchedulesTab = ({
  ctx,
  queryClient,
}: {
  ctx: ReturnType<typeof useRequestContext>
  queryClient: ReturnType<typeof useQueryClient>
}) => {
  const [createOpen, setCreateOpen] = useState(false)
  const [scheduleName, setScheduleName] = useState('')
  const [cronExpr, setCronExpr] = useState('0 0 * * *')
  const [scheduleTarget, setScheduleTarget] = useState('')
  const [scheduleBranch, setScheduleBranch] = useState('master')
  const [schedConfigKv, setSchedConfigKv] = useState<Record<string, string>>({})

  /* tracked schedules */
  const [trackedSchedules, setTrackedSchedules] = useState<string[]>([])
  const [scheduleInput, setScheduleInput] = useState('')

  const [selectedSchedule, setSelectedSchedule] = useState<string | null>(null)
  const [scheduleDetail, setScheduleDetail] = useState<Record<string, unknown> | null>(null)
  const [scheduleRuns, setScheduleRuns] = useState<Record<string, unknown>[]>([])
  const [detailLoading, setDetailLoading] = useState(false)

  /* create schedule */
  const createMut = useMutation({
    mutationFn: () => {
      const config = schedConfigKv as Record<string, unknown>
      return createOrchestrationSchedule(ctx, {
        displayName: scheduleName,
        cronExpression: cronExpr,
        target: scheduleTarget || undefined,
        branch: scheduleBranch,
        ...config,
      })
    },
    onSuccess: (data) => {
      setCreateOpen(false)
      const rid = String(data.rid ?? data.scheduleRid ?? '')
      if (rid) {
        setTrackedSchedules((prev) => [rid, ...prev])
      }
    },
  })

  /* delete schedule */
  const deleteMut = useMutation({
    mutationFn: (rid: string) => deleteOrchestrationSchedule(ctx, rid),
    onSuccess: () => {
      setTrackedSchedules((prev) => prev.filter((r) => r !== selectedSchedule))
      setSelectedSchedule(null)
      setScheduleDetail(null)
    },
  })

  /* pause/unpause */
  const pauseMut = useMutation({
    mutationFn: (rid: string) => pauseOrchestrationSchedule(ctx, rid),
    onSuccess: () => { if (selectedSchedule) loadScheduleDetail(selectedSchedule) },
  })

  const unpauseMut = useMutation({
    mutationFn: (rid: string) => unpauseOrchestrationSchedule(ctx, rid),
    onSuccess: () => { if (selectedSchedule) loadScheduleDetail(selectedSchedule) },
  })

  const loadScheduleDetail = async (rid: string) => {
    setSelectedSchedule(rid)
    setDetailLoading(true)
    try {
      const [detail, runs] = await Promise.all([
        getOrchestrationSchedule(ctx, rid),
        listOrchestrationScheduleRuns(ctx, rid),
      ])
      setScheduleDetail(detail)
      setScheduleRuns(runs)
    } catch {
      setScheduleDetail(null)
      setScheduleRuns([])
    } finally {
      setDetailLoading(false)
    }
  }

  const addTrackedSchedule = () => {
    const rid = scheduleInput.trim()
    if (rid && !trackedSchedules.includes(rid)) {
      setTrackedSchedules((prev) => [rid, ...prev])
      setScheduleInput('')
    }
  }

  return (
    <div style={{ marginTop: 12 }}>
      <div className="form-row" style={{ justifyContent: 'space-between', marginBottom: 12 }}>
        <div className="form-row" style={{ gap: 8 }}>
          <InputGroup
            value={scheduleInput}
            onChange={(e) => setScheduleInput(e.target.value)}
            placeholder="Paste schedule RID to track..."
            onKeyDown={(e) => { if (e.key === 'Enter') addTrackedSchedule() }}
            style={{ width: 300 }}
          />
          <Button icon="plus" onClick={addTrackedSchedule} disabled={!scheduleInput.trim()}>
            Track
          </Button>
        </div>
        <Button icon="time" intent={Intent.PRIMARY} onClick={() => setCreateOpen(true)}>
          Create Schedule
        </Button>
      </div>

      <div className="two-col-grid">
        {/* Schedule List */}
        <Card>
          <div className="card-title">Tracked Schedules</div>
          {trackedSchedules.length === 0 && (
            <Callout icon="info-sign">
              No schedules tracked. Create a schedule or paste a schedule RID above.
            </Callout>
          )}
          <div className="nav-list">
            {trackedSchedules.map((rid) => {
              const isActive = selectedSchedule === rid
              return (
                <button
                  key={rid}
                  className={`nav-item ${isActive ? 'is-active' : ''}`}
                  onClick={() => loadScheduleDetail(rid)}
                >
                  <Tag icon="time" minimal style={{ fontFamily: 'monospace', fontSize: 11 }}>
                    {rid.length > 30 ? `${rid.slice(0, 15)}...${rid.slice(-10)}` : rid}
                  </Tag>
                </button>
              )
            })}
          </div>
        </Card>

        {/* Schedule Detail */}
        <Card>
          <div className="card-title">Schedule Detail</div>
          {!selectedSchedule && (
            <Callout icon="info-sign">Select a schedule from the list to view details.</Callout>
          )}
          {detailLoading && <Spinner size={20} />}
          {scheduleDetail && !detailLoading && (
            <div>
              <div className="form-row" style={{ gap: 8, marginBottom: 12 }}>
                <Tag intent={
                  String(scheduleDetail.paused).toLowerCase() === 'true' ? Intent.WARNING :
                  Intent.SUCCESS
                }>
                  {String(scheduleDetail.paused).toLowerCase() === 'true' ? 'Paused' : 'Active'}
                </Tag>
                {typeof scheduleDetail.cronExpression === 'string' && (
                  <Tag minimal icon="time">{scheduleDetail.cronExpression}</Tag>
                )}
                <div style={{ marginLeft: 'auto' }}>
                  {String(scheduleDetail.paused).toLowerCase() === 'true' ? (
                    <Button
                      minimal
                      icon="play"
                      intent={Intent.SUCCESS}
                      loading={unpauseMut.isPending}
                      onClick={() => unpauseMut.mutate(selectedSchedule!)}
                    >
                      Resume
                    </Button>
                  ) : (
                    <Button
                      minimal
                      icon="pause"
                      intent={Intent.WARNING}
                      loading={pauseMut.isPending}
                      onClick={() => pauseMut.mutate(selectedSchedule!)}
                    >
                      Pause
                    </Button>
                  )}
                  <Button
                    minimal
                    icon="trash"
                    intent={Intent.DANGER}
                    loading={deleteMut.isPending}
                    onClick={() => deleteMut.mutate(selectedSchedule!)}
                  >
                    Delete
                  </Button>
                  <Button
                    minimal
                    icon="refresh"
                    onClick={() => loadScheduleDetail(selectedSchedule!)}
                  >
                    Refresh
                  </Button>
                </div>
              </div>

              {/* Schedule Info */}
              <HTMLTable compact striped style={{ width: '100%', marginBottom: 12 }}>
                <tbody>
                  {Object.entries(scheduleDetail).filter(([, val]) => typeof val !== 'object').slice(0, 10).map(([key, val]) => (
                    <tr key={key}>
                      <td style={{ fontFamily: 'monospace', fontSize: 12, fontWeight: 500, width: '40%' }}>{key}</td>
                      <td style={{ fontSize: 12 }}>{String(val ?? '—')}</td>
                    </tr>
                  ))}
                </tbody>
              </HTMLTable>

              {/* Runs */}
              {scheduleRuns.length > 0 && (
                <>
                  <div className="card-title" style={{ fontSize: 13, marginBottom: 8 }}>
                    Recent Runs ({scheduleRuns.length})
                  </div>
                  <HTMLTable compact striped style={{ width: '100%' }}>
                    <thead>
                      <tr>
                        <th>Run</th>
                        <th>Status</th>
                        <th>Started</th>
                        <th>Duration</th>
                      </tr>
                    </thead>
                    <tbody>
                      {scheduleRuns.slice(0, 20).map((run, i) => (
                        <tr key={i}>
                          <td style={{ fontFamily: 'monospace', fontSize: 11 }}>
                            {String(run.runId ?? run.rid ?? `run_${i}`).slice(0, 20)}
                          </td>
                          <td>
                            <Tag
                              minimal
                              intent={
                                String(run.status).toLowerCase() === 'succeeded' ? Intent.SUCCESS :
                                String(run.status).toLowerCase() === 'failed' ? Intent.DANGER :
                                String(run.status).toLowerCase() === 'running' ? Intent.PRIMARY :
                                Intent.NONE
                              }
                            >
                              {String(run.status ?? '—')}
                            </Tag>
                          </td>
                          <td style={{ fontSize: 12 }}>
                            {run.startedAt ? new Date(String(run.startedAt)).toLocaleString() : '—'}
                          </td>
                          <td style={{ fontSize: 12 }}>
                            {run.duration ? `${String(run.duration)}s` : '—'}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </HTMLTable>
                </>
              )}

              <div style={{ marginTop: 12 }}>
                <JsonViewer value={scheduleDetail} />
              </div>
            </div>
          )}
        </Card>
      </div>

      {/* Create Schedule Dialog */}
      <Dialog
        isOpen={createOpen}
        onClose={() => setCreateOpen(false)}
        title="Create Schedule"
        style={{ width: 500 }}
      >
        <div style={{ padding: 20 }}>
          <FormGroup label="Schedule Name">
            <InputGroup
              value={scheduleName}
              onChange={(e) => setScheduleName(e.target.value)}
              placeholder="e.g. Daily Orders ETL"
            />
          </FormGroup>
          <FormGroup label={
            <Tooltip content="Set the schedule frequency using dropdowns or presets" placement="top">
              <span className="tooltip-label">Schedule Frequency</span>
            </Tooltip>
          }>
            <CronBuilder value={cronExpr} onChange={setCronExpr} />
          </FormGroup>
          <FormGroup label="Target Resource">
            <InputGroup
              value={scheduleTarget}
              onChange={(e) => setScheduleTarget(e.target.value)}
              placeholder="e.g. ri.foundry.main.dataset.abc123"
            />
          </FormGroup>
          <FormGroup label="Branch">
            <InputGroup
              value={scheduleBranch}
              onChange={(e) => setScheduleBranch(e.target.value)}
              placeholder="master"
            />
          </FormGroup>
          <FormGroup label={
            <Tooltip content="Extra schedule settings as key-value pairs" placement="top">
              <span className="tooltip-label">Additional Config</span>
            </Tooltip>
          }>
            <KeyValueEditor
              value={schedConfigKv}
              onChange={setSchedConfigKv}
              keyPlaceholder="Config key"
              valuePlaceholder="Config value"
              addLabel="Add config"
            />
          </FormGroup>
          <div className="form-row" style={{ justifyContent: 'flex-end', gap: 8 }}>
            <Button onClick={() => setCreateOpen(false)}>Cancel</Button>
            <Button
              intent={Intent.PRIMARY}
              icon="time"
              loading={createMut.isPending}
              disabled={!scheduleName}
              onClick={() => createMut.mutate()}
            >
              Create Schedule
            </Button>
          </div>
          {createMut.error && (
            <Callout intent={Intent.DANGER} style={{ marginTop: 8 }}>Failed to create schedule.</Callout>
          )}
        </div>
      </Dialog>
    </div>
  )
}
