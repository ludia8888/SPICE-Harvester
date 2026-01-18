import { Button, Icon } from '@blueprintjs/core'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  attachAgentSessionContextItem,
  aiQuery,
  createAgentSession,
  decideAgentSessionApproval,
  executeAgentPlan,
  getAgentPlan,
  listAgentSessionEvents,
  listDatasets,
  postAgentSessionMessage,
  type AgentSessionEvent,
  type DatasetRecord,
} from '../api/bff'
import { UploadFilesDialog } from '../components/UploadFilesDialog'
import { useAppStore } from '../state/store'

type ChatMessage = {
  id: string
  role: 'user' | 'assistant'
  text: string
  timestamp?: string
}

type PlanStepSummary = {
  stepId: string
  toolId: string
  method?: string
  description?: string
  bodyPreview?: string
  requiresApproval?: boolean
  produces?: string[]
  consumes?: string[]
}

type PlanCard = {
  id: string
  planId: string
  approvalRequestId?: string
  jobId?: string
  status?: string
  riskLevel?: string
  requiresApproval?: boolean
  goal?: string
  steps?: PlanStepSummary[]
  occurredAt?: string
  source: 'approval' | 'job'
  approvalDecision?: string
}

type ChatItem =
  | ({ kind: 'message' } & ChatMessage)
  | ({ kind: 'plan' } & PlanCard)

type MessageMode = 'greeting' | 'query' | 'plan'

const buildId = () => `msg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`

const defaultPrompts = [
  'Summarize the latest dataset changes.',
  'Show me pipelines that depend on Orders.',
  'Find datasets with missing schema types.',
]

const normalizeLabel = (value: string) => value.trim().toLowerCase()

const greetingTokens = ['안녕', 'ㅎㅇ', '하이', 'hello', 'hi', 'hey']
const planTokens = [
  '파이프라인',
  'pipeline',
  '정제',
  '클렌징',
  '전처리',
  '변환',
  '조인',
  'join',
  'merge',
  'union',
  'aggregate',
  '집계',
  '필터',
  'filter',
  'drop',
  '삭제',
  'rename',
  '리네임',
  '노드',
  'build',
  'deploy',
  '실행',
  '배포',
  '생성',
  '만들',
  '추가',
  '연결',
  '매핑',
  'mapping',
  'schema',
  '컬럼',
  'column',
  '업로드',
  'upload',
  'ingest',
  'import',
  'export',
]
const actionTokens = ['해줘', '해주세요', '만들어', '생성', '적용', '추가', '삭제', '실행', '배포', '빌드', '구성']
const queryTokens = ['어떤', '무엇', '뭐', '몇', '얼마나', '리스트', '목록', '보여', '찾아', '조회', '있는지', '알려', '?']

const safeText = (value: unknown) => {
  if (typeof value === 'string') {
    return value
  }
  if (value == null) {
    return ''
  }
  return String(value)
}

const truncatePreview = (value: unknown, maxChars = 220) => {
  if (value == null) {
    return undefined
  }
  let raw = ''
  if (typeof value === 'string') {
    raw = value
  } else {
    try {
      raw = JSON.stringify(value)
    } catch {
      raw = String(value)
    }
  }
  const trimmed = raw.trim()
  if (!trimmed) {
    return undefined
  }
  if (trimmed.length <= maxChars) {
    return trimmed
  }
  return `${trimmed.slice(0, maxChars - 1)}…`
}

const buildStepSummariesFromPayload = (payload: Record<string, unknown> | null | undefined): PlanStepSummary[] => {
  const rawSteps = payload ? (payload as Record<string, unknown>)['steps'] : undefined
  const steps = Array.isArray(rawSteps) ? rawSteps : []
  const summaries: PlanStepSummary[] = []
  steps.forEach((step, index) => {
    if (!step || typeof step !== 'object') {
      return
    }
    const data = step as Record<string, unknown>
    const toolId = safeText(data['tool_id']).trim()
    if (!toolId) {
      return
    }
    const summary: PlanStepSummary = {
      stepId: safeText(data['step_id']).trim() || `step_${index + 1}`,
      toolId,
      requiresApproval: Boolean(data['requires_approval']),
    }
    const method = safeText(data['method']).trim()
    if (method) {
      summary.method = method
    }
    const description = safeText(data['description']).trim()
    if (description) {
      summary.description = description
    }
    const bodyPreview = safeText(data['body_preview']).trim() || truncatePreview(data['body'])
    if (bodyPreview) {
      summary.bodyPreview = bodyPreview
    }
    const produces = Array.isArray(data['produces']) ? (data['produces'] as string[]).filter(Boolean) : []
    if (produces.length > 0) {
      summary.produces = produces
    }
    const consumes = Array.isArray(data['consumes']) ? (data['consumes'] as string[]).filter(Boolean) : []
    if (consumes.length > 0) {
      summary.consumes = consumes
    }
    summaries.push(summary)
  })
  return summaries
}

const buildStepSummariesFromPlan = (plan: Record<string, unknown> | null | undefined): PlanStepSummary[] => {
  const rawSteps = plan ? plan['steps'] : undefined
  const steps = Array.isArray(rawSteps) ? rawSteps : []
  const summaries: PlanStepSummary[] = []
  steps.forEach((step, index) => {
    if (!step || typeof step !== 'object') {
      return
    }
    const data = step as Record<string, unknown>
    const toolId = safeText(data['tool_id']).trim()
    if (!toolId) {
      return
    }
    const summary: PlanStepSummary = {
      stepId: safeText(data['step_id']).trim() || `step_${index + 1}`,
      toolId,
      requiresApproval: Boolean(data['requires_approval']),
    }
    const method = safeText(data['method']).trim()
    if (method) {
      summary.method = method
    }
    const description = safeText(data['description']).trim()
    if (description) {
      summary.description = description
    }
    const bodyPreview = truncatePreview(data['body'])
    if (bodyPreview) {
      summary.bodyPreview = bodyPreview
    }
    const produces = Array.isArray(data['produces']) ? (data['produces'] as string[]).filter(Boolean) : []
    if (produces.length > 0) {
      summary.produces = produces
    }
    const consumes = Array.isArray(data['consumes']) ? (data['consumes'] as string[]).filter(Boolean) : []
    if (consumes.length > 0) {
      summary.consumes = consumes
    }
    summaries.push(summary)
  })
  return summaries
}

const formatStatusLabel = (value?: string) => {
  const normalized = safeText(value).trim()
  if (!normalized) {
    return 'unknown'
  }
  return normalized.replace(/_/g, ' ').toLowerCase()
}

const inferMessageMode = (text: string): MessageMode => {
  const normalized = normalizeLabel(text)
  if (!normalized) {
    return 'query'
  }
  const isGreeting = greetingTokens.some((token) => normalized.includes(token))
  if (isGreeting && normalized.length <= 12) {
    return 'greeting'
  }
  const hasPlanToken = planTokens.some((token) => normalized.includes(token))
  const hasActionToken = actionTokens.some((token) => normalized.includes(token))
  const hasQueryToken = queryTokens.some((token) => normalized.includes(token))
  if (hasPlanToken && hasActionToken) {
    return 'plan'
  }
  if (hasQueryToken) {
    return 'query'
  }
  return hasPlanToken ? 'query' : 'query'
}

const extractAiAnswer = (payload: unknown) => {
  if (!payload || typeof payload !== 'object') {
    return ''
  }
  const answer = (payload as { answer?: { answer?: unknown } }).answer
  if (answer && typeof answer.answer === 'string') {
    return answer.answer.trim()
  }
  return ''
}

const getChatItemTimestamp = (item: ChatItem) => {
  if (item.kind === 'message') {
    return item.timestamp || ''
  }
  return item.occurredAt || ''
}

const buildChatItemsFromEvents = (events: AgentSessionEvent[]) => {
  const sorted = [...events].sort((left, right) => {
    const leftTs = Date.parse(left.occurred_at || '') || 0
    const rightTs = Date.parse(right.occurred_at || '') || 0
    if (leftTs !== rightTs) {
      return leftTs - rightTs
    }
    return left.event_id.localeCompare(right.event_id)
  })

  const messageItems: ChatItem[] = []
  const planById = new Map<string, PlanCard>()
  const jobStatusByPlanId = new Map<string, { status?: string; jobId?: string; occurredAt?: string }>()
  const approvalDecisions = new Map<string, string>()

  for (const event of sorted) {
    const data = event.data ?? {}
    if (event.event_type === 'SESSION_MESSAGE') {
      if (data['is_removed']) {
        continue
      }
      const role = data['role'] === 'user' ? 'user' : 'assistant'
      const content = safeText(data['content']).trim()
      if (!content) {
        continue
      }
      messageItems.push({
        kind: 'message',
        id: event.event_id,
        role,
        text: content,
        timestamp: event.occurred_at,
      })
      continue
    }
    if (event.event_type === 'SESSION_JOB') {
      const planId = safeText(data['plan_id']).trim()
      if (planId) {
        jobStatusByPlanId.set(planId, {
          status: safeText(data['status']).trim() || undefined,
          jobId: safeText(data['job_id']).trim() || undefined,
          occurredAt: event.occurred_at,
        })
      }
      continue
    }
    if (event.event_type === 'AGENT_RUN') {
      const status = safeText(data['status']).trim()
      const label = status ? status.replace(/_/g, ' ').toLowerCase() : 'started'
      messageItems.push({
        kind: 'message',
        id: event.event_id,
        role: 'assistant',
        text: `Run ${label}.`,
        timestamp: event.occurred_at,
      })
      continue
    }
    if (event.event_type === 'CLARIFICATION_REQUESTED') {
      const questions = Array.isArray(data['questions']) ? data['questions'] : []
      const formatted = questions
        .map((item) => {
          if (typeof item === 'string') {
            return item
          }
          if (item && typeof item === 'object' && 'question' in item) {
            return safeText((item as { question?: unknown }).question)
          }
          return ''
        })
        .filter((item) => item)
      const text = formatted.length ? `Clarification needed: ${formatted.join(' | ')}` : 'Clarification needed.'
      messageItems.push({
        kind: 'message',
        id: event.event_id,
        role: 'assistant',
        text,
        timestamp: event.occurred_at,
      })
      continue
    }
    if (event.event_type === 'APPROVAL_REQUESTED') {
      const approvalRequestId = safeText(data['approval_request_id']).trim()
      const payload =
        data['request_payload'] && typeof data['request_payload'] === 'object'
          ? (data['request_payload'] as Record<string, unknown>)
          : null
      const planId = safeText(data['plan_id'] || payload?.['plan_id']).trim()
      if (!planId) {
        continue
      }
      const existing = planById.get(planId)
      const steps = payload ? buildStepSummariesFromPayload(payload) : undefined
      const planCard: PlanCard = {
        id: approvalRequestId ? `approval-${approvalRequestId}` : `plan-${planId}`,
        planId,
        approvalRequestId: approvalRequestId || undefined,
        jobId: safeText(data['job_id']).trim() || undefined,
        status: safeText(data['status']).trim() || undefined,
        riskLevel: safeText(payload?.['risk_level'] || data['risk_level']).trim() || undefined,
        requiresApproval: Boolean(payload?.['requires_approval'] ?? true),
        goal: safeText(payload?.['goal']).trim() || undefined,
        steps,
        occurredAt: existing?.occurredAt ?? event.occurred_at,
        source: 'approval',
        approvalDecision: existing?.approvalDecision,
      }
      planById.set(planId, planCard)
      continue
    }
    if (event.event_type === 'APPROVAL_DECIDED') {
      const approvalId = safeText(data['approval_request_id']).trim()
      const decision = safeText(data['decision']).trim().toLowerCase()
      if (approvalId && decision) {
        approvalDecisions.set(approvalId, decision)
      }
      const text = decision ? `Approval ${decision}.` : 'Approval decision recorded.'
      messageItems.push({
        kind: 'message',
        id: event.event_id,
        role: 'assistant',
        text,
        timestamp: event.occurred_at,
      })
      continue
    }
  }

  const planItems: ChatItem[] = []
  jobStatusByPlanId.forEach((jobMeta, planId) => {
    const existing = planById.get(planId)
    if (existing) {
      if (!existing.status && jobMeta.status) {
        existing.status = jobMeta.status
      }
      if (!existing.jobId && jobMeta.jobId) {
        existing.jobId = jobMeta.jobId
      }
      planById.set(planId, existing)
    } else {
      planById.set(planId, {
        id: `plan-${planId}`,
        planId,
        jobId: jobMeta.jobId,
        status: jobMeta.status,
        occurredAt: jobMeta.occurredAt,
        source: 'job',
      })
    }
  })

  planById.forEach((plan) => {
    if (plan.approvalRequestId && approvalDecisions.has(plan.approvalRequestId)) {
      plan.approvalDecision = approvalDecisions.get(plan.approvalRequestId)
    }
    planItems.push({ ...plan, kind: 'plan' })
  })

  const combined = [...messageItems, ...planItems].sort((left, right) => {
    const leftTs = Date.parse(getChatItemTimestamp(left)) || 0
    const rightTs = Date.parse(getChatItemTimestamp(right)) || 0
    if (leftTs !== rightTs) {
      return leftTs - rightTs
    }
    return left.id.localeCompare(right.id)
  })

  return combined
}

export const AIAgentPage = () => {
  const activeNav = useAppStore((state) => state.activeNav)
  const pipelineContext = useAppStore((state) => state.pipelineContext)
  const aiAgentContext = useAppStore((state) => state.aiAgentContext)
  const isAiAgentOpen = useAppStore((state) => state.isAiAgentOpen)
  const queryClient = useQueryClient()
  const [extraItems, setExtraItems] = useState<ChatItem[]>([])
  const [planDetails, setPlanDetails] = useState<Record<string, { goal?: string; riskLevel?: string; requiresApproval?: boolean; steps?: PlanStepSummary[] }>>({})
  const [planActions, setPlanActions] = useState<Record<string, { status: 'idle' | 'loading' | 'error'; error?: string }>>({})
  const [sessionId, setSessionId] = useState('')
  const [isSending, setIsSending] = useState(false)
  const [draft, setDraft] = useState('')
  const [prompts] = useState(defaultPrompts)
  const [isContextOpen, setContextOpen] = useState(false)
  const [isUploadOpen, setUploadOpen] = useState(false)
  const chatRef = useRef<HTMLDivElement | null>(null)
  const sessionIdRef = useRef('')
  const planFetchInFlightRef = useRef<Set<string>>(new Set())
  const attachedDatasetIdsRef = useRef<Set<string>>(new Set())
  const canSend = draft.trim().length > 0 && !isSending
  const isAgentVisible = isAiAgentOpen || activeNav === 'ai-agent'
  const activeDbName = pipelineContext?.folderId ?? ''
  const contextLabel = useMemo(() => {
    if (aiAgentContext.pipelineName) {
      return aiAgentContext.pipelineName
    }
    if (aiAgentContext.projectName) {
      return aiAgentContext.projectName
    }
    return 'No file selected'
  }, [aiAgentContext])
  const sessionMetadata = useMemo(
    () => ({
      source: 'ui',
      project_name: aiAgentContext.projectName || null,
      pipeline_name: aiAgentContext.pipelineName || null,
      node_count: aiAgentContext.nodes.length,
      db_name: activeDbName || null,
    }),
    [aiAgentContext, activeDbName],
  )
  const dataScope = useMemo(() => {
    if (!activeDbName) {
      return undefined
    }
    return { db_name: activeDbName }
  }, [activeDbName])

  const { data: datasets = [] } = useQuery({
    queryKey: ['datasets', activeDbName],
    queryFn: () => listDatasets(activeDbName),
    enabled: Boolean(activeDbName) && isAgentVisible,
  })

  const datasetsById = useMemo(() => {
    const map = new Map<string, DatasetRecord>()
    datasets.forEach((dataset) => map.set(dataset.dataset_id, dataset))
    return map
  }, [datasets])

  const datasetsByName = useMemo(() => {
    const map = new Map<string, DatasetRecord>()
    datasets.forEach((dataset) => {
      map.set(normalizeLabel(dataset.name), dataset)
    })
    return map
  }, [datasets])

  const datasetIdsForNodes = useMemo(() => {
    const ids = new Set<string>()
    aiAgentContext.nodes.forEach((node) => {
      const direct = datasetsById.get(node.id)
      if (direct) {
        ids.add(direct.dataset_id)
        return
      }
      const label = normalizeLabel(node.label)
      if (!label) {
        return
      }
      const byName = datasetsByName.get(label)
      if (byName) {
        ids.add(byName.dataset_id)
      }
    })
    return Array.from(ids)
  }, [aiAgentContext.nodes, datasetsById, datasetsByName])

  const eventQueryParams = useMemo(
    () => ({
      limit: 500,
      include_agent_steps: false,
      include_tool_calls: false,
      include_llm_calls: false,
    }),
    [],
  )

  const refreshEvents = useCallback(
    async (activeSessionId: string) => {
      if (!activeSessionId) {
        return null
      }
      const refreshed = await listAgentSessionEvents(activeSessionId, eventQueryParams)
      queryClient.setQueryData(['agent-session-events', activeSessionId], refreshed)
      return refreshed
    },
    [eventQueryParams, queryClient],
  )

  const { data: eventsPayload } = useQuery({
    queryKey: ['agent-session-events', sessionId],
    queryFn: () => listAgentSessionEvents(sessionId, eventQueryParams),
    enabled: Boolean(sessionId) && isAgentVisible,
    refetchInterval: isAgentVisible && sessionId ? 2000 : false,
  })

  const eventItems = useMemo(() => buildChatItemsFromEvents(eventsPayload?.events ?? []), [eventsPayload])
  const items = useMemo(() => {
    if (extraItems.length === 0) {
      return eventItems
    }
    const seen = new Set(eventItems.map((item) => item.id))
    return [...eventItems, ...extraItems.filter((item) => !seen.has(item.id))]
  }, [eventItems, extraItems])

  const planIdsToFetch = useMemo(() => {
    const ids: string[] = []
    items.forEach((item) => {
      if (item.kind !== 'plan') {
        return
      }
      const hasDetails = Boolean(item.goal) || Boolean(item.steps?.length) || Boolean(planDetails[item.planId])
      if (!hasDetails) {
        ids.push(item.planId)
      }
    })
    return ids
  }, [items, planDetails])

  useEffect(() => {
    if (planIdsToFetch.length === 0) {
      return
    }
    const pending = planIdsToFetch.filter((planId) => !planFetchInFlightRef.current.has(planId))
    if (pending.length === 0) {
      return
    }
    pending.forEach((planId) => planFetchInFlightRef.current.add(planId))
    const run = async () => {
      const updates: Record<string, { goal?: string; riskLevel?: string; requiresApproval?: boolean; steps?: PlanStepSummary[] }> =
        {}
      await Promise.all(
        pending.map(async (planId) => {
          try {
            const data = await getAgentPlan(planId)
            const plan = data.plan && typeof data.plan === 'object' ? (data.plan as Record<string, unknown>) : null
            if (!plan) {
              return
            }
            updates[planId] = {
              goal: safeText(plan['goal']).trim() || undefined,
              riskLevel: safeText(plan['risk_level']).trim() || undefined,
              requiresApproval: Boolean(plan['requires_approval']),
              steps: buildStepSummariesFromPlan(plan),
            }
          } catch {
            return
          }
        }),
      )
      if (Object.keys(updates).length > 0) {
        setPlanDetails((current) => ({ ...current, ...updates }))
      }
      pending.forEach((planId) => planFetchInFlightRef.current.delete(planId))
    }
    void run()
  }, [planIdsToFetch])

  useEffect(() => {
    const node = chatRef.current
    if (!node) {
      return
    }
    node.scrollTop = node.scrollHeight
  }, [items])

  useEffect(() => {
    sessionIdRef.current = ''
    attachedDatasetIdsRef.current = new Set()
    setSessionId('')
    setExtraItems([])
    setPlanDetails({})
    setPlanActions({})
    planFetchInFlightRef.current = new Set()
  }, [activeDbName])

  const ensureSession = useCallback(async () => {
    if (sessionIdRef.current) {
      return sessionIdRef.current
    }
    const created = await createAgentSession({ metadata: sessionMetadata })
    const nextId = created.session?.session_id
    if (!nextId) {
      throw new Error('Failed to create agent session')
    }
    sessionIdRef.current = nextId
    attachedDatasetIdsRef.current = new Set()
    setSessionId(nextId)
    return nextId
  }, [sessionMetadata])

  const syncDatasetContext = useCallback(
    async (targetSessionId: string) => {
      if (!targetSessionId || datasetIdsForNodes.length === 0) {
        return
      }
      const missing = datasetIdsForNodes.filter((id) => !attachedDatasetIdsRef.current.has(id))
      if (missing.length === 0) {
        return
      }
      await Promise.all(
        missing.map((datasetId) => {
          const dataset = datasetsById.get(datasetId)
          if (!dataset) {
            return Promise.resolve()
          }
          return attachAgentSessionContextItem(targetSessionId, {
            item_type: 'dataset',
            include_mode: 'summary',
            ref: {
              dataset_id: dataset.dataset_id,
              db_name: dataset.db_name,
              branch: dataset.branch,
              dataset_name: dataset.name,
            },
          })
        }),
      )
      missing.forEach((datasetId) => attachedDatasetIdsRef.current.add(datasetId))
    },
    [datasetIdsForNodes, datasetsById],
  )

  useEffect(() => {
    if (!sessionId || !isAgentVisible) {
      return
    }
    void syncDatasetContext(sessionId)
  }, [sessionId, isAgentVisible, syncDatasetContext])

  const handleSend = async () => {
    const text = draft.trim()
    if (!text || isSending) {
      return
    }
    const mode = inferMessageMode(text)
    if (mode === 'greeting') {
      const timestamp = new Date().toISOString()
      setDraft('')
      setExtraItems((current) => [
        ...current,
        { kind: 'message', id: buildId(), role: 'user', text, timestamp },
        {
          kind: 'message',
          id: buildId(),
          role: 'assistant',
          text: 'Hello! How can I help with your pipeline?',
          timestamp,
        },
      ])
      return
    }
    if (mode === 'query') {
      const timestamp = new Date().toISOString()
      setDraft('')
      setIsSending(true)
      setExtraItems((current) => [...current, { kind: 'message', id: buildId(), role: 'user', text, timestamp }])
      if (!activeDbName) {
        setExtraItems((current) => [
          ...current,
          {
            kind: 'message',
            id: buildId(),
            role: 'assistant',
            text: 'Select a project first to run a query.',
            timestamp,
          },
        ])
        setIsSending(false)
        return
      }
      try {
        const response = await aiQuery(activeDbName, {
          question: text,
          mode: 'auto',
          include_documents: false,
          include_provenance: false,
        })
        const answer = extractAiAnswer(response) || 'I could not find a grounded answer from the data.'
        setExtraItems((current) => [
          ...current,
          {
            kind: 'message',
            id: buildId(),
            role: 'assistant',
            text: answer,
            timestamp: new Date().toISOString(),
          },
        ])
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Query failed'
        setExtraItems((current) => [
          ...current,
          {
            kind: 'message',
            id: buildId(),
            role: 'assistant',
            text: message,
            timestamp: new Date().toISOString(),
          },
        ])
      } finally {
        setIsSending(false)
      }
      return
    }
    setDraft('')
    setIsSending(true)
    try {
      const activeSession = await ensureSession()
      await syncDatasetContext(activeSession)
      await postAgentSessionMessage(activeSession, {
        content: text,
        data_scope: dataScope,
      })
      await refreshEvents(activeSession)
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Agent request failed'
      setExtraItems((current) => [
        ...current,
        {
          kind: 'message',
          id: buildId(),
          role: 'assistant',
          text: message,
          timestamp: new Date().toISOString(),
        },
      ])
    } finally {
      setIsSending(false)
    }
  }

  const handleSubmit: React.FormEventHandler<HTMLFormElement> = (event) => {
    event.preventDefault()
    if (canSend) {
      handleSend()
    }
  }

  const handleOpenUpload = () => {
    setUploadOpen(true)
  }

  const handleCloseUpload = () => {
    setUploadOpen(false)
  }

  const handleKeyDown: React.KeyboardEventHandler<HTMLTextAreaElement> = (event) => {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault()
      if (canSend) {
        handleSend()
      }
    }
  }

  const resolvePlanCard = useCallback(
    (plan: PlanCard) => {
      const details = planDetails[plan.planId]
      return {
        ...plan,
        goal: plan.goal || details?.goal,
        riskLevel: plan.riskLevel || details?.riskLevel,
        requiresApproval: plan.requiresApproval ?? details?.requiresApproval,
        steps: plan.steps && plan.steps.length > 0 ? plan.steps : details?.steps,
      }
    },
    [planDetails],
  )

  const setPlanActionState = useCallback((key: string, next: { status: 'idle' | 'loading' | 'error'; error?: string }) => {
    setPlanActions((current) => ({ ...current, [key]: next }))
  }, [])

  const handleApprovePlan = useCallback(
    async (plan: PlanCard) => {
      if (!plan.approvalRequestId || !sessionId) {
        return
      }
      const key = plan.approvalRequestId
      setPlanActionState(key, { status: 'loading' })
      try {
        await decideAgentSessionApproval(sessionId, plan.approvalRequestId, { decision: 'APPROVE' })
        await refreshEvents(sessionId)
        setPlanActionState(key, { status: 'idle' })
      } catch (error) {
        setPlanActionState(key, {
          status: 'error',
          error: error instanceof Error ? error.message : 'Approval failed',
        })
      }
    },
    [refreshEvents, sessionId, setPlanActionState],
  )

  const handleRejectPlan = useCallback(
    async (plan: PlanCard) => {
      if (!plan.approvalRequestId || !sessionId) {
        return
      }
      const key = plan.approvalRequestId
      setPlanActionState(key, { status: 'loading' })
      try {
        await decideAgentSessionApproval(sessionId, plan.approvalRequestId, { decision: 'REJECT' })
        await refreshEvents(sessionId)
        setPlanActionState(key, { status: 'idle' })
      } catch (error) {
        setPlanActionState(key, {
          status: 'error',
          error: error instanceof Error ? error.message : 'Rejection failed',
        })
      }
    },
    [refreshEvents, sessionId, setPlanActionState],
  )

  const handleExecutePlan = useCallback(
    async (plan: PlanCard) => {
      const key = plan.planId
      setPlanActionState(key, { status: 'loading' })
      try {
        await executeAgentPlan(plan.planId)
        if (sessionId) {
          await refreshEvents(sessionId)
        }
        setPlanActionState(key, { status: 'idle' })
      } catch (error) {
        setPlanActionState(key, {
          status: 'error',
          error: error instanceof Error ? error.message : 'Execution failed',
        })
      }
    },
    [refreshEvents, sessionId, setPlanActionState],
  )

  const renderedItems = useMemo(() => {
    return items.map((item) => {
      if (item.kind === 'message') {
        return (
          <div
            key={item.id}
            className={`ai-agent-message ${item.role === 'user' ? 'is-user' : 'is-assistant'}`}
          >
            {item.text}
          </div>
        )
      }
      const plan = resolvePlanCard(item)
      const actionKey = plan.approvalRequestId || plan.planId
      const actionState = planActions[actionKey] ?? { status: 'idle' }
      const hasDecision = Boolean(plan.approvalDecision)
      const canApprove = Boolean(plan.approvalRequestId) && !hasDecision
      const canReject = Boolean(plan.approvalRequestId) && !hasDecision
      const canExecute = !plan.requiresApproval && plan.source === 'job'
      const steps = plan.steps ?? []
      return (
        <div key={plan.id} className="ai-agent-plan-card">
          <div className="ai-agent-plan-header">
            <div className="ai-agent-plan-title">{plan.goal || 'Pipeline plan'}</div>
            <span className={`ai-agent-plan-status is-${formatStatusLabel(plan.status)}`}>{formatStatusLabel(plan.status)}</span>
          </div>
          <div className="ai-agent-plan-meta">
            <span>Risk: {plan.riskLevel || 'unknown'}</span>
            <span>Plan ID: {plan.planId.slice(0, 8)}</span>
            {plan.approvalDecision ? <span>Decision: {plan.approvalDecision}</span> : null}
          </div>
          {steps.length > 0 ? (
            <details className="ai-agent-plan-steps">
              <summary>Steps ({steps.length})</summary>
              <div className="ai-agent-plan-step-list">
                {steps.map((step) => (
                  <div key={`${plan.planId}-${step.stepId}`} className="ai-agent-plan-step">
                    <div className="ai-agent-plan-step-header">
                      <span>{step.stepId}</span>
                      <span>{step.toolId}</span>
                    </div>
                    {step.description ? <div className="ai-agent-plan-step-desc">{step.description}</div> : null}
                    {step.bodyPreview ? <div className="ai-agent-plan-step-body">{step.bodyPreview}</div> : null}
                  </div>
                ))}
              </div>
            </details>
          ) : (
            <div className="ai-agent-plan-empty">Plan details loading...</div>
          )}
          <div className="ai-agent-plan-actions">
            {canApprove ? (
              <Button small intent="primary" onClick={() => handleApprovePlan(plan)} disabled={actionState.status === 'loading'}>
                Approve & Run
              </Button>
            ) : null}
            {canReject ? (
              <Button small onClick={() => handleRejectPlan(plan)} disabled={actionState.status === 'loading'}>
                Reject
              </Button>
            ) : null}
            {canExecute ? (
              <Button small intent="primary" onClick={() => handleExecutePlan(plan)} disabled={actionState.status === 'loading'}>
                Execute
              </Button>
            ) : null}
            {actionState.status === 'error' ? (
              <span className="ai-agent-plan-error">{actionState.error || 'Action failed'}</span>
            ) : null}
          </div>
        </div>
      )
    })
  }, [handleApprovePlan, handleExecutePlan, handleRejectPlan, items, planActions, resolvePlanCard])

  return (
    <div className="ai-agent-panel-content">
      <div className="ai-agent-panel-header">
        <div className="ai-agent-panel-title">
          <Icon icon="predictive-analysis" size={16} />
          <span>AI Agent</span>
        </div>
        <button
          type="button"
          className={`ai-agent-context-toggle ${isContextOpen ? 'is-open' : ''}`}
          onClick={() => setContextOpen((open) => !open)}
          aria-expanded={isContextOpen}
        >
          <span>{contextLabel}</span>
          <Icon icon={isContextOpen ? 'caret-up' : 'caret-down'} size={12} />
        </button>
      </div>

      <div className="ai-agent-panel-body" ref={chatRef}>
        {isContextOpen ? (
          <div className="ai-agent-context">
            <div className="ai-agent-context-title">{contextLabel}</div>
            <div className="ai-agent-breadcrumb">
              <span className="ai-agent-breadcrumb-item">{aiAgentContext.projectName || 'No project selected'}</span>
              <Icon icon="chevron-right" size={12} />
              <span className="ai-agent-breadcrumb-item">{aiAgentContext.pipelineName || 'No pipeline selected'}</span>
            </div>
            <div className="ai-agent-context-meta">
              <span className="ai-agent-context-label">Nodes</span>
              <span className="ai-agent-context-count">{aiAgentContext.nodes.length}</span>
            </div>
            {aiAgentContext.nodes.length > 0 ? (
              <div className="ai-agent-node-list">
                {aiAgentContext.nodes.map((node) => (
                  <div key={node.id} className="ai-agent-node">
                    <span className="ai-agent-node-label">{node.label}</span>
                    {node.type ? <span className="ai-agent-node-type">{node.type}</span> : null}
                  </div>
                ))}
              </div>
            ) : (
              <div className="ai-agent-node-empty">No nodes on canvas.</div>
            )}
          </div>
        ) : null}
        {items.length === 0 ? (
          <div className="ai-agent-empty">
            <div className="ai-agent-empty-title">Start a conversation</div>
            <div className="ai-agent-empty-subtitle">Ask about datasets, pipelines, or lineage.</div>
            <div className="ai-agent-prompts">
              {prompts.map((prompt) => (
                <button
                  key={prompt}
                  type="button"
                  className="ai-agent-prompt"
                  onClick={() => setDraft(prompt)}
                >
                  {prompt}
                </button>
              ))}
            </div>
          </div>
        ) : (
          <div className="ai-agent-messages">{renderedItems}</div>
        )}
      </div>

      <form className="ai-agent-panel-input" onSubmit={handleSubmit}>
        <button type="button" className="ai-agent-attach-btn" onClick={handleOpenUpload} aria-label="Upload file">
          <Icon icon="plus" size={16} />
        </button>
        <textarea
          className="ai-agent-input-field"
          rows={2}
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Message AI Agent..."
        />
        <Button
          className="ai-agent-send-btn"
          intent="primary"
          icon="arrow-right"
          minimal={!canSend}
          disabled={!canSend}
          type="submit"
        />
      </form>
      <UploadFilesDialog
        isOpen={isUploadOpen}
        onClose={handleCloseUpload}
        activeFolderId={pipelineContext?.folderId ?? null}
      />
    </div>
  )
}
