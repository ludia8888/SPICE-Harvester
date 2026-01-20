import { Button, Icon } from '@blueprintjs/core'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  attachAgentSessionContextItem,
  aiIntent,
  aiQuery,
  createAgentSession,
  decideAgentSessionApproval,
  executeAgentPlan,
  getAgentPlan,
  listAgentSessionContextItems,
  listAgentSessionEvents,
  listDatasets,
  postAgentSessionMessage,
  removeAgentSessionContextItem,
  type AgentSessionContextItem,
  type AgentSessionEvent,
  type AIIntentResponse,
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

type ToolCallOutlineItem = {
  toolRunId: string
  toolId: string
  method?: string
  path?: string
  status?: string
  httpStatus?: number
  startedAt?: string
  finishedAt?: string
}

type ChatItem =
  | ({ kind: 'message' } & ChatMessage)
  | ({ kind: 'plan' } & PlanCard)

const buildId = () => `msg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`

const defaultPrompts = [
  'Summarize the latest dataset changes.',
  'Show me pipelines that depend on Orders.',
  'Find datasets with missing schema types.',
]

const normalizeLabel = (value: string) => value.trim().toLowerCase()

const safeText = (value: unknown) => {
  if (typeof value === 'string') {
    return value
  }
  if (value == null) {
    return ''
  }
  return String(value)
}

const resolveIntentReply = (intent: AIIntentResponse) => {
  const text = intent.requires_clarification
    ? safeText(intent.clarifying_question).trim()
    : safeText(intent.reply).trim()
  return text
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

const resolvePinnedLabel = (item: AgentSessionContextItem) => {
  const ref = item.ref ?? {}
  if (item.item_type === 'message') {
    const role = safeText(ref['role']).trim() || 'message'
    const content = truncatePreview(ref['content'] ?? '')
    return content ? `${role}: ${content}` : role
  }
  if (item.item_type === 'tool_call') {
    const toolId = safeText(ref['tool_id']).trim() || 'tool_call'
    const method = safeText(ref['method']).trim()
    const path = safeText(ref['path']).trim()
    const suffix = [method, path].filter((part) => part).join(' ')
    return suffix ? `${toolId} ${suffix}` : toolId
  }
  if (item.item_type === 'plan') {
    const goal = safeText(ref['goal']).trim()
    if (goal) {
      return `plan: ${goal}`
    }
    const planId = safeText(ref['plan_id']).trim()
    if (planId) {
      return `plan: ${planId.slice(0, 8)}`
    }
    return 'plan'
  }
  const name =
    safeText(ref['name']).trim() ||
    safeText(ref['label']).trim() ||
    safeText(ref['dataset_name']).trim() ||
    safeText(ref['dataset_id']).trim()
  return name ? `${item.item_type}: ${name}` : item.item_type
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

const buildToolCallOutlineFromEvents = (events: AgentSessionEvent[]): ToolCallOutlineItem[] => {
  const byRunId = new Map<string, ToolCallOutlineItem>()
  events.forEach((event) => {
    const data = event.data ?? {}
    if (event.event_type === 'TOOL_CALL_STARTED') {
      const toolRunId = safeText(data['tool_run_id']).trim()
      if (!toolRunId) {
        return
      }
      const current = byRunId.get(toolRunId) ?? { toolRunId, toolId: '' }
      current.toolId = safeText(data['tool_id']).trim() || current.toolId
      current.method = safeText(data['method']).trim() || current.method
      current.path = safeText(data['path']).trim() || current.path
      current.startedAt = event.occurred_at
      byRunId.set(toolRunId, current)
      return
    }
    if (event.event_type === 'TOOL_CALL_FINISHED') {
      const toolRunId = safeText(data['tool_run_id']).trim()
      if (!toolRunId) {
        return
      }
      const current = byRunId.get(toolRunId) ?? { toolRunId, toolId: '' }
      current.toolId = safeText(data['tool_id']).trim() || current.toolId
      current.status = safeText(data['status']).trim() || current.status
      const httpStatusRaw = data['http_status']
      if (typeof httpStatusRaw === 'number') {
        current.httpStatus = httpStatusRaw
      } else if (typeof httpStatusRaw === 'string' && httpStatusRaw.trim()) {
        const parsed = Number(httpStatusRaw)
        current.httpStatus = Number.isNaN(parsed) ? current.httpStatus : parsed
      }
      current.finishedAt = event.occurred_at
      byRunId.set(toolRunId, current)
    }
  })
  return Array.from(byRunId.values()).sort((left, right) => {
    const leftTs = Date.parse(left.startedAt || left.finishedAt || '') || 0
    const rightTs = Date.parse(right.startedAt || right.finishedAt || '') || 0
    if (leftTs !== rightTs) {
      return leftTs - rightTs
    }
    return left.toolRunId.localeCompare(right.toolRunId)
  })
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
      project_id: activeDbName || null,
      pipeline_name: aiAgentContext.pipelineName || null,
      node_count: aiAgentContext.nodes.length,
      db_name: activeDbName || null,
      environment: import.meta.env.MODE || 'dev',
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
      include_tool_calls: true,
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

  const events = eventsPayload?.events ?? []
  const eventItems = useMemo(() => buildChatItemsFromEvents(events), [events])
  const toolCallOutline = useMemo(() => buildToolCallOutlineFromEvents(events), [events])

  const { data: contextItemsPayload, refetch: refreshContextItems } = useQuery({
    queryKey: ['agent-session-context', sessionId],
    queryFn: () => listAgentSessionContextItems(sessionId, { limit: 200, offset: 0 }),
    enabled: Boolean(sessionId) && isAgentVisible,
  })

  const pinnedItems = useMemo(() => {
    const items = contextItemsPayload?.context_items ?? []
    return items.filter((item) => Boolean(item?.metadata && item.metadata['pin'] === true))
  }, [contextItemsPayload])

  const pinnedMessageIds = useMemo(() => {
    const map = new Map<string, string>()
    pinnedItems.forEach((item) => {
      if (item.item_type !== 'message') {
        return
      }
      const ref = item.ref ?? {}
      const messageId = safeText(ref['message_id']).trim()
      if (messageId) {
        map.set(messageId, item.item_id)
      }
    })
    return map
  }, [pinnedItems])

  const pinnedToolRunIds = useMemo(() => {
    const map = new Map<string, string>()
    pinnedItems.forEach((item) => {
      if (item.item_type !== 'tool_call') {
        return
      }
      const ref = item.ref ?? {}
      const toolRunId = safeText(ref['tool_run_id']).trim()
      if (toolRunId) {
        map.set(toolRunId, item.item_id)
      }
    })
    return map
  }, [pinnedItems])

  const pinnedPlanIds = useMemo(() => {
    const map = new Map<string, string>()
    pinnedItems.forEach((item) => {
      if (item.item_type !== 'plan') {
        return
      }
      const ref = item.ref ?? {}
      const planId = safeText(ref['plan_id']).trim()
      if (planId) {
        map.set(planId, item.item_id)
      }
    })
    return map
  }, [pinnedItems])
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

  const handleNewConversation = useCallback(async () => {
    try {
      const created = await createAgentSession({ metadata: sessionMetadata })
      const nextId = created.session?.session_id
      if (!nextId) {
        throw new Error('Failed to create agent session')
      }
      sessionIdRef.current = nextId
      attachedDatasetIdsRef.current = new Set()
      setSessionId(nextId)
      setExtraItems([])
      setPlanDetails({})
      setPlanActions({})
      planFetchInFlightRef.current = new Set()
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to create agent session'
      const timestamp = new Date().toISOString()
      setExtraItems((current) => [
        ...current,
        { kind: 'message', id: buildId(), role: 'assistant', text: message, timestamp },
      ])
    }
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

  const handleToggleMessagePin = useCallback(
    async (item: ChatMessage) => {
      if (!sessionId) {
        return
      }
      const existingId = pinnedMessageIds.get(item.id)
      if (existingId) {
        await removeAgentSessionContextItem(sessionId, existingId)
        await refreshContextItems()
        return
      }
      await attachAgentSessionContextItem(sessionId, {
        item_type: 'message',
        include_mode: 'summary',
        ref: {
          message_id: item.id,
          role: item.role,
          content: item.text,
          occurred_at: item.timestamp,
        },
        metadata: { pin: true, source: 'chat' },
      })
      await refreshContextItems()
    },
    [sessionId, pinnedMessageIds, refreshContextItems],
  )

  const handleToggleToolCallPin = useCallback(
    async (call: ToolCallOutlineItem) => {
      if (!sessionId || !call.toolRunId) {
        return
      }
      const existingId = pinnedToolRunIds.get(call.toolRunId)
      if (existingId) {
        await removeAgentSessionContextItem(sessionId, existingId)
        await refreshContextItems()
        return
      }
      await attachAgentSessionContextItem(sessionId, {
        item_type: 'tool_call',
        include_mode: 'summary',
        ref: {
          tool_run_id: call.toolRunId,
          tool_id: call.toolId,
          method: call.method,
          path: call.path,
          status: call.status,
          http_status: call.httpStatus,
          started_at: call.startedAt,
          finished_at: call.finishedAt,
        },
        metadata: { pin: true, source: 'outline' },
      })
      await refreshContextItems()
    },
    [sessionId, pinnedToolRunIds, refreshContextItems],
  )

  const handleTogglePlanPin = useCallback(
    async (plan: PlanCard) => {
      if (!sessionId || !plan.planId) {
        return
      }
      const existingId = pinnedPlanIds.get(plan.planId)
      if (existingId) {
        await removeAgentSessionContextItem(sessionId, existingId)
        await refreshContextItems()
        return
      }
      await attachAgentSessionContextItem(sessionId, {
        item_type: 'plan',
        include_mode: 'summary',
        ref: {
          plan_id: plan.planId,
          goal: plan.goal,
          risk_level: plan.riskLevel,
          approval_request_id: plan.approvalRequestId,
          job_id: plan.jobId,
        },
        metadata: { pin: true, source: 'plan' },
      })
      await refreshContextItems()
    },
    [sessionId, pinnedPlanIds, refreshContextItems],
  )

  const handleTogglePinnedItemRemoval = useCallback(
    async (item: AgentSessionContextItem) => {
      if (!sessionId) {
        return
      }
      await removeAgentSessionContextItem(sessionId, item.item_id)
      await refreshContextItems()
    },
    [sessionId, refreshContextItems],
  )

  const handleSend = async () => {
    const text = draft.trim()
    if (!text || isSending) {
      return
    }
    setDraft('')
    setIsSending(true)
    let activeSession: string
    try {
      activeSession = await ensureSession()
      await syncDatasetContext(activeSession)
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to create agent session'
      const timestamp = new Date().toISOString()
      setExtraItems((current) => [
        ...current,
        { kind: 'message', id: buildId(), role: 'user', text, timestamp },
        { kind: 'message', id: buildId(), role: 'assistant', text: message, timestamp },
      ])
      setIsSending(false)
      return
    }
    const intentContext: Record<string, unknown> = {}
    if (aiAgentContext.nodes.length > 0) {
      intentContext.node_count = aiAgentContext.nodes.length
    }
    if (datasetIdsForNodes.length > 0) {
      intentContext.dataset_count = datasetIdsForNodes.length
    }
    const preferredLanguage = typeof navigator !== 'undefined' ? navigator.language : null
    let intent: AIIntentResponse
    try {
      intent = await aiIntent({
        question: text,
        db_name: activeDbName || null,
        project_name: aiAgentContext.projectName || null,
        pipeline_name: aiAgentContext.pipelineName || null,
        language: preferredLanguage,
        context: Object.keys(intentContext).length > 0 ? intentContext : null,
        session_id: activeSession,
      })
    } catch (error) {
      const timestamp = new Date().toISOString()
      const message = error instanceof Error ? error.message : 'AI intent routing failed'
      setExtraItems((current) => [
        ...current,
        { kind: 'message', id: buildId(), role: 'user', text, timestamp },
        { kind: 'message', id: buildId(), role: 'assistant', text: message, timestamp },
      ])
      setIsSending(false)
      return
    }

    if (intent.requires_clarification || intent.route === 'chat') {
      const timestamp = new Date().toISOString()
      const reply = resolveIntentReply(intent)
      if (!reply) {
        const message = intent.requires_clarification
          ? 'LLM intent response missing clarifying_question'
          : 'LLM intent response missing reply'
        setExtraItems((current) => [
          ...current,
          { kind: 'message', id: buildId(), role: 'user', text, timestamp },
          { kind: 'message', id: buildId(), role: 'assistant', text: message, timestamp },
        ])
        setIsSending(false)
        return
      }
      setExtraItems((current) => [
        ...current,
        { kind: 'message', id: buildId(), role: 'user', text, timestamp },
        { kind: 'message', id: buildId(), role: 'assistant', text: reply, timestamp },
      ])
      setIsSending(false)
      return
    }

    if (intent.route === 'query') {
      const timestamp = new Date().toISOString()
      setExtraItems((current) => [...current, { kind: 'message', id: buildId(), role: 'user', text, timestamp }])
      if (!activeDbName) {
        const reply = 'Missing active project/database for query.'
        setExtraItems((current) => [
          ...current,
          { kind: 'message', id: buildId(), role: 'assistant', text: reply, timestamp },
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
          session_id: activeSession,
        })
        const answer = extractAiAnswer(response)
        if (!answer) {
          throw new Error('LLM answer response missing answer')
        }
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

    try {
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
        const pinned = pinnedMessageIds.has(item.id)
        return (
          <div
            key={item.id}
            className={`ai-agent-message ${item.role === 'user' ? 'is-user' : 'is-assistant'}`}
          >
            <div className="ai-agent-message-body">{item.text}</div>
            <button
              type="button"
              className={`ai-agent-pin-btn ${pinned ? 'is-pinned' : ''}`}
              onClick={() => handleToggleMessagePin(item)}
              aria-label={pinned ? 'Unpin message' : 'Pin message'}
              title={pinned ? 'Unpin message' : 'Pin message'}
            >
              <Icon icon="pin" size={12} />
            </button>
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
      const planPinned = pinnedPlanIds.has(plan.planId)
      return (
        <div key={plan.id} className="ai-agent-plan-card">
          <div className="ai-agent-plan-header">
            <div className="ai-agent-plan-title">{plan.goal || 'Pipeline plan'}</div>
            <div className="ai-agent-plan-header-actions">
              <span className={`ai-agent-plan-status is-${formatStatusLabel(plan.status)}`}>
                {formatStatusLabel(plan.status)}
              </span>
              <button
                type="button"
                className={`ai-agent-pin-btn ${planPinned ? 'is-pinned' : ''}`}
                onClick={() => handleTogglePlanPin(plan)}
                aria-label={planPinned ? 'Unpin plan' : 'Pin plan'}
                title={planPinned ? 'Unpin plan' : 'Pin plan'}
              >
                <Icon icon="pin" size={12} />
              </button>
            </div>
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
  }, [
    handleApprovePlan,
    handleExecutePlan,
    handleRejectPlan,
    handleToggleMessagePin,
    handleTogglePlanPin,
    items,
    pinnedMessageIds,
    pinnedPlanIds,
    planActions,
    resolvePlanCard,
  ])

  return (
    <div className="ai-agent-panel-content">
      <div className="ai-agent-panel-header">
        <div className="ai-agent-panel-title">
          <Icon icon="predictive-analysis" size={16} />
          <span>AI Agent</span>
        </div>
        <div className="ai-agent-panel-actions">
          <Button
            small
            minimal
            icon="add"
            onClick={handleNewConversation}
            disabled={isSending}
          >
            New conversation
          </Button>
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
            <div className="ai-agent-pins">
              <div className="ai-agent-pins-title">Pinned</div>
              {pinnedItems.length > 0 ? (
                <div className="ai-agent-pins-list">
                  {pinnedItems.map((item) => (
                    <div key={item.item_id} className="ai-agent-pin-item">
                      <span className="ai-agent-pin-label">{resolvePinnedLabel(item)}</span>
                      <button
                        type="button"
                        className="ai-agent-pin-remove"
                        onClick={() => handleTogglePinnedItemRemoval(item)}
                        aria-label="Unpin item"
                        title="Unpin item"
                      >
                        <Icon icon="cross" size={12} />
                      </button>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="ai-agent-pin-empty">No pinned items yet.</div>
              )}
            </div>
          </div>
        ) : null}
        {toolCallOutline.length > 0 ? (
          <details className="ai-agent-outline" open>
            <summary>Outline · Tool calls ({toolCallOutline.length})</summary>
            <div className="ai-agent-outline-list">
              {toolCallOutline.map((call) => {
                const pinned = pinnedToolRunIds.has(call.toolRunId)
                const method = call.method ? `${call.method} ` : ''
                const path = call.path ? call.path : ''
                const status = call.status ? call.status.toLowerCase() : ''
                const httpStatus = call.httpStatus ? ` (${call.httpStatus})` : ''
                return (
                  <div key={call.toolRunId} className={`ai-agent-outline-item ${status ? `is-${status}` : ''}`}>
                    <div className="ai-agent-outline-main">
                      <span className="ai-agent-outline-tool">{call.toolId || 'tool'}</span>
                      <span className="ai-agent-outline-path">
                        {method}
                        {path}
                      </span>
                    </div>
                    <div className="ai-agent-outline-meta">
                      <span className="ai-agent-outline-status">
                        {status ? `${status}${httpStatus}` : 'running'}
                      </span>
                      <button
                        type="button"
                        className={`ai-agent-pin-btn ${pinned ? 'is-pinned' : ''}`}
                        onClick={() => handleToggleToolCallPin(call)}
                        aria-label={pinned ? 'Unpin tool call' : 'Pin tool call'}
                        title={pinned ? 'Unpin tool call' : 'Pin tool call'}
                      >
                        <Icon icon="pin" size={12} />
                      </button>
                    </div>
                  </div>
                )
              })}
            </div>
          </details>
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
