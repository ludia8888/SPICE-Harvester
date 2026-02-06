import { Button, Icon } from '@blueprintjs/core'
import { useQuery } from '@tanstack/react-query'
import { useMemo, useRef, useState } from 'react'
import { flushSync } from 'react-dom'
import { listDatasets, runPipelineAgent, type DatasetRecord } from '../api/bff'
import { UploadFilesDialog } from '../components/UploadFilesDialog'
import { useAppStore } from '../state/store'

type ChatMessage = {
  id: string
  role: 'user' | 'assistant'
  text: string
  timestamp?: string
}

const buildId = () => `msg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`

const safeText = (value: unknown) => {
  if (typeof value === 'string') {
    return value
  }
  if (value == null) {
    return ''
  }
  return String(value)
}

const extractQuestions = (items: unknown) => {
  const questionItems = Array.isArray(items) ? items : []
  return questionItems
    .map((item) => {
      if (!item || typeof item !== 'object') {
        return ''
      }
      const ref = item as Record<string, unknown>
      return safeText(ref['question'] ?? ref['text']).trim()
    })
    .filter(Boolean)
}

const buildPipelineReply = (response: Record<string, unknown>, status: string, runId: string, planId: string) => {
  // If the agent provided a natural language message, use that as the primary response
  const agentMessage = safeText(response['message']).trim()
  if (agentMessage) {
    const parts: string[] = [agentMessage]
    const questions = extractQuestions(response['questions'])
    if (questions.length > 0) {
      parts.push(`\n---\n재질문: ${questions.join(' ')}`)
    }
    const validationWarnings = Array.isArray(response['validation_warnings']) ? response['validation_warnings'] : []
    const warningText = validationWarnings.map((item) => safeText(item).trim()).filter((item) => item)
    if (warningText.length > 0) {
      parts.push(`\n⚠️ ${warningText.slice(0, 3).join(' | ')}`)
    }
    return parts.join('')
  }

  // Fallback: structured response format
  const summaryParts = [`Pipeline agent ${status}.`]
  if (runId) {
    summaryParts.push(`run_id=${runId}`)
  }
  if (planId) {
    summaryParts.push(`plan_id=${planId}`)
  }

  const details: string[] = []

  // Include planner notes as natural language if available
  const planner = response['planner']
  if (planner && typeof planner === 'object') {
    const plannerNotes = (planner as Record<string, unknown>)['notes']
    if (Array.isArray(plannerNotes) && plannerNotes.length > 0) {
      details.push(plannerNotes.map((n) => safeText(n).trim()).filter(Boolean).join('\n'))
    }
  }

  const questions = extractQuestions(response['questions'])
  if (questions.length > 0) {
    details.push(`재질문: ${questions.join(' ')}`)
  }

  const validationErrors = Array.isArray(response['validation_errors']) ? response['validation_errors'] : []
  const validationText = validationErrors.map((item) => safeText(item).trim()).filter((item) => item)
  if (validationText.length > 0) {
    details.push(`검증 오류: ${validationText.slice(0, 2).join(' | ')}`)
  }

  return details.length > 0 ? `${summaryParts.join(' ')}\n${details.join('\n')}` : summaryParts.join(' ')
}

export const AIAgentPage = () => {
  const pipelineContext = useAppStore((state) => state.pipelineContext)
  const pipelineAgentRequest = useAppStore((state) => state.pipelineAgentRequest)
  const setPipelineAgentRequest = useAppStore((state) => state.setPipelineAgentRequest)
  const pipelineAgentQuestions = useAppStore((state) => state.pipelineAgentQuestions)
  const setPipelineAgentQuestions = useAppStore((state) => state.setPipelineAgentQuestions)
  const setPipelineAgentRun = useAppStore((state) => state.setPipelineAgentRun)

  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [draft, setDraft] = useState('')
  const [isSending, setIsSending] = useState(false)
  const [isUploadOpen, setUploadOpen] = useState(false)

  const pendingReplyIdRef = useRef<string | null>(null)
  const isSendingRef = useRef(false)

  const activeDbName = pipelineContext?.folderId ?? ''

  const { data: datasets = [] } = useQuery({
    queryKey: ['datasets', activeDbName],
    queryFn: () => listDatasets(activeDbName),
    enabled: Boolean(activeDbName),
  })

  const resolveAssistantMessage = (reply: string, pendingId: string) => {
    const timestamp = new Date().toISOString()
    setMessages((current) => {
      const next = current.map((item) => {
        if (item.id === pendingId) {
          return { ...item, text: reply, timestamp }
        }
        return item
      })
      return next
    })
    pendingReplyIdRef.current = null
  }

  const handleSend = async () => {
    const text = draft.trim()
    if (!text || isSending || isSendingRef.current) {
      return
    }

    if (!activeDbName) {
      return
    }

    isSendingRef.current = true
    const timestamp = new Date().toISOString()
    const pendingId = buildId()
    pendingReplyIdRef.current = pendingId
    flushSync(() => {
      setDraft('')
      setIsSending(true)
      setMessages((current) => [
        ...current,
        { id: buildId(), role: 'user', text, timestamp },
        { id: pendingId, role: 'assistant', text: '처리 중입니다...', timestamp },
      ])
    })

    const finishSend = () => {
      isSendingRef.current = false
      setIsSending(false)
    }

    if (pipelineAgentQuestions.length > 0 && pipelineAgentRequest) {
      const answers: Record<string, unknown> = {}
      if (pipelineAgentQuestions.length === 1) {
        const question = pipelineAgentQuestions[0]
        const questionId = safeText(question?.['id']).trim() || 'answer'
        answers[questionId] = text
      } else {
        answers['freeform'] = text
        answers['questions'] = pipelineAgentQuestions
      }
      try {
        const response = await runPipelineAgent({
          ...pipelineAgentRequest,
          answers,
        })
        setPipelineAgentRun(response)
        const status = safeText(response['status']).trim() || 'unknown'
        const runId = safeText(response['run_id']).trim()
        const planId = safeText(response['plan_id']).trim()
        const questionItems = Array.isArray(response['questions']) ? response['questions'] : []
        setPipelineAgentQuestions(questionItems)
        const reply = buildPipelineReply(response, status, runId, planId)
        resolveAssistantMessage(reply, pendingId)
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Pipeline agent failed'
        resolveAssistantMessage(message, pendingId)
      } finally {
        finishSend()
      }
      return
    }

    const fallbackDatasetIds = datasets.map((dataset) => dataset.dataset_id)
    const uniqueDatasetIds = Array.from(new Set(fallbackDatasetIds.filter(Boolean)))
    if (uniqueDatasetIds.length === 0) {
      resolveAssistantMessage(
        'No datasets available for pipeline agent. Select datasets or upload datasets first.',
        pendingId,
      )
      finishSend()
      return
    }

    const requestPayload = {
      goal: text,
      data_scope: {
        db_name: activeDbName,
        dataset_ids: uniqueDatasetIds,
      },
      max_transform: 2,
      max_cleansing: 2,
      max_repairs: 2,
    }
    setPipelineAgentRequest(requestPayload)
    setPipelineAgentQuestions([])
    try {
      const response = await runPipelineAgent(requestPayload)
      setPipelineAgentRun(response)
      const status = safeText(response['status']).trim() || 'unknown'
      const runId = safeText(response['run_id']).trim()
      const planId = safeText(response['plan_id']).trim()
      const questionItems = Array.isArray(response['questions']) ? response['questions'] : []
      setPipelineAgentQuestions(questionItems)
      const reply = buildPipelineReply(response, status, runId, planId)
      resolveAssistantMessage(reply, pendingId)
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Pipeline agent failed'
      resolveAssistantMessage(message, pendingId)
    } finally {
      finishSend()
    }
  }

  const canSend = draft.trim().length > 0 && !isSending

  return (
    <div className="ai-agent">
      <div className="ai-agent__header">
        <div className="ai-agent__header-left">
          <Icon icon="lab-test" />
          <div className="ai-agent__title">Pipeline Agent</div>
          <div className="ai-agent__subtitle">{activeDbName ? `db=${activeDbName}` : 'Select a project/db first'}</div>
        </div>
        <div className="ai-agent__header-right">
          <Button icon="upload" minimal onClick={() => setUploadOpen(true)} disabled={!activeDbName}>
            Upload
          </Button>
        </div>
      </div>

      <div className="ai-agent__body">
        <div className="ai-agent__messages">
          {messages.map((msg) => (
            <div key={msg.id} className={`ai-agent__message ai-agent__message--${msg.role}`}>
              <div className="ai-agent__message-role">{msg.role}</div>
              <div className="ai-agent__message-text">{msg.text}</div>
            </div>
          ))}
        </div>

        <form
          className="ai-agent__composer"
          onSubmit={(event) => {
            event.preventDefault()
            if (canSend) {
              handleSend()
            }
          }}
        >
          <textarea
            value={draft}
            onChange={(event) => setDraft(event.target.value)}
            placeholder="자연어로 ETL 목표를 입력하세요. 예: null check 해줘 / 주문+아이템 조인해서 카테고리별 매출 Top10 만들어줘"
            rows={3}
            disabled={!activeDbName}
          />
          <div className="ai-agent__composer-actions">
            <Button intent="primary" onClick={handleSend} disabled={!canSend || !activeDbName}>
              Send
            </Button>
          </div>
        </form>
      </div>

      <UploadFilesDialog isOpen={isUploadOpen} onClose={() => setUploadOpen(false)} activeFolderId={activeDbName || null} />
    </div>
  )
}
