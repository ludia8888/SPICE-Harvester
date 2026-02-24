import { useState, useRef, useEffect, useCallback } from 'react'
import { Button, Icon, Intent, Spinner } from '@blueprintjs/core'

type ChatMessage = {
  id: string
  role: 'user' | 'assistant'
  content: string
  timestamp: number
}

type Props = {
  project: string | null
}

const SUGGESTIONS = [
  '이 파이프라인의 데이터 흐름을 분석해줘',
  '새로운 Transform 노드를 추가해줘',
  '현재 파이프라인의 문제점을 찾아줘',
  '데이터셋 스키마를 검증해줘',
]

export const BuilderAgentChat = ({ project }: Props) => {
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [input, setInput] = useState('')
  const [sending, setSending] = useState(false)
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages.length])

  const resetTextareaHeight = useCallback(() => {
    const el = textareaRef.current
    if (el) {
      el.style.height = 'auto'
      el.style.height = `${Math.min(el.scrollHeight, 160)}px`
    }
  }, [])

  const addMessage = useCallback(
    (role: 'user' | 'assistant', content: string) => {
      setMessages((prev) => [
        ...prev,
        { id: `${role}-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`, role, content, timestamp: Date.now() },
      ])
    },
    [],
  )

  const handleSend = useCallback(
    async (text?: string) => {
      const msg = (text ?? input).trim()
      if (!msg || sending) return
      addMessage('user', msg)
      setInput('')
      if (textareaRef.current) {
        textareaRef.current.style.height = 'auto'
      }

      setSending(true)
      // Simulate agent response (replace with real API later)
      await new Promise((r) => setTimeout(r, 1200))
      addMessage(
        'assistant',
        `네, "${msg.slice(0, 40)}${msg.length > 40 ? '...' : ''}" 요청을 처리하겠습니다.\n\n현재 ${project ?? '프로젝트'} 컨텍스트에서 분석 중입니다. 이 기능은 곧 실제 AI 에이전트와 연결될 예정입니다.`,
      )
      setSending(false)
    },
    [input, sending, addMessage, project],
  )

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault()
        handleSend()
      }
    },
    [handleSend],
  )

  if (!project) {
    return (
      <div className="builder-agent-chat">
        <div className="builder-agent-body">
          <div className="builder-agent-center">
            <div className="builder-agent-welcome-icon">
              <Icon icon="cube" size={28} />
            </div>
            <h2 className="builder-agent-welcome-title">Builder Agent</h2>
            <p className="builder-agent-welcome-hint">프로젝트를 먼저 선택하세요</p>
          </div>
        </div>
      </div>
    )
  }

  const hasMessages = messages.length > 0

  return (
    <div className="builder-agent-chat">
      <div className="builder-agent-body">
        <div className="builder-agent-scroll-inner">
          {!hasMessages ? (
            <div className="builder-agent-center">
              <div className="builder-agent-welcome-icon">
                <Icon icon="cube" size={28} />
              </div>
              <h2 className="builder-agent-welcome-title">Builder Agent</h2>
              <p className="builder-agent-welcome-hint">
                {project}의 파이프라인을 함께 빌드하세요
              </p>
              <div className="builder-agent-suggestions">
                {SUGGESTIONS.map((s) => (
                  <button
                    key={s}
                    className="builder-agent-suggestion"
                    onClick={() => handleSend(s)}
                  >
                    {s}
                  </button>
                ))}
              </div>
            </div>
          ) : (
            <div className="ai-agent-messages">
              {messages.map((m) => (
                <div
                  key={m.id}
                  className={`ai-agent-message ${m.role === 'user' ? 'is-user' : 'is-assistant'}`}
                >
                  <span className="ai-agent-message-body">{m.content}</span>
                </div>
              ))}
              {sending && (
                <div className="ai-agent-message is-assistant">
                  <Spinner size={16} />
                </div>
              )}
              <div ref={messagesEndRef} />
            </div>
          )}
        </div>
      </div>

      <div className="builder-agent-input-bar">
        <div className="builder-agent-input-inner">
          <textarea
            ref={textareaRef}
            className="builder-agent-textarea"
            placeholder="메시지를 입력하세요..."
            value={input}
            rows={1}
            disabled={sending}
            onChange={(e) => {
              setInput(e.target.value)
              resetTextareaHeight()
            }}
            onKeyDown={handleKeyDown}
          />
          <Button
            className="builder-agent-send-btn"
            intent={Intent.PRIMARY}
            icon="arrow-up"
            minimal
            disabled={!input.trim() || sending}
            onClick={() => handleSend()}
          />
        </div>
      </div>
    </div>
  )
}
