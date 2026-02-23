import { useState, useRef, useCallback, useEffect } from 'react'
import {
  Button,
  Card,
  Callout,
  FormGroup,
  HTMLSelect,
  InputGroup,
  Intent,
  Spinner,
  Tab,
  Tabs,
  Tag,
  Tooltip,
  HTMLTable,
} from '@blueprintjs/core'
import { useMutation } from '@tanstack/react-query'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { useRequestContext } from '../api/useRequestContext'
import { useAppStore } from '../store/useAppStore'
import {
  classifyAiIntent,
  translateQueryPlan,
  runAiQuery,
  runPipelineAgent,
  runOntologyAgent,
  searchContext7,
} from '../api/bff'

/* ── types ──────────────────────────────────────────── */
type ChatMessage = {
  id: string
  role: 'user' | 'assistant' | 'system'
  content: string
  timestamp: number
  metadata?: Record<string, unknown>
}

type AgentMode = 'query' | 'pipeline' | 'ontology' | 'search'

/* ── page ────────────────────────────────────────────── */
export const AIAssistantPage = () => {
  const ctx = useRequestContext()
  const project = useAppStore((s) => s.context.project)
  const dbName = project ?? ''

  const [agentMode, setAgentMode] = useState<AgentMode>('query')
  const [messages, setMessages] = useState<ChatMessage[]>([
    {
      id: 'sys-1',
      role: 'system',
      content: `AI Assistant ready. Connected to database: ${dbName || '(none)'}. Select an agent mode and start asking questions.`,
      timestamp: Date.now(),
    },
  ])
  const [input, setInput] = useState('')
  const [activeTab, setActiveTab] = useState('chat')
  const messagesEndRef = useRef<HTMLDivElement>(null)

  /* scroll to bottom on new messages */
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages.length])

  /* intent classification */
  const intentMut = useMutation({
    mutationFn: (text: string) => classifyAiIntent(ctx, { text, db_name: dbName }),
  })

  /* query agent */
  const queryMut = useMutation({
    mutationFn: (text: string) => runAiQuery(ctx, dbName, { query: text, natural_language: true }),
  })

  /* pipeline agent */
  const pipelineMut = useMutation({
    mutationFn: (text: string) => runPipelineAgent(ctx, { instruction: text, db_name: dbName }),
  })

  /* ontology agent */
  const ontologyMut = useMutation({
    mutationFn: (text: string) => runOntologyAgent(ctx, { instruction: text, db_name: dbName }),
  })

  /* knowledge search */
  const searchMut = useMutation({
    mutationFn: (text: string) => searchContext7(ctx, { query: text, db_name: dbName }),
  })

  /* translate query */
  const translateMut = useMutation({
    mutationFn: (text: string) => translateQueryPlan(ctx, dbName, { query: text }),
  })

  const isProcessing = queryMut.isPending || pipelineMut.isPending || ontologyMut.isPending || searchMut.isPending

  const addMessage = useCallback((role: ChatMessage['role'], content: string, metadata?: Record<string, unknown>) => {
    setMessages((prev) => [...prev, {
      id: `msg-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      role,
      content,
      timestamp: Date.now(),
      metadata,
    }])
  }, [])

  const handleSend = useCallback(async () => {
    const text = input.trim()
    if (!text || isProcessing) return

    setInput('')
    addMessage('user', text)

    if (!dbName) {
      addMessage('assistant', 'Please select a database/project first (from the Databases page).')
      return
    }

    try {
      /* first classify intent */
      const intent = await classifyAiIntent(ctx, { text, db_name: dbName }).catch(() => ({}))
      const intentType = String((intent as Record<string, unknown>).intent ?? agentMode)

      let result: Record<string, unknown> = {}
      let responseText = ''

      switch (agentMode) {
        case 'query': {
          /* try to translate to query plan first */
          const plan = await translateQueryPlan(ctx, dbName, { query: text }).catch(() => null)
          if (plan && Object.keys(plan).length > 0) {
            addMessage('system', `Query plan generated. Executing...`, { queryPlan: plan })
          }
          result = await runAiQuery(ctx, dbName, { query: text, natural_language: true })
          const rows = Array.isArray((result as Record<string, unknown>).data)
            ? (result as Record<string, unknown>).data as unknown[]
            : []
          responseText = rows.length > 0
            ? `Found ${rows.length} result(s). See the results panel for details.`
            : (typeof result.answer === 'string' ? result.answer : 'Query executed. Check results panel for output.')
          break
        }
        case 'pipeline': {
          result = await runPipelineAgent(ctx, { instruction: text, db_name: dbName })
          responseText = typeof result.message === 'string'
            ? result.message
            : typeof result.status === 'string'
            ? `Pipeline agent completed with status: ${result.status}`
            : 'Pipeline agent task submitted. Check the results panel.'
          break
        }
        case 'ontology': {
          result = await runOntologyAgent(ctx, { instruction: text, db_name: dbName })
          responseText = typeof result.message === 'string'
            ? result.message
            : typeof result.status === 'string'
            ? `Ontology agent completed with status: ${result.status}`
            : 'Ontology agent task submitted. Check the results panel.'
          break
        }
        case 'search': {
          result = await searchContext7(ctx, { query: text, db_name: dbName })
          const searchResults = Array.isArray(result.results) ? result.results as unknown[] : []
          responseText = searchResults.length > 0
            ? `Found ${searchResults.length} relevant knowledge entries.`
            : 'No results found in knowledge base.'
          break
        }
      }

      addMessage('assistant', responseText, { result, intentType })
    } catch (err) {
      const errMsg = err instanceof Error ? err.message : 'An error occurred'
      addMessage('assistant', `Error: ${errMsg}. The backend API may not be configured for this agent mode.`)
    }
  }, [input, isProcessing, dbName, agentMode, ctx, addMessage])

  const lastResult = messages.filter((m) => m.metadata?.result).pop()?.metadata?.result as Record<string, unknown> | undefined

  return (
    <div>
      <PageHeader
        title="AI Assistant"
        subtitle={`Natural language queries, pipeline generation & ontology design${dbName ? ` · ${dbName}` : ''}`}
        actions={
          <Button
            minimal
            icon="trash"
            onClick={() => setMessages([{
              id: 'sys-clear',
              role: 'system',
              content: 'Chat cleared. Ready for new conversation.',
              timestamp: Date.now(),
            }])}
          >
            Clear Chat
          </Button>
        }
      />

      {!dbName && (
        <Callout intent={Intent.WARNING} icon="warning-sign" style={{ marginBottom: 12 }}>
          No database selected. Please go to the Databases page and select a project first.
        </Callout>
      )}

      {/* Agent Mode Selector — Card-based */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 10, marginBottom: 12 }}>
        {([
          { mode: 'query' as AgentMode, icon: 'search', label: 'Query', desc: 'Ask questions about your data in natural language', color: '#137CBD' },
          { mode: 'pipeline' as AgentMode, icon: 'data-lineage', label: 'Pipeline', desc: 'Generate data transformation pipelines', color: '#D9822B' },
          { mode: 'ontology' as AgentMode, icon: 'diagram-tree', label: 'Ontology', desc: 'Design and create data models', color: '#0F9960' },
          { mode: 'search' as AgentMode, icon: 'book', label: 'Search', desc: 'Search the knowledge base', color: '#8F398F' },
        ] as const).map((item) => (
          <Card
            key={item.mode}
            interactive
            onClick={() => setAgentMode(item.mode)}
            style={{
              padding: '14px 16px',
              border: agentMode === item.mode ? `2px solid ${item.color}` : '2px solid transparent',
              background: agentMode === item.mode ? `${item.color}10` : undefined,
              cursor: 'pointer',
              transition: 'border-color 0.15s, background 0.15s',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
              <Tag icon={item.icon as any} intent={agentMode === item.mode ? Intent.PRIMARY : Intent.NONE} minimal>
                {item.label}
              </Tag>
              {agentMode === item.mode && <Tag minimal intent={Intent.SUCCESS} icon="tick" style={{ fontSize: 10 }}>Active</Tag>}
            </div>
            <div style={{ fontSize: 11, opacity: 0.7, lineHeight: 1.4 }}>{item.desc}</div>
          </Card>
        ))}
      </div>

      <div className="two-col-grid">
        {/* Left: Chat */}
        <Card style={{ display: 'flex', flexDirection: 'column', minHeight: 500 }}>
          <div className="card-title">
            <Tooltip content="Chat with the AI assistant using natural language" placement="top">
              <span className="tooltip-label">Chat</span>
            </Tooltip>
          </div>

          {/* Messages */}
          <div style={{ flex: 1, overflowY: 'auto', maxHeight: 400, marginBottom: 12 }}>
            {messages.map((msg) => (
              <div
                key={msg.id}
                style={{
                  marginBottom: 12,
                  display: 'flex',
                  flexDirection: 'column',
                  alignItems: msg.role === 'user' ? 'flex-end' : 'flex-start',
                }}
              >
                <div style={{ display: 'flex', gap: 6, alignItems: 'center', marginBottom: 4 }}>
                  <Tag
                    minimal
                    intent={
                      msg.role === 'user' ? Intent.PRIMARY :
                      msg.role === 'assistant' ? Intent.SUCCESS :
                      Intent.NONE
                    }
                  >
                    {msg.role === 'user' ? 'You' : msg.role === 'assistant' ? 'AI' : 'System'}
                  </Tag>
                  <span style={{ fontSize: 10, opacity: 0.5 }}>
                    {new Date(msg.timestamp).toLocaleTimeString()}
                  </span>
                </div>
                <div
                  style={{
                    padding: '8px 12px',
                    borderRadius: 8,
                    maxWidth: '90%',
                    fontSize: 13,
                    lineHeight: 1.5,
                    background: msg.role === 'user' ? '#137cbd20' : msg.role === 'assistant' ? '#0f996020' : '#f5f8fa',
                    border: '1px solid',
                    borderColor: msg.role === 'user' ? '#137cbd40' : msg.role === 'assistant' ? '#0f996040' : '#d8e1e8',
                  }}
                >
                  {msg.content}
                </div>
              </div>
            ))}
            <div ref={messagesEndRef} />
          </div>

          {/* Input */}
          <div className="form-row" style={{ gap: 8 }}>
            <InputGroup
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder={
                agentMode === 'query' ? 'Ask a question about your data...' :
                agentMode === 'pipeline' ? 'Describe the pipeline you want to create...' :
                agentMode === 'ontology' ? 'Describe the data model you want...' :
                'Search the knowledge base...'
              }
              onKeyDown={(e) => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); handleSend() } }}
              disabled={isProcessing}
              fill
              large
              leftIcon={
                agentMode === 'query' ? 'search' :
                agentMode === 'pipeline' ? 'data-lineage' :
                agentMode === 'ontology' ? 'diagram-tree' :
                'book'
              }
              rightElement={
                isProcessing ? <Spinner size={16} /> : undefined
              }
            />
            <Button
              intent={Intent.PRIMARY}
              icon="arrow-right"
              loading={isProcessing}
              disabled={!input.trim() || !dbName}
              onClick={handleSend}
              large
            >
              Send
            </Button>
          </div>

          {/* Quick Suggestions */}
          <div style={{ marginTop: 8 }}>
            <div style={{ fontSize: 11, opacity: 0.5, marginBottom: 4 }}>Try:</div>
            <div className="form-row" style={{ gap: 4, flexWrap: 'wrap' }}>
              {agentMode === 'query' && (
                <>
                  <Tag minimal interactive onClick={() => setInput('Show all object types')}>Show all object types</Tag>
                  <Tag minimal interactive onClick={() => setInput('Count instances by type')}>Count instances by type</Tag>
                  <Tag minimal interactive onClick={() => setInput('Find recent changes')}>Find recent changes</Tag>
                </>
              )}
              {agentMode === 'pipeline' && (
                <>
                  <Tag minimal interactive onClick={() => setInput('Create a pipeline that filters null values')}>Filter nulls</Tag>
                  <Tag minimal interactive onClick={() => setInput('Create a join pipeline for two datasets')}>Join datasets</Tag>
                  <Tag minimal interactive onClick={() => setInput('Aggregate sales by month')}>Aggregate by month</Tag>
                </>
              )}
              {agentMode === 'ontology' && (
                <>
                  <Tag minimal interactive onClick={() => setInput('Create an Employee object type')}>Create Employee type</Tag>
                  <Tag minimal interactive onClick={() => setInput('Add a relationship between Employee and Department')}>Add relationship</Tag>
                  <Tag minimal interactive onClick={() => setInput('Suggest schema for my orders data')}>Suggest schema</Tag>
                </>
              )}
              {agentMode === 'search' && (
                <>
                  <Tag minimal interactive onClick={() => setInput('How to configure data sources')}>Configure data sources</Tag>
                  <Tag minimal interactive onClick={() => setInput('Best practices for ontology design')}>Ontology best practices</Tag>
                  <Tag minimal interactive onClick={() => setInput('Pipeline transform types')}>Transform types</Tag>
                </>
              )}
            </div>
          </div>
        </Card>

        {/* Right: Results */}
        <Card>
          <Tabs selectedTabId={activeTab} onChange={(id) => setActiveTab(id as string)}>
            <Tab id="chat" title="Latest Result" panel={
              <div style={{ marginTop: 8 }}>
                {lastResult ? (
                  <ResultView data={lastResult} />
                ) : (
                  <Callout icon="info-sign">
                    Send a message to see results here.
                  </Callout>
                )}
              </div>
            } />
            <Tab id="history" title="History" panel={
              <div style={{ marginTop: 8 }}>
                {messages.filter((m) => m.metadata?.result).length === 0 ? (
                  <Callout>No results yet.</Callout>
                ) : (
                  <div>
                    {messages.filter((m) => m.metadata?.result).reverse().map((msg) => (
                      <Card key={msg.id} style={{ marginBottom: 8 }}>
                        <div className="form-row" style={{ gap: 8, marginBottom: 8 }}>
                          <Tag minimal>{new Date(msg.timestamp).toLocaleTimeString()}</Tag>
                          {typeof msg.metadata?.intentType === 'string' && (
                            <Tag minimal intent={Intent.PRIMARY}>{msg.metadata.intentType}</Tag>
                          )}
                        </div>
                        <div style={{ fontSize: 12, marginBottom: 8 }}>{msg.content}</div>
                        <JsonViewer value={msg.metadata?.result ?? {}} />
                      </Card>
                    ))}
                  </div>
                )}
              </div>
            } />
            <Tab id="tools" title="Tools" panel={
              <div style={{ marginTop: 8 }}>
                <Card style={{ marginBottom: 12 }}>
                  <div className="card-title">Query Translator</div>
                  <p style={{ fontSize: 12, opacity: 0.7 }}>
                    Translate natural language into a structured query plan.
                  </p>
                  <QueryTranslatorTool ctx={ctx} dbName={dbName} />
                </Card>
              </div>
            } />
          </Tabs>
        </Card>
      </div>
    </div>
  )
}

/* ── Query Translator Tool ──────────────────────────── */
const QueryTranslatorTool = ({
  ctx,
  dbName,
}: {
  ctx: ReturnType<typeof useRequestContext>
  dbName: string
}) => {
  const [queryInput, setQueryInput] = useState('')

  const translateMut = useMutation({
    mutationFn: () => translateQueryPlan(ctx, dbName, { query: queryInput }),
  })

  return (
    <div>
      <FormGroup label="Natural Language Query">
        <InputGroup
          value={queryInput}
          onChange={(e) => setQueryInput(e.target.value)}
          placeholder="e.g. Find all employees hired after 2025..."
          onKeyDown={(e) => { if (e.key === 'Enter') translateMut.mutate() }}
        />
      </FormGroup>
      <Button
        intent={Intent.PRIMARY}
        icon="translate"
        loading={translateMut.isPending}
        disabled={!queryInput.trim() || !dbName}
        onClick={() => translateMut.mutate()}
      >
        Translate
      </Button>
      {translateMut.error && (
        <Callout intent={Intent.DANGER} style={{ marginTop: 8 }}>Translation failed.</Callout>
      )}
      {translateMut.data && (
        <div style={{ marginTop: 12 }}>
          <div className="card-title" style={{ fontSize: 13, marginBottom: 8 }}>Query Plan</div>
          <JsonViewer value={translateMut.data} />
        </div>
      )}
    </div>
  )
}

/* ── Result View (auto-detect tabular data) ─────────── */
const ResultView = ({ data }: { data: Record<string, unknown> }) => {
  /* Try to find a tabular array in the result */
  const rows = Array.isArray(data.data) ? data.data as Record<string, unknown>[]
    : Array.isArray(data.results) ? data.results as Record<string, unknown>[]
    : Array.isArray(data.objects) ? data.objects as Record<string, unknown>[]
    : Array.isArray(data.instances) ? data.instances as Record<string, unknown>[]
    : null

  if (rows && rows.length > 0 && typeof rows[0] === 'object' && rows[0] !== null) {
    const keys = Object.keys(rows[0]).filter((k) => !k.startsWith('_')).slice(0, 10)
    if (keys.length > 0) {
      return (
        <div>
          <div className="form-row" style={{ marginBottom: 8 }}>
            <Tag icon="th">{rows.length} rows</Tag>
            <Tag icon="column-layout" minimal>{keys.length} columns</Tag>
          </div>
          <div style={{ overflow: 'auto', maxHeight: 400 }}>
            <HTMLTable compact striped style={{ width: '100%' }}>
              <thead>
                <tr>{keys.map((k) => <th key={k}>{k}</th>)}</tr>
              </thead>
              <tbody>
                {rows.slice(0, 100).map((row, i) => (
                  <tr key={i}>
                    {keys.map((k) => (
                      <td key={k} style={{ fontSize: 12, maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {row[k] === null || row[k] === undefined ? '—'
                          : typeof row[k] === 'object' ? JSON.stringify(row[k]).slice(0, 50)
                          : String(row[k])}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </HTMLTable>
          </div>
          {rows.length > 100 && (
            <div style={{ fontSize: 12, opacity: 0.6, padding: 8 }}>Showing 100 of {rows.length} rows</div>
          )}
        </div>
      )
    }
  }

  /* Fallback to JSON viewer */
  return <JsonViewer value={data} />
}
