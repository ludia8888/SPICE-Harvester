import { useEffect, useMemo, useState } from 'react'
import { Button, Card, FormGroup, H3, HTMLSelect, InputGroup, Spinner, Switch, Text, TextArea } from '@blueprintjs/core'
import { useQuery } from '@tanstack/react-query'
import { listDatabases, approveAgentPlan, compileAgentPlan, executeAgentPlan, type AgentClarificationQuestion, type AgentPlan, type DatabaseRecord } from '../api/bff'

type FolderOption = { id: string; name: string }

const normalizeFolder = (record: DatabaseRecord | string): FolderOption | null => {
  if (typeof record === 'string') {
    return { id: record, name: record }
  }
  const id = record.name || record.db_name || record.id
  if (!id) {
    return null
  }
  return {
    id,
    name: record.display_name || record.label || id,
  }
}

type CompileResult = {
  status?: string
  plan_id?: string
  plan?: AgentPlan | null
  questions?: AgentClarificationQuestion[]
  validation_errors?: string[]
  validation_warnings?: string[]
  compilation_report?: Record<string, unknown> | null
  planner?: Record<string, unknown> | null
}

const coerceAnswerValue = (question: AgentClarificationQuestion, raw: unknown): unknown => {
  const t = String(question.type || 'string').toLowerCase()
  if (t === 'boolean') {
    return Boolean(raw)
  }
  if (t === 'number') {
    const num = typeof raw === 'number' ? raw : Number.parseFloat(String(raw))
    return Number.isFinite(num) ? num : question.default ?? null
  }
  if (t === 'object') {
    if (typeof raw !== 'string') {
      return raw
    }
    try {
      return JSON.parse(raw)
    } catch {
      return raw
    }
  }
  return typeof raw === 'string' ? raw : String(raw ?? '')
}

export const AIAgentPage = () => {
  const { data: databasesRaw = [], isLoading: isLoadingDatabases } = useQuery({
    queryKey: ['databases'],
    queryFn: listDatabases,
  })

  const databases = useMemo(
    () => databasesRaw.map(normalizeFolder).filter((item): item is FolderOption => Boolean(item)),
    [databasesRaw],
  )

  const [activeDbName, setActiveDbName] = useState<string>('')
  const [branch, setBranch] = useState('main')
  const [goal, setGoal] = useState('')
  const [compileResult, setCompileResult] = useState<CompileResult | null>(null)
  const [answers, setAnswers] = useState<Record<string, unknown>>({})
  const [isCompiling, setCompiling] = useState(false)
  const [compileError, setCompileError] = useState<string | null>(null)
  const [isExecuting, setExecuting] = useState(false)
  const [executionResult, setExecutionResult] = useState<Record<string, unknown> | null>(null)
  const [executionError, setExecutionError] = useState<string | null>(null)

  useEffect(() => {
    if (!activeDbName && databases.length > 0) {
      setActiveDbName(databases[0].id)
    }
  }, [activeDbName, databases])

  const questions = compileResult?.questions ?? []
  const plan = compileResult?.plan ?? null

  const handleCompile = () => {
    const trimmed = goal.trim()
    if (!trimmed) {
      setCompileError('Goal is required.')
      return
    }
    const run = async () => {
      setCompiling(true)
      setCompileError(null)
      setExecutionResult(null)
      setExecutionError(null)
      try {
        const normalizedAnswers: Record<string, unknown> = {}
        questions.forEach((q) => {
          const raw = answers[q.id]
          if (raw === undefined || raw === null || raw === '') {
            if (q.default !== undefined) {
              normalizedAnswers[q.id] = q.default
            }
            return
          }
          normalizedAnswers[q.id] = coerceAnswerValue(q, raw)
        })

        const result = await compileAgentPlan({
          goal: trimmed,
          data_scope: { db_name: activeDbName || undefined, branch: branch.trim() || undefined },
          answers: Object.keys(normalizedAnswers).length ? normalizedAnswers : undefined,
        })
        setCompileResult(result)

        const nextQuestions = result.questions ?? []
        if (nextQuestions.length > 0) {
          setAnswers((current) => {
            const next = { ...current }
            nextQuestions.forEach((q) => {
              if (next[q.id] === undefined && q.default !== undefined) {
                next[q.id] = q.type === 'object' ? JSON.stringify(q.default, null, 2) : q.default
              }
            })
            return next
          })
        }
      } catch (error) {
        setCompileError(error instanceof Error ? error.message : 'Failed to compile plan')
      } finally {
        setCompiling(false)
      }
    }
    void run()
  }

  const handleExecute = () => {
    if (!compileResult?.plan_id) {
      setExecutionError('Compile a plan first.')
      return
    }
    const run = async () => {
      setExecuting(true)
      setExecutionError(null)
      setExecutionResult(null)
      try {
        if (plan?.requires_approval) {
          await approveAgentPlan(compileResult.plan_id!, { decision: 'APPROVED', comment: 'Approved from UI' })
        }
        const result = await executeAgentPlan(compileResult.plan_id!)
        setExecutionResult(result)
      } catch (error) {
        setExecutionError(error instanceof Error ? error.message : 'Failed to execute plan')
      } finally {
        setExecuting(false)
      }
    }
    void run()
  }

  return (
    <div className="page">
      <H3>AI Agent</H3>

      <Card className="card">
        <div className="card-title">Context</div>
        {isLoadingDatabases ? (
          <Spinner size={18} />
        ) : (
          <div className="grid" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))' }}>
            <FormGroup label="Project">
              <HTMLSelect
                fill
                value={activeDbName}
                onChange={(event) => setActiveDbName(event.currentTarget.value)}
                options={databases.map((db) => ({ value: db.id, label: db.name }))}
              />
            </FormGroup>
            <FormGroup label="Branch">
              <InputGroup value={branch} onChange={(event) => setBranch(event.currentTarget.value)} />
            </FormGroup>
          </div>
        )}
        <Text className="card-meta">LLM generates a typed plan. Server enforces allowlist + approval before writes.</Text>
      </Card>

      <Card className="card">
        <div className="card-title">Goal</div>
        <FormGroup label="What do you want to do?">
          <TextArea
            fill
            value={goal}
            placeholder='e.g. "Create a pipeline that cleanses dataset A and produces dataset B, then run objectify."'
            onChange={(event) => setGoal(event.currentTarget.value)}
            rows={4}
          />
        </FormGroup>

        {questions.length > 0 ? (
          <>
            <Text className="card-title">Clarifications</Text>
            {questions.map((q) => {
              const qType = String(q.type || 'string').toLowerCase()
              const value = answers[q.id]
              if (qType === 'boolean') {
                return (
                  <Switch
                    key={q.id}
                    checked={Boolean(value)}
                    label={q.question}
                    onChange={() => setAnswers((cur) => ({ ...cur, [q.id]: !Boolean(cur[q.id]) }))}
                  />
                )
              }
              if (qType === 'enum') {
                return (
                  <FormGroup key={q.id} label={q.question}>
                    <HTMLSelect
                      fill
                      value={String(value ?? '')}
                      onChange={(event) => setAnswers((cur) => ({ ...cur, [q.id]: event.currentTarget.value }))}
                      options={[{ value: '', label: 'Select…' }, ...(q.options ?? []).map((opt) => ({ value: opt, label: opt }))]}
                    />
                  </FormGroup>
                )
              }
              if (qType === 'object') {
                return (
                  <FormGroup key={q.id} label={q.question}>
                    <TextArea
                      fill
                      rows={4}
                      value={typeof value === 'string' ? value : JSON.stringify(value ?? {}, null, 2)}
                      onChange={(event) => setAnswers((cur) => ({ ...cur, [q.id]: event.currentTarget.value }))}
                    />
                  </FormGroup>
                )
              }
              return (
                <FormGroup key={q.id} label={q.question}>
                  <InputGroup
                    value={String(value ?? '')}
                    onChange={(event) => setAnswers((cur) => ({ ...cur, [q.id]: event.currentTarget.value }))}
                  />
                </FormGroup>
              )
            })}
          </>
        ) : null}

        {compileError ? <Text className="upload-error">{compileError}</Text> : null}
        <div style={{ display: 'flex', gap: 8 }}>
          <Button intent="primary" text="Compile plan" onClick={handleCompile} loading={isCompiling} />
          <Button
            intent={plan?.requires_approval ? 'warning' : 'success'}
            text={plan?.requires_approval ? 'Approve & Execute' : 'Execute'}
            onClick={handleExecute}
            loading={isExecuting}
            disabled={!compileResult?.plan_id || isCompiling}
          />
        </div>
      </Card>

      <Card className="card">
        <div className="card-title">Plan</div>
        {compileResult ? (
          <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>{JSON.stringify(compileResult, null, 2)}</pre>
        ) : (
          <Text>No plan yet.</Text>
        )}
      </Card>

      <Card className="card">
        <div className="card-title">Execution</div>
        {executionError ? <Text className="upload-error">{executionError}</Text> : null}
        {executionResult ? (
          <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>{JSON.stringify(executionResult, null, 2)}</pre>
        ) : (
          <Text>Run a plan to see execution metadata (run_id, status, etc.).</Text>
        )}
      </Card>
    </div>
  )
}

