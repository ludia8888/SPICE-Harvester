import { useEffect, useMemo, useState } from 'react'
import { Button, Card, FormGroup, H3, HTMLSelect, InputGroup, Spinner, Switch, Text, TextArea } from '@blueprintjs/core'
import { useQuery } from '@tanstack/react-query'
import { aiQuery, listDatabases, runGraphQuery, type DatabaseRecord } from '../api/bff'

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

export const WorkshopPage = () => {
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

  const [question, setQuestion] = useState('')
  const [aiIncludeDocuments, setAiIncludeDocuments] = useState(true)
  const [aiIncludeProvenance, setAiIncludeProvenance] = useState(true)
  const [aiLimit, setAiLimit] = useState('50')
  const [aiResult, setAiResult] = useState<Record<string, unknown> | null>(null)
  const [aiError, setAiError] = useState<string | null>(null)
  const [isAiRunning, setAiRunning] = useState(false)

  const [graphRequest, setGraphRequest] = useState(
    JSON.stringify(
      {
        start_class: 'Customer',
        hops: [],
        filters: null,
        limit: 10,
        offset: 0,
        max_nodes: 200,
        max_edges: 500,
        include_paths: false,
        path_depth_limit: null,
        max_paths: 25,
        no_cycles: true,
        include_documents: true,
        include_provenance: false,
      },
      null,
      2,
    ),
  )
  const [graphResult, setGraphResult] = useState<Record<string, unknown> | null>(null)
  const [graphError, setGraphError] = useState<string | null>(null)
  const [isGraphRunning, setGraphRunning] = useState(false)

  useEffect(() => {
    if (!activeDbName && databases.length > 0) {
      setActiveDbName(databases[0].id)
    }
  }, [activeDbName, databases])

  const handleRunAi = () => {
    if (!activeDbName) {
      setAiError('Select a project first.')
      return
    }
    const trimmed = question.trim()
    if (!trimmed) {
      setAiError('Enter a question.')
      return
    }
    const run = async () => {
      setAiRunning(true)
      setAiError(null)
      setAiResult(null)
      try {
        const limit = Number.parseInt(aiLimit, 10)
        const result = await aiQuery(activeDbName, {
          question: trimmed,
          branch: branch.trim() || 'main',
          mode: 'auto',
          limit: Number.isFinite(limit) ? limit : 50,
          include_documents: aiIncludeDocuments,
          include_provenance: aiIncludeProvenance,
        })
        setAiResult(result)
      } catch (error) {
        setAiError(error instanceof Error ? error.message : 'AI query failed')
      } finally {
        setAiRunning(false)
      }
    }
    void run()
  }

  const handleRunGraph = () => {
    if (!activeDbName) {
      setGraphError('Select a project first.')
      return
    }
    const run = async () => {
      setGraphRunning(true)
      setGraphError(null)
      setGraphResult(null)
      try {
        const parsed = JSON.parse(graphRequest)
        const result = await runGraphQuery(activeDbName, parsed, { branch })
        setGraphResult(result)
      } catch (error) {
        setGraphError(error instanceof Error ? error.message : 'Graph query failed')
      } finally {
        setGraphRunning(false)
      }
    }
    void run()
  }

  return (
    <div className="page">
      <H3>Workshop</H3>

      <Card className="card">
        <div className="card-title">Project scope</div>
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
      </Card>

      <div className="grid" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(340px, 1fr))' }}>
        <Card className="card">
          <div className="card-title">Natural language query (read-only)</div>
          <FormGroup label="Question">
            <TextArea
              fill
              rows={3}
              placeholder='e.g. "Name이 Alice인 고객 찾아줘"'
              value={question}
              onChange={(event) => setQuestion(event.currentTarget.value)}
            />
          </FormGroup>
          <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'center' }}>
            <Switch checked={aiIncludeDocuments} label="Include documents" onChange={() => setAiIncludeDocuments((v) => !v)} />
            <Switch checked={aiIncludeProvenance} label="Include provenance" onChange={() => setAiIncludeProvenance((v) => !v)} />
            <FormGroup label="Limit" style={{ margin: 0 }}>
              <InputGroup value={aiLimit} onChange={(event) => setAiLimit(event.currentTarget.value)} style={{ width: 120 }} />
            </FormGroup>
          </div>
          {aiError ? <Text className="upload-error">{aiError}</Text> : null}
          <Button intent="primary" text="Run" onClick={handleRunAi} loading={isAiRunning} />
          {aiResult ? (
            <pre style={{ margin: '12px 0 0', whiteSpace: 'pre-wrap' }}>{JSON.stringify(aiResult, null, 2)}</pre>
          ) : null}
        </Card>

        <Card className="card">
          <div className="card-title">Graph query (JSON)</div>
          <Text className="card-meta">Runs `POST /api/v1/graph-query/&lt;db&gt;` directly for multi-hop + federation.</Text>
          <FormGroup label="Request JSON">
            <TextArea
              fill
              rows={10}
              value={graphRequest}
              onChange={(event) => setGraphRequest(event.currentTarget.value)}
            />
          </FormGroup>
          {graphError ? <Text className="upload-error">{graphError}</Text> : null}
          <Button intent="primary" text="Run graph query" onClick={handleRunGraph} loading={isGraphRunning} />
          {graphResult ? (
            <pre style={{ margin: '12px 0 0', whiteSpace: 'pre-wrap' }}>{JSON.stringify(graphResult, null, 2)}</pre>
          ) : null}
        </Card>
      </div>
    </div>
  )
}
