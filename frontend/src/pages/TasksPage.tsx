import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Button,
  Card,
  FormGroup,
  HTMLTable,
  InputGroup,
  Intent,
} from '@blueprintjs/core'
import { cancelTask, getTask, getTaskMetrics, getTaskResult, listTasks } from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { useAppStore } from '../store/useAppStore'

type TaskItem = Record<string, unknown>

export const TasksPage = () => {
  const queryClient = useQueryClient()
  const requestContext = useRequestContext()
  const language = useAppStore((state) => state.context.language)

  const searchParams = useMemo(() => {
    if (typeof window === 'undefined') {
      return new URLSearchParams()
    }
    return new URL(window.location.href).searchParams
  }, [])

  const initialTaskId = searchParams.get('task_id') ?? ''

  const [status, setStatus] = useState('')
  const [taskType, setTaskType] = useState('')
  const [selectedTaskId, setSelectedTaskId] = useState<string | null>(initialTaskId || null)

  const tasksQuery = useQuery({
    queryKey: qk.tasks(requestContext.language, { status, task_type: taskType }),
    queryFn: () => listTasks(requestContext, { status: status || undefined, task_type: taskType || undefined }),
  })

  const taskMutation = useMutation({
    mutationFn: () => (selectedTaskId ? getTask(requestContext, selectedTaskId) : Promise.resolve(null)),
    onError: (error) => toastApiError(error, language),
  })

  const resultMutation = useMutation({
    mutationFn: () => (selectedTaskId ? getTaskResult(requestContext, selectedTaskId) : Promise.resolve(null)),
    onError: (error) => toastApiError(error, language),
  })

  const cancelMutation = useMutation({
    mutationFn: (taskId: string) => cancelTask(requestContext, taskId),
    onSuccess: () => void queryClient.invalidateQueries({ queryKey: qk.tasks(requestContext.language, { status, task_type: taskType }) }),
    onError: (error) => toastApiError(error, language),
  })

  const metricsMutation = useMutation({
    mutationFn: () => getTaskMetrics(requestContext),
    onError: (error) => toastApiError(error, language),
  })

  useEffect(() => {
    if (!selectedTaskId) {
      return
    }
    taskMutation.mutate()
    resultMutation.mutate()
  }, [resultMutation, selectedTaskId, taskMutation])

  const tasks = (tasksQuery.data as { tasks?: TaskItem[] } | undefined)?.tasks ?? []

  return (
    <div>
      <PageHeader title="Tasks" subtitle="Background task monitoring." />

      <div className="page-grid two-col">
        <Card className="card-stack">
          <FormGroup label="Status filter">
            <InputGroup value={status} onChange={(event) => setStatus(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Task type filter">
            <InputGroup value={taskType} onChange={(event) => setTaskType(event.currentTarget.value)} />
          </FormGroup>
          <Button onClick={() => void queryClient.invalidateQueries({ queryKey: qk.tasks(requestContext.language, { status, task_type: taskType }) })}>
            Refresh
          </Button>
          <HTMLTable striped interactive className="command-table" style={{ marginTop: 12 }}>
            <thead>
              <tr>
                <th>Task ID</th>
                <th>Status</th>
                <th>Type</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {tasks.map((task) => {
                const id = String(task.task_id ?? '')
                return (
                <tr key={id} onClick={() => setSelectedTaskId(id)}>
                    <td>{id}</td>
                    <td>{String(task.status ?? '')}</td>
                    <td>{String(task.task_type ?? '')}</td>
                    <td>
                      <Button small onClick={() => { setSelectedTaskId(id); taskMutation.mutate(); }}>
                        View
                      </Button>
                      <Button small intent={Intent.DANGER} style={{ marginLeft: 8 }} onClick={() => cancelMutation.mutate(id)}>
                        Cancel
                      </Button>
                      <Button small style={{ marginLeft: 8 }} onClick={() => { setSelectedTaskId(id); resultMutation.mutate(); }}>
                        Result
                      </Button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </HTMLTable>
          <Button style={{ marginTop: 12 }} onClick={() => metricsMutation.mutate()}>
            Load metrics
          </Button>
          <JsonViewer value={metricsMutation.data} empty="Metrics will appear here." />
        </Card>

        <Card className="card-stack">
          <div className="card-title">Task detail</div>
          <JsonViewer value={taskMutation.data} empty="Select a task to view detail." />
          <div className="card-title">Task result</div>
          <JsonViewer value={resultMutation.data} empty="Select a task to view result." />
        </Card>
      </div>
    </div>
  )
}
