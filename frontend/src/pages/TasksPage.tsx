import { useMemo, useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import {
  Button,
  Card,
  FormGroup,
  H5,
  HTMLSelect,
  HTMLTable,
  InputGroup,
  Intent,
  NumericInput,
} from '@blueprintjs/core'
import { cancelTask, getTask, getTaskMetrics, getTaskResult, listTasks } from '../api/bff'
import { PageHeader } from '../components/PageHeader'
import { JsonView } from '../components/JsonView'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { useAppStore } from '../store/useAppStore'

export const TasksPage = () => {
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)

  const [status, setStatus] = useState('')
  const [taskType, setTaskType] = useState('')
  const [limit, setLimit] = useState(100)
  const [selectedTask, setSelectedTask] = useState<any>(null)
  const [taskResult, setTaskResult] = useState<any>(null)
  const [metrics, setMetrics] = useState<any>(null)

  const requestContext = useMemo(
    () => ({ language: context.language, authToken, adminToken }),
    [adminToken, authToken, context.language],
  )

  const listQuery = useQuery({
    queryKey: qk.tasks({ status: status || undefined, taskType: taskType || undefined, language: context.language }),
    queryFn: () => listTasks(requestContext, { status: status || undefined, task_type: taskType || undefined, limit }),
  })

  const detailMutation = useMutation({
    mutationFn: (taskId: string) => getTask(requestContext, taskId),
    onSuccess: (payload) => setSelectedTask(payload),
    onError: (error) => toastApiError(error, context.language),
  })

  const resultMutation = useMutation({
    mutationFn: (taskId: string) => getTaskResult(requestContext, taskId),
    onSuccess: (payload) => setTaskResult(payload),
    onError: (error) => toastApiError(error, context.language),
  })

  const cancelMutation = useMutation({
    mutationFn: (taskId: string) => cancelTask(requestContext, taskId),
    onError: (error) => toastApiError(error, context.language),
  })

  const metricsMutation = useMutation({
    mutationFn: () => getTaskMetrics(requestContext),
    onSuccess: (payload) => setMetrics(payload),
    onError: (error) => toastApiError(error, context.language),
  })

  const tasks = (listQuery.data as any)?.tasks ?? []

  return (
    <div>
      <PageHeader title="Tasks" subtitle="백그라운드 작업 모니터링" />

      <Card elevation={1} className="section-card">
        <div className="form-row">
          <FormGroup label="Status">
            <HTMLSelect
              value={status}
              onChange={(event) => setStatus(event.currentTarget.value)}
              options={['', 'PENDING', 'RUNNING', 'COMPLETED', 'FAILED']}
            />
          </FormGroup>
          <FormGroup label="Task type">
            <InputGroup value={taskType} onChange={(event) => setTaskType(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Limit">
            <NumericInput value={limit} min={1} max={1000} onValueChange={(value) => setLimit(value)} />
          </FormGroup>
          <Button icon="refresh" onClick={() => listQuery.refetch()}>
            Refresh
          </Button>
          <Button intent={Intent.PRIMARY} onClick={() => metricsMutation.mutate()}>
            Metrics
          </Button>
        </div>
      </Card>

      <Card elevation={1} className="section-card">
        <H5>Tasks</H5>
        <HTMLTable striped interactive className="full-width">
          <thead>
            <tr>
              <th>ID</th>
              <th>Name</th>
              <th>Status</th>
              <th>Type</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {tasks.map((task: any) => (
              <tr key={task.task_id}>
                <td>{task.task_id}</td>
                <td>{task.task_name}</td>
                <td>{task.status}</td>
                <td>{task.task_type}</td>
                <td>
                  <Button minimal onClick={() => detailMutation.mutate(task.task_id)}>
                    Details
                  </Button>
                  <Button minimal onClick={() => resultMutation.mutate(task.task_id)}>
                    Result
                  </Button>
                  <Button minimal intent={Intent.DANGER} onClick={() => cancelMutation.mutate(task.task_id)}>
                    Cancel
                  </Button>
                </td>
              </tr>
            ))}
          </tbody>
        </HTMLTable>
      </Card>

      <Card elevation={1} className="section-card">
        <H5>Details</H5>
        <JsonView value={selectedTask} />
      </Card>

      <Card elevation={1} className="section-card">
        <H5>Result</H5>
        <JsonView value={taskResult} />
      </Card>

      <Card elevation={1} className="section-card">
        <H5>Metrics</H5>
        <JsonView value={metrics} />
      </Card>
    </div>
  )
}
