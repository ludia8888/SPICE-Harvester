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
import { asArray, asRecord, getString, type UnknownRecord } from '../utils/typed'

export const TasksPage = () => {
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)

  const [status, setStatus] = useState('')
  const [taskType, setTaskType] = useState('')
  const [limit, setLimit] = useState(100)
  const [selectedTask, setSelectedTask] = useState<unknown>(null)
  const [taskResult, setTaskResult] = useState<unknown>(null)
  const [metrics, setMetrics] = useState<unknown>(null)

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

  const listRecord = asRecord(listQuery.data)
  const tasks = asArray<UnknownRecord>(listRecord.tasks ?? asRecord(listRecord.data).tasks)

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
            {tasks.map((task) => {
              const taskId = getString(task.task_id) ?? ''
              return (
              <tr key={taskId}>
                <td>{taskId}</td>
                <td>{getString(task.task_name) ?? '-'}</td>
                <td>{getString(task.status) ?? '-'}</td>
                <td>{getString(task.task_type) ?? '-'}</td>
                <td>
                  <Button minimal onClick={() => detailMutation.mutate(taskId)}>
                    Details
                  </Button>
                  <Button minimal onClick={() => resultMutation.mutate(taskId)}>
                    Result
                  </Button>
                  <Button minimal intent={Intent.DANGER} onClick={() => cancelMutation.mutate(taskId)}>
                    Cancel
                  </Button>
                </td>
              </tr>
            )})}
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
