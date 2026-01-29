import { Icon, Spinner, Tag } from '@blueprintjs/core'
import type { ActionLog } from '../../api/bff'

type ActionHistoryProps = {
  logs: ActionLog[]
  isLoading?: boolean
}

const formatTime = (isoString: string): string => {
  const date = new Date(isoString)
  return date.toLocaleString('ko-KR', {
    month: 'numeric',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

const getStatusIntent = (status: ActionLog['status']) => {
  switch (status) {
    case 'success':
      return 'success'
    case 'failed':
      return 'danger'
    case 'pending':
      return 'warning'
    default:
      return 'none'
  }
}

const getStatusLabel = (status: ActionLog['status']) => {
  switch (status) {
    case 'success':
      return '완료'
    case 'failed':
      return '실패'
    case 'pending':
      return '대기중'
    default:
      return status
  }
}

export const ActionHistory = ({ logs, isLoading = false }: ActionHistoryProps) => {
  if (isLoading) {
    return (
      <div className="action-history is-loading">
        <Spinner size={20} />
        <span>실행 기록을 불러오는 중...</span>
      </div>
    )
  }

  if (logs.length === 0) {
    return (
      <div className="action-history is-empty">
        <Icon icon="history" size={20} />
        <span>실행 기록이 없습니다</span>
      </div>
    )
  }

  return (
    <div className="action-history">
      <div className="action-history-header">
        <Icon icon="history" size={14} />
        <span className="action-history-title">실행 기록</span>
      </div>
      <div className="action-history-list">
        <table className="action-history-table">
          <thead>
            <tr>
              <th>시간</th>
              <th>Action</th>
              <th>상태</th>
              <th>실행자</th>
            </tr>
          </thead>
          <tbody>
            {logs.map((log) => (
              <tr key={log.id} className="action-history-row">
                <td className="action-history-time">{formatTime(log.executedAt)}</td>
                <td className="action-history-action">{log.actionTypeName}</td>
                <td className="action-history-status">
                  <Tag minimal intent={getStatusIntent(log.status) as never}>
                    {getStatusLabel(log.status)}
                  </Tag>
                </td>
                <td className="action-history-user">{log.executedBy}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
