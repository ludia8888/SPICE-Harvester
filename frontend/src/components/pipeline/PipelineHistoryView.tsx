import { useState } from 'react'
import { HTMLSelect, Icon, Menu, MenuItem, Popover } from '@blueprintjs/core'

type HistoryEntry = {
  version: number
  savedAt: string
  savedBy: string
  message: string
  branch: string
}

type Props = {
  pipelineId: string
  branch: string
}

/** Sample data — will be replaced with API calls */
const SAMPLE_HISTORY: HistoryEntry[] = [
  {
    version: 5,
    savedAt: '2026-02-24T14:32:00Z',
    savedBy: 'analyst@example.com',
    message: 'Added filter transform for active cases',
    branch: 'main',
  },
  {
    version: 4,
    savedAt: '2026-02-24T10:15:00Z',
    savedBy: 'engineer@example.com',
    message: 'Connected join node to evidence dataset',
    branch: 'main',
  },
  {
    version: 3,
    savedAt: '2026-02-23T16:30:00Z',
    savedBy: 'admin@example.com',
    message: 'Initial pipeline structure with 3 datasets',
    branch: 'main',
  },
  {
    version: 2,
    savedAt: '2026-02-22T11:00:00Z',
    savedBy: 'admin@example.com',
    message: 'Added aggregate step for summary',
    branch: 'dev',
  },
  {
    version: 1,
    savedAt: '2026-02-21T09:00:00Z',
    savedBy: 'admin@example.com',
    message: 'Created pipeline',
    branch: 'dev',
  },
]

const BRANCHES = ['main', 'dev']

function formatDate(iso: string): string {
  const d = new Date(iso)
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' }) +
    ' ' + d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
}

function timeAgo(iso: string): string {
  const now = Date.now()
  const then = new Date(iso).getTime()
  const diff = now - then
  const mins = Math.floor(diff / 60000)
  if (mins < 60) return `${mins}m ago`
  const hours = Math.floor(mins / 60)
  if (hours < 24) return `${hours}h ago`
  const days = Math.floor(hours / 24)
  return `${days}d ago`
}

export const PipelineHistoryView = ({ pipelineId: _pid, branch }: Props) => {
  const [selectedBranch, setSelectedBranch] = useState(branch)

  const entries = SAMPLE_HISTORY.filter((e) => e.branch === selectedBranch)

  return (
    <div className="pipeline-view-container">
      {/* Header */}
      <div className="pipeline-view-header">
        <h2 className="pipeline-view-title">History</h2>
        <p className="pipeline-view-subtitle">
          View recent activity in any branch of your pipeline workflow.
        </p>
      </div>

      {/* Branch selector */}
      <div className="pipeline-view-filter">
        <div className="pipeline-history-branch-select">
          <Icon icon="git-branch" size={13} />
          <HTMLSelect
            value={selectedBranch}
            onChange={(e) => setSelectedBranch(e.target.value)}
            minimal
          >
            {BRANCHES.map((b) => (
              <option key={b} value={b}>{b}</option>
            ))}
          </HTMLSelect>
        </div>
      </div>

      {/* History list */}
      {entries.length > 0 ? (
        <div className="pipeline-view-list">
          {entries.map((entry) => (
            <div key={entry.version} className="pipeline-view-card">
              <div className="pipeline-view-card-header">
                <div className="pipeline-view-card-title-row">
                  <span className="pipeline-history-version">v{entry.version}</span>
                  <span className="pipeline-history-time-ago">{timeAgo(entry.savedAt)}</span>
                </div>

                {/* Actions dropdown */}
                <Popover
                  content={
                    <Menu>
                      <MenuItem icon="git-branch" text="Create branch" />
                      <MenuItem icon="document" text="View details" />
                      <MenuItem icon="comparison" text="View changes" />
                    </Menu>
                  }
                  placement="bottom-end"
                >
                  <button className="pipeline-history-actions-btn">
                    <span>Actions</span>
                    <Icon icon="caret-down" size={10} />
                  </button>
                </Popover>
              </div>

              <p className="pipeline-view-card-desc">{entry.message}</p>

              <div className="pipeline-view-card-meta">
                <Icon icon="user" size={11} />
                <span>{entry.savedBy}</span>
                <span className="pipeline-view-card-sep">&middot;</span>
                <Icon icon="time" size={11} />
                <span>Saved {formatDate(entry.savedAt)}</span>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div className="pipeline-view-empty">
          <Icon icon="history" size={32} style={{ opacity: 0.3 }} />
          <p>No history entries for branch &ldquo;{selectedBranch}&rdquo;.</p>
        </div>
      )}
    </div>
  )
}
