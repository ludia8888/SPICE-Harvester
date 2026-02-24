import { useState } from 'react'
import { Icon, Tag } from '@blueprintjs/core'

type ProposalStatus = 'open' | 'merged' | 'closed'
type StatusFilter = 'all' | ProposalStatus

type Proposal = {
  id: string
  title: string
  status: ProposalStatus
  createdAt: string
  createdBy: string
  description?: string
}

type Props = {
  pipelineId: string
  branch: string
}

/** Sample data — will be replaced with API calls */
const SAMPLE_PROPOSALS: Proposal[] = [
  {
    id: 'prop-3',
    title: 'Add aggregation step for quarterly report',
    status: 'open',
    createdAt: '2026-02-24T14:32:00Z',
    createdBy: 'analyst@example.com',
    description: 'Adds a new aggregate transform grouping by quarter',
  },
  {
    id: 'prop-2',
    title: 'Update join keys for customer lookup',
    status: 'merged',
    createdAt: '2026-02-20T09:15:00Z',
    createdBy: 'engineer@example.com',
    description: 'Changed join from single key to composite key',
  },
  {
    id: 'prop-1',
    title: 'Initial pipeline structure',
    status: 'closed',
    createdAt: '2026-02-18T16:45:00Z',
    createdBy: 'admin@example.com',
    description: 'Closed in favor of a different approach',
  },
]

const STATUS_CONFIG: Record<ProposalStatus, { icon: string; color: string; label: string }> = {
  open: { icon: 'dot', color: '#48AFF0', label: 'Open' },
  merged: { icon: 'git-merge', color: '#4ade80', label: 'Merged' },
  closed: { icon: 'cross', color: '#f87171', label: 'Closed' },
}

const FILTERS: { value: StatusFilter; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'open', label: 'Open' },
  { value: 'merged', label: 'Merged' },
  { value: 'closed', label: 'Closed' },
]

function formatDate(iso: string): string {
  const d = new Date(iso)
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' }) +
    ' ' + d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' })
}

export const PipelineProposalsView = ({ pipelineId: _pid, branch: _branch }: Props) => {
  const [filter, setFilter] = useState<StatusFilter>('all')

  const proposals = SAMPLE_PROPOSALS.filter(
    (p) => filter === 'all' || p.status === filter,
  )

  return (
    <div className="pipeline-view-container">
      {/* Header */}
      <div className="pipeline-view-header">
        <h2 className="pipeline-view-title">Proposals</h2>
        <p className="pipeline-view-subtitle">
          View open, merged, or closed proposals for this pipeline.
        </p>
      </div>

      {/* Filter bar */}
      <div className="pipeline-view-filter">
        {FILTERS.map((f) => (
          <button
            key={f.value}
            className={`pipeline-view-filter-btn${filter === f.value ? ' is-active' : ''}`}
            onClick={() => setFilter(f.value)}
          >
            {f.label}
            {f.value !== 'all' && (
              <span className="pipeline-view-filter-count">
                {SAMPLE_PROPOSALS.filter((p) => p.status === f.value).length}
              </span>
            )}
          </button>
        ))}
      </div>

      {/* Proposal list */}
      {proposals.length > 0 ? (
        <div className="pipeline-view-list">
          {proposals.map((p) => {
            const cfg = STATUS_CONFIG[p.status]
            return (
              <div key={p.id} className="pipeline-view-card">
                <div className="pipeline-view-card-header">
                  <div className="pipeline-view-card-title-row">
                    <Icon
                      icon={cfg.icon as 'dot'}
                      size={14}
                      style={{ color: cfg.color }}
                    />
                    <span className="pipeline-view-card-title">{p.title}</span>
                  </div>
                  <Tag
                    minimal
                    style={{ color: cfg.color, borderColor: cfg.color, background: 'transparent' }}
                  >
                    {cfg.label}
                  </Tag>
                </div>
                {p.description && (
                  <p className="pipeline-view-card-desc">{p.description}</p>
                )}
                <div className="pipeline-view-card-meta">
                  <Icon icon="time" size={11} />
                  <span>Created {formatDate(p.createdAt)}</span>
                  <span className="pipeline-view-card-sep">by</span>
                  <span>{p.createdBy}</span>
                </div>
              </div>
            )
          })}
        </div>
      ) : (
        <div className="pipeline-view-empty">
          <Icon icon="git-pull" size={32} style={{ opacity: 0.3 }} />
          <p>No {filter === 'all' ? '' : filter + ' '}proposals yet.</p>
        </div>
      )}
    </div>
  )
}
