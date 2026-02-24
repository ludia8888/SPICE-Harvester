import { Icon, Menu, MenuItem, Popover } from '@blueprintjs/core'
import type { UseMutationResult } from '@tanstack/react-query'

export type PipelineTab = 'edit' | 'proposals' | 'history'

type Props = {
  pipelineName: string
  pipelineType?: string
  pipelineStatus?: string
  branch: string
  saveMut: UseMutationResult<unknown, Error, void, unknown>
  buildMut: UseMutationResult<unknown, Error, void, unknown>
  deployMut: UseMutationResult<
    unknown,
    Error,
    { interval_seconds?: number; cron?: string } | undefined,
    unknown
  >
  onUndo?: () => void
  onRedo?: () => void
  canUndo?: boolean
  canRedo?: boolean
  onBack: () => void
  onDeployClick?: () => void
  activeTab?: PipelineTab
  onTabChange?: (tab: PipelineTab) => void
}

export const PipelineToolbar = ({
  pipelineName,
  pipelineType,
  pipelineStatus,
  branch,
  saveMut,
  buildMut,
  deployMut,
  onUndo,
  onRedo,
  canUndo = false,
  canRedo = false,
  onBack,
  onDeployClick,
  activeTab = 'edit',
  onTabChange,
}: Props) => {
  return (
    <div className="pipeline-topbar">
      {/* Left: breadcrumb */}
      <div className="pipeline-topbar-left">
        <button className="pipeline-topbar-back" onClick={onBack} title="Back">
          <Icon icon="arrow-left" size={14} />
        </button>
        <span
          className="pipeline-topbar-crumb pipeline-topbar-crumb-link"
          onClick={onBack}
        >
          Pipelines
        </span>
        <span className="pipeline-topbar-separator">/</span>
        <span
          className="pipeline-topbar-name pipeline-topbar-name-link"
          onClick={onBack}
          title="Switch pipeline"
        >
          {pipelineName}
          <Icon icon="caret-down" size={12} style={{ opacity: 0.5 }} />
        </span>
        {pipelineType && (
          <span className="pipeline-topbar-badge">{pipelineType}</span>
        )}
        {pipelineStatus && (
          <span className={`pipeline-topbar-badge is-${pipelineStatus}`}>
            {pipelineStatus}
          </span>
        )}
      </div>

      {/* Center: tabs */}
      <div className="pipeline-topbar-center">
        <div className="pipeline-topbar-tabs">
          <button
            className={`pipeline-tab${activeTab === 'edit' ? ' is-active' : ''}`}
            onClick={() => onTabChange?.('edit')}
          >
            Edit
          </button>
          <button
            className={`pipeline-tab${activeTab === 'proposals' ? ' is-active' : ''}`}
            onClick={() => onTabChange?.('proposals')}
          >
            Proposals
          </button>
          <button
            className={`pipeline-tab${activeTab === 'history' ? ' is-active' : ''}`}
            onClick={() => onTabChange?.('history')}
          >
            History
          </button>
        </div>
      </div>

      {/* Right: all action items */}
      <div className="pipeline-topbar-right">
        {/* Undo / Redo */}
        <div className="ptb-btn-group">
          <button
            className="ptb-icon-btn"
            disabled={!canUndo}
            onClick={onUndo}
            title="Undo (Ctrl+Z)"
          >
            <Icon icon="undo" size={14} />
          </button>
          <button
            className="ptb-icon-btn"
            disabled={!canRedo}
            onClick={onRedo}
            title="Redo (Ctrl+Shift+Z)"
          >
            <Icon icon="redo" size={14} />
          </button>
        </div>

        {/* Branch */}
        <button className="ptb-branch-btn" title="Switch branch">
          <Icon icon="git-branch" size={13} />
          <span>{branch}</span>
          <Icon icon="caret-down" size={10} className="ptb-branch-caret" />
        </button>

        {/* Save */}
        <button
          className="ptb-save-btn"
          onClick={() => saveMut.mutate()}
          disabled={saveMut.isPending}
          title="Save changes"
        >
          {saveMut.isPending ? (
            <Icon icon="refresh" size={13} className="ptb-spin" />
          ) : (
            <Icon icon="upload" size={13} />
          )}
          <span>Save</span>
        </button>

        {/* Propose */}
        <button className="ptb-outlined-btn" title="Propose merge">
          <Icon icon="git-merge" size={13} />
          <span>Propose</span>
        </button>

        {/* Deploy dropdown */}
        <Popover
          content={
            <Menu>
              <MenuItem
                icon="build"
                text="Build"
                onClick={() => buildMut.mutate()}
                disabled={buildMut.isPending}
              />
              <MenuItem
                icon="rocket-slant"
                text="Deploy..."
                onClick={() => onDeployClick?.()}
                disabled={deployMut.isPending}
              />
            </Menu>
          }
          placement="bottom-end"
        >
          <button
            className="ptb-deploy-btn"
            disabled={deployMut.isPending || buildMut.isPending}
          >
            <span>Deploy</span>
            <Icon icon="caret-down" size={10} />
          </button>
        </Popover>

        {/* Build settings */}
        <Popover
          content={
            <Menu>
              <MenuItem icon="cog" text="Default" />
              <MenuItem icon="dashboard" text="Medium" />
              <MenuItem icon="rocket-slant" text="Large" />
            </Menu>
          }
          placement="bottom-end"
        >
          <button className="ptb-icon-btn" title="Build settings">
            <Icon icon="settings" size={14} />
          </button>
        </Popover>

        <div className="ptb-divider" />

        {/* Builds & checks status */}
        <div className="ptb-checks-group">
          <span className="ptb-check-item is-sync">
            <Icon icon="refresh" size={11} />
            <span>0</span>
          </span>
          <span className="ptb-check-item is-pass">
            <Icon icon="tick" size={11} />
            <span>1</span>
          </span>
          <span className="ptb-check-item is-fail">
            <Icon icon="cross" size={11} />
            <span>1</span>
          </span>
        </div>
      </div>
    </div>
  )
}
