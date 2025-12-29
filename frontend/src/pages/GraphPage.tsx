import { useState } from 'react'
import { Button, Icon } from '@blueprintjs/core'
import { useAppStore } from '../state/store'

export const GraphPage = () => {
  const pipelineContext = useAppStore((state) => state.pipelineContext)
  const [activeTab, setActiveTab] = useState<'edit' | 'proposals' | 'history'>('edit')
  const pipelineDisplayName = pipelineContext?.folderName || 'Pipeline Builder'
  const pipelineType = 'batch'
  const actionsDisabled = true

  const topbar = (
    <div className="pipeline-topbar">
      <div className="pipeline-topbar-left">
        <div className="pipeline-header-details">
          <div className="pipeline-title-row">
            <div className="pipeline-breadcrumb">
              <Icon icon="folder-close" size={14} className="pipeline-breadcrumb-icon" />
              <span className="pipeline-breadcrumb-text">Pipeline Builder</span>
            </div>
            <Icon icon="chevron-right" className="pipeline-breadcrumb-separator" size={14} />
            <div className="pipeline-name-wrapper">
              <span className="pipeline-name">{pipelineDisplayName}</span>
              <Button minimal small icon="star-empty" className="pipeline-star" disabled={actionsDisabled} />
            </div>
          </div>

          <div className="pipeline-menu-row">
            <div className="pipeline-menu">
              <Button minimal text="File" rightIcon="caret-down" small />
              <Button minimal text="Settings" rightIcon="caret-down" small />
              <Button minimal text="Help" rightIcon="caret-down" small />
            </div>
            <div className="pipeline-divider" />
            <div className="pipeline-info-item">
              <Icon icon="office" size={12} />
              <span>1</span>
            </div>
            <div className="pipeline-divider" />
            <div className="pipeline-batch-badge">
              <span>{pipelineType === 'batch' ? 'Batch' : 'Streaming'}</span>
            </div>
          </div>
        </div>
      </div>
      <div className="pipeline-topbar-center">
        <div className="pipeline-tabs">
          <Button
            minimal
            className={activeTab === 'edit' ? 'pipeline-tab-active' : ''}
            onClick={() => setActiveTab('edit')}
            text="Edit"
          />
          <Button
            minimal
            className={activeTab === 'proposals' ? 'pipeline-tab-active' : ''}
            onClick={() => setActiveTab('proposals')}
            text="Proposals"
          />
          <Button
            minimal
            className={activeTab === 'history' ? 'pipeline-tab-active' : ''}
            onClick={() => setActiveTab('history')}
            text="History"
          />
        </div>
      </div>
      <div className="pipeline-topbar-right">
        <div className="pipeline-history-controls">
          <Button minimal icon="undo" disabled={actionsDisabled} small />
          <Button minimal icon="redo" disabled={actionsDisabled} small />
        </div>

        <div className="pipeline-divider" />

        <div className="pipeline-branch-selector">
          <Icon icon="lock" size={12} className="pipeline-branch-icon" />
          <span className="pipeline-branch-name">Main</span>
          <Icon icon="caret-down" size={12} />
        </div>

        <Button
          className="pipeline-action-btn"
          intent="success"
          icon="floppy-disk"
          text="Save"
          small
          disabled={actionsDisabled}
        />
        <Button
          className="pipeline-action-btn"
          intent="primary"
          icon="git-pull"
          text="Propose"
          outlined
          small
          disabled={actionsDisabled}
        />
        <div className="pipeline-deploy-group">
          <Button
            className="pipeline-deploy-btn"
            intent="primary"
            text="Deploy"
            small
            disabled={actionsDisabled}
          />
          <Button
            className="pipeline-deploy-options"
            intent="primary"
            icon="settings"
            small
            disabled={actionsDisabled}
          />
        </div>

        <div className="pipeline-divider" />

        <Button minimal icon="share" text="Share" small disabled={actionsDisabled} />
        <Button minimal icon="menu" small />
      </div>
    </div>
  )

  return (
    <div className="page pipeline-page">
      {topbar}
    </div>
  )
}
