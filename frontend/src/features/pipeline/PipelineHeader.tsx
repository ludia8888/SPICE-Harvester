import { Button, Icon, Menu, MenuDivider, MenuItem, Popover, Tag } from '@blueprintjs/core'
import type { PipelineMode } from './pipelineTypes'

type HeaderCopy = {
    file: string
    help: string
    batch: string
    undo: string
    redo: string
    deploySettings: string
    tabs: {
        edit: string
        proposals: string
        history: string
    }
    commands: string
    save: string
    deploy: string
    projectFallback: string
    pipelineFallback: string
    newPipeline: string
    openPipeline: string
    saveMenu: string
    noPipelines: string
    noBranches: string
    createBranch: string
    archiveBranch: string
    restoreBranch: string
    archivedLabel: string
}

type HeaderProps = {
    dbName: string
    pipelineName: string
    pipelines: Array<{ id: string; name: string }>
    mode: PipelineMode
    branch: string
    branches: string[]
    branchArchived: boolean
    canUndo: boolean
    canRedo: boolean
    isDirty: boolean
    activeCommandCount: number
    copy: HeaderCopy
    onModeChange: (mode: PipelineMode) => void
    onUndo: () => void
    onRedo: () => void
    onBranchSelect: (branch: string) => void
    onPipelineSelect: (pipelineId: string) => void
    onSave: () => void
    onDeploy: () => void
    onOpenDeploySettings: () => void
    onOpenHelp: () => void
    onCreatePipeline: () => void
    onOpenCommandDrawer: () => void
    onArchiveBranch: () => void
    onRestoreBranch: () => void
}

export const PipelineHeader = ({
    dbName,
    pipelineName,
    pipelines,
    mode,
    branch,
    branches,
    branchArchived,
    activeCommandCount,
    copy,
    canUndo,
    canRedo,
    isDirty,
    onModeChange,
    onUndo,
    onRedo,
    onBranchSelect,
    onPipelineSelect,
    onSave,
    onDeploy,
    onOpenDeploySettings,
    onOpenHelp,
    onCreatePipeline,
    onOpenCommandDrawer,
    onArchiveBranch,
    onRestoreBranch,
}: HeaderProps) => {
    const projectLabel = dbName || copy.projectFallback
    const pipelineLabel = pipelineName || copy.pipelineFallback

    const fileMenu = (
        <Menu>
            <MenuItem icon="document" text={copy.newPipeline} onClick={onCreatePipeline} />
            <MenuItem icon="folder-open" text={copy.openPipeline}>
                {pipelines.length === 0 ? (
                    <MenuItem disabled text={copy.noPipelines} />
                ) : (
                    pipelines.map((pipeline) => (
                        <MenuItem key={pipeline.id} text={pipeline.name} onClick={() => onPipelineSelect(pipeline.id)} />
                    ))
                )}
            </MenuItem>
            <MenuItem icon="floppy-disk" text={copy.saveMenu} onClick={onSave} />
        </Menu>
    )

    const branchMenu = (
        <Menu>
            {branches.length === 0 ? (
                <MenuItem disabled text={copy.noBranches} />
            ) : (
                branches.map((name) => (
                    <MenuItem
                        key={name}
                        text={name === '__create__' ? copy.createBranch : name}
                        icon={name === '__create__' ? 'add' : undefined}
                        onClick={() => onBranchSelect(name)}
                    />
                ))
            )}
            {branch !== 'main' ? (
                <>
                    <MenuDivider />
                    <MenuItem
                        icon={branchArchived ? 'refresh' : 'archive'}
                        text={branchArchived ? copy.restoreBranch : copy.archiveBranch}
                        onClick={branchArchived ? onRestoreBranch : onArchiveBranch}
                    />
                </>
            ) : null}
        </Menu>
    )

    return (
        <div className="pipeline-header">
            <div className="pipeline-header-left">
                <div className="pipeline-header-menu">
                    <Popover content={fileMenu} position="bottom-left">
                        <button className="pipeline-header-link" type="button">{copy.file}</button>
                    </Popover>
                    <button className="pipeline-header-link" type="button" onClick={onOpenHelp}>{copy.help}</button>
                </div>
                <div className="pipeline-header-divider" />
                <div className="pipeline-header-breadcrumbs">
                    <span className="breadcrumb-ellipsis">…</span>
                    <span className="breadcrumb-separator">/</span>
                    <Tag minimal icon="folder-close">{projectLabel}</Tag>
                    <span className="breadcrumb-separator">/</span>
                    <span className="pipeline-header-current">{pipelineLabel}</span>
                </div>
                <span className="pipeline-header-badge">{copy.batch}</span>
            </div>
            <div className="pipeline-header-right">
                <div className="pipeline-header-tabs">
                    <button
                        className={`pipeline-header-link ${mode === 'edit' ? 'is-active' : ''}`}
                        type="button"
                        onClick={() => onModeChange('edit')}
                    >
                        {copy.tabs.edit}
                    </button>
                    <button
                        className={`pipeline-header-link ${mode === 'proposals' ? 'is-active' : ''}`}
                        type="button"
                        onClick={() => onModeChange('proposals')}
                    >
                        {copy.tabs.proposals}
                    </button>
                    <button
                        className={`pipeline-header-link ${mode === 'history' ? 'is-active' : ''}`}
                        type="button"
                        onClick={() => onModeChange('history')}
                    >
                        {copy.tabs.history}
                    </button>
                </div>
                <div className="pipeline-header-divider" />
                <div className="pipeline-header-actions">
                    <button className="pipeline-command-btn" type="button" onClick={onOpenCommandDrawer}>
                        <Icon icon="history" size={12} />
                        {copy.commands}
                        {activeCommandCount > 0 ? (
                            <Tag minimal round style={{ marginLeft: 6 }}>
                                {activeCommandCount}
                            </Tag>
                        ) : null}
                    </button>
                    <Button className="pipeline-icon-btn" type="button" aria-label={copy.undo} onClick={onUndo} disabled={!canUndo} minimal icon="undo" />
                    <Button className="pipeline-icon-btn" type="button" aria-label={copy.redo} onClick={onRedo} disabled={!canRedo} minimal icon="redo" />
                    <Popover content={branchMenu} position="bottom-right">
                        <button className="pipeline-pill" type="button">
                            <Icon icon="git-branch" size={12} />
                            <span>{branch}</span>
                            {branchArchived ? (
                                <Tag minimal intent="warning" style={{ marginLeft: 6 }}>
                                    {copy.archivedLabel}
                                </Tag>
                            ) : null}
                            <Icon icon="caret-down" size={12} />
                        </button>
                    </Popover>
                    <button className="pipeline-action-btn" type="button" onClick={onSave} disabled={branchArchived}>
                        <span className={`pipeline-save-status ${isDirty ? 'is-dirty' : 'is-saved'}`} />
                        {copy.save}
                    </button>
                </div>
                <div className="pipeline-deliver">
                    <div className="pipeline-deliver-actions">
                        <button className="pipeline-action-btn primary" type="button" onClick={onDeploy} disabled={branchArchived}>
                            {copy.deploy}
                            <Icon icon="caret-down" size={12} />
                        </button>
                        <button
                            className="pipeline-icon-btn"
                            type="button"
                            aria-label={copy.deploySettings}
                            onClick={onOpenDeploySettings}
                            disabled={branchArchived}
                        >
                            <Icon icon="cog" size={12} />
                        </button>
                    </div>
                </div>
            </div>
        </div>
    )
}
