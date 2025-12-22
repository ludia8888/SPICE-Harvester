import { Icon } from '@blueprintjs/core'
import type { PipelineTool } from './pipelineTypes'

type ToolbarCopy = {
    tools: string
    select: string
    remove: string
    layout: string
    addDatasets: string
    parameters: string
    transform: string
    edit: string
    zoomIn: string
    zoomOut: string
}

type ToolbarProps = {
    activeTool: PipelineTool
    copy: ToolbarCopy
    onToolChange: (tool: PipelineTool) => void
    onLayout: () => void
    onRemove: () => void
    onAddDatasets: () => void
    onParameters: () => void
    onTransform: () => void
    onEdit: () => void
    onZoomIn: () => void
    onZoomOut: () => void
}

export const PipelineToolbar = ({
    activeTool,
    copy,
    onToolChange,
    onLayout,
    onRemove,
    onAddDatasets,
    onParameters,
    onTransform,
    onEdit,
    onZoomIn,
    onZoomOut,
}: ToolbarProps) => {
    return (
        <div className="pipeline-toolbar">
            <div className="toolbar-group">
                <button className={`toolbar-btn ${activeTool === 'tools' ? 'active' : ''}`} type="button" onClick={() => onToolChange('tools')}>
                    <Icon icon="locate" size={14} /> {copy.tools}
                </button>
                <button className={`toolbar-btn ${activeTool === 'select' ? 'active' : ''}`} type="button" onClick={() => onToolChange('select')}>
                    <Icon icon="select" size={14} /> {copy.select}
                </button>
                <button className={`toolbar-btn ${activeTool === 'remove' ? 'active' : ''}`} type="button" onClick={onRemove}>
                    <Icon icon="trash" size={14} /> {copy.remove}
                </button>
                <button className="toolbar-btn" type="button" onClick={onLayout}>
                    <Icon icon="layout-auto" size={14} /> {copy.layout}
                </button>
            </div>

            <div className="toolbar-group">
                <button className="toolbar-btn primary" type="button" onClick={onAddDatasets}>
                    <Icon icon="add" size={14} /> {copy.addDatasets}
                </button>
                <button className="toolbar-btn" type="button" onClick={onParameters}>
                    <Icon icon="variable" size={14} /> {copy.parameters}
                </button>
            </div>

            <div className="toolbar-group">
                <button className="toolbar-btn accent" type="button" onClick={onTransform}>
                    <Icon icon="function" size={14} /> {copy.transform}
                </button>
                <button className="toolbar-btn" type="button" onClick={onEdit}>
                    <Icon icon="edit" size={14} /> {copy.edit}
                </button>
            </div>
            <div className="toolbar-group">
                <button className="toolbar-btn" type="button" onClick={onZoomIn}>
                    <Icon icon="zoom-in" size={14} /> {copy.zoomIn}
                </button>
                <button className="toolbar-btn" type="button" onClick={onZoomOut}>
                    <Icon icon="zoom-out" size={14} /> {copy.zoomOut}
                </button>
            </div>
            <div className="toolbar-group toolbar-spacer" />
        </div>
    )
}
