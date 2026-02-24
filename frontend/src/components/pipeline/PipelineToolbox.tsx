import { Icon, Menu, MenuItem, Popover } from '@blueprintjs/core'
import { ALL_TRANSFORM_TYPES } from './nodes/nodeStyles'

type InteractionMode = 'pan' | 'select' | 'remove' | 'edit'

type Props = {
  interactionMode: InteractionMode
  onSetMode: (mode: InteractionMode) => void
  onAddTransform: (type: string) => void
  onAddDatasets: () => void
  onAutoLayout: () => void
  onSelectAll?: () => void
}

/** Single-input transforms shown inside the Transform popover */
const SINGLE_TRANSFORMS = ALL_TRANSFORM_TYPES.filter(
  (t) => !['join', 'union'].includes(t.type),
)

export const PipelineToolbox = ({
  interactionMode,
  onSetMode,
  onAddTransform,
  onAddDatasets,
  onAutoLayout,
  onSelectAll,
}: Props) => {
  return (
    <div className="pipeline-toolbox">
      {/* ── Tools (pan + drag-select grouped) ── */}
      <div className="pipeline-tb-group">
        <div className="pipeline-tb-row">
          <button
            className={`pipeline-tb-icon${interactionMode === 'pan' ? ' is-active' : ''}`}
            onClick={() => onSetMode('pan')}
            title="Panning Mode"
          >
            <Icon icon="move" size={16} />
          </button>
          <button
            className={`pipeline-tb-icon${interactionMode === 'select' ? ' is-active' : ''}`}
            onClick={() => onSetMode('select')}
            title="Drag Select Mode"
          >
            <Icon icon="select" size={16} />
          </button>
        </div>
        <span className="pipeline-tb-label">Tools</span>
      </div>

      {/* ── Select All ── */}
      <div className="pipeline-tb-group">
        <button
          className="pipeline-tb-icon"
          onClick={onSelectAll}
          title="Select all nodes"
        >
          <Icon icon="circle" size={16} />
        </button>
        <span className="pipeline-tb-label">Select</span>
      </div>

      {/* ── Remove ── */}
      <div className="pipeline-tb-group">
        <button
          className={`pipeline-tb-icon${interactionMode === 'remove' ? ' is-active' : ''}`}
          onClick={() => onSetMode('remove')}
          title="Remove selected nodes"
        >
          <Icon icon="disable" size={16} />
        </button>
        <span className="pipeline-tb-label">Remove</span>
      </div>

      {/* ── Layout ── */}
      <div className="pipeline-tb-group">
        <button className="pipeline-tb-icon" onClick={onAutoLayout} title="Auto Layout">
          <Icon icon="layout-auto" size={16} />
        </button>
        <span className="pipeline-tb-label">Layout</span>
      </div>

      <div className="pipeline-toolbox-divider" />

      {/* ── Add datasets ── */}
      <button className="pipeline-tool-button-outlined" onClick={onAddDatasets}>
        <Icon icon="import" size={14} />
        <span>Add datasets</span>
      </button>

      {/* ── Parameters (with dropdown) ── */}
      <Popover
        content={
          <Menu>
            <MenuItem
              icon="variable"
              text="Add parameter"
              onClick={() => onAddTransform('output')}
            />
          </Menu>
        }
        placement="bottom-start"
      >
        <button className="pipeline-tool-button-outlined">
          <Icon icon="variable" size={14} />
          <span>Parameters</span>
          <Icon icon="caret-down" size={10} className="pipeline-tb-caret" />
        </button>
      </Popover>

      <div className="pipeline-toolbox-divider" />

      {/* ── Transform (3 icons grouped) ── */}
      <div className="pipeline-tb-group">
        <div className="pipeline-tb-row">
          <Popover
            content={
              <Menu>
                {SINGLE_TRANSFORMS.map((t) => (
                  <MenuItem
                    key={t.type}
                    icon={<Icon icon={t.icon} />}
                    text={t.label}
                    onClick={() => onAddTransform(t.type)}
                  />
                ))}
              </Menu>
            }
            placement="bottom-start"
          >
            <button className="pipeline-tb-icon pipeline-tb-color-purple" title="Transform">
              <Icon icon="function" size={16} />
            </button>
          </Popover>

          <button
            className="pipeline-tb-icon pipeline-tb-color-teal"
            onClick={() => onAddTransform('join')}
            title="Join"
          >
            <Icon icon="join-table" size={16} />
          </button>

          <button
            className="pipeline-tb-icon pipeline-tb-color-rose"
            onClick={() => onAddTransform('union')}
            title="Union"
          >
            <Icon icon="merge-links" size={16} />
          </button>
        </div>
        <span className="pipeline-tb-label">Transform</span>
      </div>

      {/* ── Search (find node) ── */}
      <div className="pipeline-tb-group">
        <button className="pipeline-tb-icon" title="Search nodes">
          <Icon icon="search" size={16} />
        </button>
      </div>

      {/* ── Edit (rename / edit node) ── */}
      <div className="pipeline-tb-group">
        <button
          className={`pipeline-tb-icon${interactionMode === 'edit' ? ' is-active' : ''}`}
          onClick={() => onSetMode('edit')}
          title="Edit / Rename node"
        >
          <Icon icon="edit" size={16} />
        </button>
        <span className="pipeline-tb-label">Edit</span>
      </div>
    </div>
  )
}
