import { Menu, MenuItem, Popover, Icon } from '@blueprintjs/core'

export type NodeCategory = 'input' | 'transform' | 'output'

export type PaletteNode = {
  id: string
  label: string
  category: NodeCategory
  description?: string
  icon: string
  transformType?: string  // 백엔드 변환 타입
}

const INPUT_NODES: PaletteNode[] = [
  { id: 'dataset', label: '데이터셋', category: 'input', icon: 'database', description: '기존 데이터셋 불러오기' },
  { id: 'file', label: '파일', category: 'input', icon: 'document', description: '파일에서 데이터 불러오기' },
]

type NodePaletteProps = {
  onAddNode: (node: PaletteNode) => void
  disabled?: boolean
}

export const NodePalette = ({ onAddNode, disabled = false }: NodePaletteProps) => {
  const renderMenu = () => (
    <Menu className="node-palette-menu">
      {INPUT_NODES.map((node) => (
        <MenuItem
          key={node.id}
          icon={node.icon as never}
          text={node.label}
          label={node.description}
          onClick={() => onAddNode(node)}
          className="node-palette-item"
        />
      ))}
    </Menu>
  )

  return (
    <div className="pipeline-toolbox-group">
      <Popover
        content={renderMenu()}
        placement="bottom"
        minimal
        disabled={disabled}
      >
        <button type="button" className="pipeline-tool-button-outlined" disabled={disabled}>
          <Icon icon="import" size={14} />
          <span>Add datasets</span>
        </button>
      </Popover>
    </div>
  )
}
