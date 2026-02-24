import { useEffect, useRef } from 'react'
import { Icon } from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/core'
import { getCatalog } from './nodes/nodeStyles'

type Props = {
  position: { x: number; y: number }
  onSelect: (transformType: string) => void
  onClose: () => void
}

const TOOLBAR_ITEMS: Array<{ type: string; label: string }> = [
  { type: 'filter', label: 'Transform' },
  { type: 'join', label: 'Join' },
  { type: 'union', label: 'Union' },
]

export const NodeContextToolbar = ({ position, onSelect, onClose }: Props) => {
  const ref = useRef<HTMLDivElement>(null)

  /* Click-away listener — skip if clicking a ReactFlow node
     (onNodeClick will replace the toolbar position instead) */
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        const target = e.target as Element | null
        // If clicking on a ReactFlow node, let onNodeClick handle toolbar replacement
        // instead of closing here (avoids mid-click re-render that breaks ReactFlow events)
        if (target?.closest('.react-flow__node')) return
        onClose()
      }
    }
    const timer = setTimeout(() => document.addEventListener('mousedown', handler), 80)
    return () => {
      clearTimeout(timer)
      document.removeEventListener('mousedown', handler)
    }
  }, [onClose])

  /* Clamp position to viewport */
  const menuHeight = TOOLBAR_ITEMS.length * 44 + 12
  const x = Math.min(position.x, window.innerWidth - 180)
  const y = Math.min(position.y, window.innerHeight - menuHeight - 16)

  return (
    <div ref={ref} className="node-context-toolbar" style={{ left: x, top: y }}>
      {TOOLBAR_ITEMS.map((item) => {
        const cat = getCatalog(item.type)
        return (
          <button
            key={item.type}
            className="node-context-toolbar-item"
            onClick={(e) => {
              e.stopPropagation()
              onSelect(item.type)
            }}
          >
            <Icon icon={cat.icon as IconName} size={16} color={cat.color} />
            <span style={{ color: cat.color }}>{item.label}</span>
          </button>
        )
      })}
    </div>
  )
}
