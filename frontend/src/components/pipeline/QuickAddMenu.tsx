import { useEffect, useRef } from 'react'
import { Icon, Menu, MenuDivider, MenuItem } from '@blueprintjs/core'
import { getCatalog } from './nodes/nodeStyles'
import type { IconName } from '@blueprintjs/core'

type Props = {
  position: { x: number; y: number }
  onSelect: (transformType: string) => void
  onClose: () => void
}

/* ── Categorized transform types ─── */
const MENU_SECTIONS: Array<{ label: string; types: string[] }> = [
  {
    label: 'Data',
    types: ['source', 'output'],
  },
  {
    label: 'Transform',
    types: ['filter', 'select', 'drop', 'cast', 'map', 'sort', 'limit', 'dedupe', 'fill', 'normalize'],
  },
  {
    label: 'Combine',
    types: ['join', 'union', 'lookup'],
  },
  {
    label: 'Reshape',
    types: ['aggregate', 'window', 'pivot', 'unpivot', 'flatten', 'split'],
  },
  {
    label: 'Advanced',
    types: ['compute', 'udf'],
  },
]

export const QuickAddMenu = ({ position, onSelect, onClose }: Props) => {
  const ref = useRef<HTMLDivElement>(null)

  /* ── Click-away listener ─── */
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        onClose()
      }
    }
    // Delay to avoid the same click that opened the menu
    const timer = setTimeout(() => document.addEventListener('mousedown', handler), 50)
    return () => {
      clearTimeout(timer)
      document.removeEventListener('mousedown', handler)
    }
  }, [onClose])

  /* ── Clamp position to viewport ─── */
  const menuWidth = 220
  const menuHeight = 480
  const x = Math.min(position.x, window.innerWidth - menuWidth - 16)
  const y = Math.min(position.y, window.innerHeight - menuHeight - 16)

  return (
    <div
      ref={ref}
      className="pipeline-quick-add-menu"
      style={{ left: x, top: y }}
    >
      <Menu>
        {MENU_SECTIONS.map((section, si) => (
          <div key={section.label}>
            {si > 0 && <MenuDivider />}
            <MenuDivider title={section.label} />
            {section.types.map((type) => {
              const cat = getCatalog(type)
              return (
                <MenuItem
                  key={type}
                  icon={<Icon icon={cat.icon as IconName} color={cat.color} />}
                  text={cat.label}
                  onClick={() => onSelect(type)}
                />
              )
            })}
          </div>
        ))}
      </Menu>
    </div>
  )
}
