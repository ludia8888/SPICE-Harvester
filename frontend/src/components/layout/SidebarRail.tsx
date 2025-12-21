import type { ReactElement } from 'react'
import { Icon, Popover, Position } from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/icons'

export type RailItem = {
  icon: IconName | string
  label: string
  active?: boolean
  onClick?: () => void
}

export type SidebarRailProps = {
  items: RailItem[]
  settingsContent: ReactElement
  settingsLabel: string
  userLabel: string
}

export const SidebarRail = ({ items, settingsContent, settingsLabel, userLabel }: SidebarRailProps) => {
  return (
    <aside className="sidebar-rail">
      <div className="rail-content">
        {items.map((item) => (
          <button
            key={item.label}
            className={`rail-item ${item.active ? 'is-active' : ''}`}
            onClick={item.onClick}
            title={item.label}
          >
            <Icon icon={item.icon as IconName} className="rail-icon" />
            <span className="rail-label">{item.label}</span>
          </button>
        ))}
      </div>

      <div className="rail-spacer" />

      <div className="rail-footer">
        <Popover content={settingsContent} position={Position.RIGHT}>
          <button className="rail-item" title={settingsLabel}>
            <Icon icon="cog" className="rail-icon" />
            <span className="rail-label">{settingsLabel}</span>
          </button>
        </Popover>

        <button className="rail-item" title={userLabel}>
          <Icon icon="user" className="rail-icon" />
          <span className="rail-label">{userLabel}</span>
        </button>
      </div>
    </aside>
  )
}
