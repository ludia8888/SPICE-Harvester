import { Icon } from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/icons'

export type RailItem = {
  icon: IconName | string
  label: string
  active?: boolean
  onClick?: () => void
}

type SidebarRailProps = {
  items: RailItem[]
  onHoverChange?: (expanded: boolean) => void
}

export const SidebarRail = ({ items, onHoverChange }: SidebarRailProps) => (
  <aside
    className="sidebar-rail"
    onMouseEnter={() => onHoverChange?.(true)}
    onMouseLeave={() => onHoverChange?.(false)}
  >
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
      <button className="rail-item" title="Settings">
        <Icon icon="cog" className="rail-icon" />
        <span className="rail-label">Settings</span>
      </button>
      <button className="rail-item" title="User">
        <Icon icon="user" className="rail-icon" />
        <span className="rail-label">User</span>
      </button>
    </div>
  </aside>
)
