import { NavLink } from 'react-router-dom'
import { Button } from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/icons'

export type RailItem = {
  icon: IconName
  label: string
  active?: boolean
  onClick?: () => void
  to?: string
}

export type SidebarRailProps = {
  items: RailItem[]
  onOpenSettings: () => void
  settingsLabel: string
  userLabel: string
}

export const SidebarRail = ({ items, onOpenSettings, settingsLabel, userLabel }: SidebarRailProps) => {
  return (
    <aside className="sidebar-rail">
      {items.map((item) => (
        <span key={item.label}>
          {item.to ? (
            <NavLink to={item.to} className="rail-link" title={item.label}>
              {({ isActive }) => (
                <Button
                  minimal
                  icon={item.icon}
                  className={`rail-button ${item.active || isActive ? 'is-active' : ''}`}
                  aria-label={item.label}
                />
              )}
            </NavLink>
          ) : (
            <Button
              minimal
              icon={item.icon}
              className={`rail-button ${item.active ? 'is-active' : ''}`}
              aria-label={item.label}
              title={item.label}
              onClick={item.onClick}
            />
          )}
        </span>
      ))}
      <div className="rail-spacer" />
      <Button
        minimal
        icon="cog"
        className="rail-button"
        aria-label={settingsLabel}
        title={settingsLabel}
        onClick={onOpenSettings}
      />
      <Button
        minimal
        icon="user"
        className="rail-button"
        aria-label={userLabel}
        title={userLabel}
      />
    </aside>
  )
}
