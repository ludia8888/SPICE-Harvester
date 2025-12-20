import type { ReactElement } from 'react'
import { Button, Popover, Position } from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/icons'

export type RailItem = {
  icon: IconName
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
      {items.map((item) => (
        <Button
          key={item.label}
          minimal
          icon={item.icon}
          className={`rail-button ${item.active ? 'is-active' : ''}`}
          aria-label={item.label}
          title={item.label}
          onClick={item.onClick}
        />
      ))}
      <div className="rail-spacer" />
      <Popover content={settingsContent} position={Position.RIGHT}>
        <Button
          minimal
          icon="cog"
          className="rail-button"
          aria-label={settingsLabel}
          title={settingsLabel}
        />
      </Popover>
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
