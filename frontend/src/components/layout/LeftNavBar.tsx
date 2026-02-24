import { useState, useCallback } from 'react'
import { Button, Collapse, Icon, Tooltip } from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/icons'
import type { LnbGroup, LnbItem, LnbChildItem } from '../../AppShell'

/* ── Helpers ── */

const isChildActive = (child: LnbChildItem, pathname: string): boolean =>
  child.match ? pathname.startsWith(child.match) : pathname === child.path

const isItemActive = (item: LnbItem, pathname: string): boolean => {
  if (item.match && pathname.startsWith(item.match)) return true
  if (!item.match && pathname === item.path) return true
  if (item.children) {
    return item.children.some((c) => isChildActive(c, pathname))
  }
  return false
}

const isDisabled = (item: LnbItem | LnbChildItem, project: string | null): boolean =>
  Boolean(item.requiresProject && !project)

/* ── Sub-components ── */

type LnbNavItemProps = {
  item: LnbItem
  expanded: boolean
  pathname: string
  project: string | null
  onNavigate: (path: string) => void
  onSettingsClick: () => void
  selectProjectLabel: string
}

const LnbNavItem = ({
  item,
  expanded,
  pathname,
  project,
  onNavigate,
  onSettingsClick,
  selectProjectLabel,
}: LnbNavItemProps) => {
  const [subOpen, setSubOpen] = useState(false)
  const active = isItemActive(item, pathname)
  const disabled = isDisabled(item, project)
  const hasChildren = Boolean(item.children?.length)

  const handleClick = useCallback(() => {
    if (item.path === '__settings__') {
      onSettingsClick()
      return
    }
    if (disabled) return
    if (hasChildren && expanded) {
      setSubOpen((prev) => !prev)
      return
    }
    onNavigate(item.path)
  }, [item.path, disabled, hasChildren, expanded, onSettingsClick, onNavigate])

  const tooltipContent = disabled ? selectProjectLabel : item.label

  const button = (
    <button
      className={`lnb-item ${active ? 'is-active' : ''} ${disabled ? 'is-disabled' : ''}`}
      onClick={handleClick}
      aria-label={item.label}
      title={expanded ? undefined : item.label}
    >
      <Icon icon={item.icon as IconName} size={16} />
      <span className="lnb-item-label">{item.label}</span>
      {hasChildren && expanded && (
        <Icon
          icon={subOpen ? 'chevron-down' : 'chevron-right'}
          size={12}
          className="lnb-item-chevron"
        />
      )}
    </button>
  )

  return (
    <div className="lnb-item-wrapper">
      {!expanded ? (
        <Tooltip content={tooltipContent} position="right" compact>
          {button}
        </Tooltip>
      ) : (
        button
      )}
      {hasChildren && expanded && (
        <Collapse isOpen={subOpen}>
          <div className="lnb-children">
            {item.children!.map((child) => {
              const childActive = isChildActive(child, pathname)
              const childDisabled = isDisabled(child, project)
              return (
                <button
                  key={child.id}
                  className={`lnb-child-item ${childActive ? 'is-active' : ''} ${childDisabled ? 'is-disabled' : ''}`}
                  onClick={() => {
                    if (!childDisabled) onNavigate(child.path)
                  }}
                >
                  {child.label}
                </button>
              )
            })}
          </div>
        </Collapse>
      )}
    </div>
  )
}

/* ── Main component ── */

export type LeftNavBarProps = {
  groups: LnbGroup[]
  expanded: boolean
  onToggleExpanded: () => void
  pathname: string
  project: string | null
  onNavigate: (path: string) => void
  onSettingsClick: () => void
  selectProjectLabel: string
}

export const LeftNavBar = ({
  groups,
  expanded,
  onToggleExpanded,
  pathname,
  project,
  onNavigate,
  onSettingsClick,
  selectProjectLabel,
}: LeftNavBarProps) => {
  const topGroups = groups.filter((g) => g.position === 'top')
  const bottomGroups = groups.filter((g) => g.position === 'bottom')

  const renderGroup = (group: LnbGroup) => (
    <div className="lnb-group" key={group.id}>
      {expanded && <div className="lnb-group-title">{group.title}</div>}
      {group.items.map((item) => (
        <LnbNavItem
          key={item.id}
          item={item}
          expanded={expanded}
          pathname={pathname}
          project={project}
          onNavigate={onNavigate}
          onSettingsClick={onSettingsClick}
          selectProjectLabel={selectProjectLabel}
        />
      ))}
    </div>
  )

  return (
    <aside className="lnb">
      <div className="lnb-header">
        <Button
          minimal
          icon="menu"
          className="lnb-toggle"
          onClick={onToggleExpanded}
          aria-label="Toggle sidebar"
        />
        {expanded && <span className="lnb-title">SPICE</span>}
      </div>

      <div className="lnb-top-groups">
        {topGroups.map(renderGroup)}
      </div>

      <div className="lnb-spacer" />

      <div className="lnb-bottom-groups">
        {bottomGroups.map(renderGroup)}
      </div>
    </aside>
  )
}
