import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { SidebarRail, type RailItem } from '../../src/components/layout/SidebarRail'

const buildItems = (): RailItem[] => [
  { icon: 'home', label: 'Home', active: true, onClick: vi.fn() },
  { icon: 'folder-close', label: 'Files', onClick: vi.fn() },
  { icon: 'predictive-analysis', label: 'AI Agent', onClick: vi.fn() },
]

describe('SidebarRail', () => {
  it('renders primary rail items and utility buttons', () => {
    render(
      <SidebarRail
        items={buildItems()}
        settingsContent={<div>Settings content</div>}
        settingsLabel="Settings"
        userLabel="User"
      />,
    )

    expect(screen.getByTitle('Home')).toBeInTheDocument()
    expect(screen.getByTitle('Files')).toBeInTheDocument()
    expect(screen.getByTitle('AI Agent')).toBeInTheDocument()
    expect(screen.getByTitle('Settings')).toBeInTheDocument()
    expect(screen.getByTitle('User')).toBeInTheDocument()
  })

  it('fires item click callback', () => {
    const items = buildItems()
    render(
      <SidebarRail
        items={items}
        settingsContent={<div>Settings content</div>}
        settingsLabel="Settings"
        userLabel="User"
      />,
    )

    fireEvent.click(screen.getByTitle('Files'))
    expect(items[1].onClick).toHaveBeenCalled()
  })
})
