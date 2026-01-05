import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { SidebarRail, type RailItem } from '../../src/components/SidebarRail'

const buildItems = (overrides?: Partial<RailItem>[]) => {
  const base: RailItem[] = [
    { id: 'home', icon: 'home', label: 'Home', active: true, onClick: vi.fn() },
    { id: 'datasets', icon: 'folder-close', label: 'Files', onClick: vi.fn() },
    { id: 'ai-agent', icon: 'predictive-analysis', label: 'AI Agent', onClick: vi.fn() },
  ]
  if (!overrides) {
    return base
  }
  return base.map((item, index) => ({ ...item, ...overrides[index] }))
}

describe('SidebarRail', () => {
  it('renders primary items and puts the AI agent in the footer', () => {
    render(<SidebarRail items={buildItems()} />)

    expect(screen.getByTitle('Home')).toBeInTheDocument()
    expect(screen.getByTitle('Files')).toBeInTheDocument()

    const aiButton = screen.getByTitle('AI Agent')
    expect(aiButton).toBeInTheDocument()
    expect(aiButton.className).toContain('rail-item-ai')
  })

  it('fires hover and click callbacks', () => {
    const onHoverChange = vi.fn()
    const items = buildItems()
    render(<SidebarRail items={items} onHoverChange={onHoverChange} />)

    const rail = document.querySelector('.sidebar-rail')
    expect(rail).not.toBeNull()
    if (!rail) {
      return
    }

    fireEvent.mouseEnter(rail)
    fireEvent.mouseLeave(rail)
    expect(onHoverChange).toHaveBeenCalledWith(true)
    expect(onHoverChange).toHaveBeenCalledWith(false)

    fireEvent.click(screen.getByTitle('Files'))
    expect(items[1].onClick).toHaveBeenCalled()
  })
})
