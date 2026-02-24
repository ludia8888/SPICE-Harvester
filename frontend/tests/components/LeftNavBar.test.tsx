import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { LeftNavBar } from '../../src/components/layout/LeftNavBar'
import type { LnbGroup } from '../../src/AppShell'

const buildGroups = (): LnbGroup[] => [
  {
    id: 'workspace',
    title: 'Workspace',
    position: 'top',
    items: [
      { id: 'home', icon: 'home', label: 'Home', path: '/home' },
      { id: 'files', icon: 'folder-close', label: 'Files', path: '/files' },
      { id: 'settings', icon: 'cog', label: 'Settings', path: '__settings__' },
    ],
  },
  {
    id: 'admin',
    title: 'Admin',
    position: 'bottom',
    items: [{ id: 'user', icon: 'user', label: 'User', path: '/user' }],
  },
]

describe('LeftNavBar', () => {
  it('renders navigation items and utility controls', () => {
    render(
      <LeftNavBar
        groups={buildGroups()}
        expanded
        onToggleExpanded={vi.fn()}
        pathname="/home"
        project="core"
        onNavigate={vi.fn()}
        onSettingsClick={vi.fn()}
        selectProjectLabel="Select project first"
      />,
    )

    expect(screen.getByRole('button', { name: 'Toggle sidebar' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Home' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Files' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Settings' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'User' })).toBeInTheDocument()
  })

  it('routes regular items via onNavigate and settings via onSettingsClick', () => {
    const onNavigate = vi.fn()
    const onSettingsClick = vi.fn()

    render(
      <LeftNavBar
        groups={buildGroups()}
        expanded
        onToggleExpanded={vi.fn()}
        pathname="/home"
        project="core"
        onNavigate={onNavigate}
        onSettingsClick={onSettingsClick}
        selectProjectLabel="Select project first"
      />,
    )

    fireEvent.click(screen.getByRole('button', { name: 'Files' }))
    expect(onNavigate).toHaveBeenCalledWith('/files')

    fireEvent.click(screen.getByRole('button', { name: 'Settings' }))
    expect(onSettingsClick).toHaveBeenCalledTimes(1)
  })
})
