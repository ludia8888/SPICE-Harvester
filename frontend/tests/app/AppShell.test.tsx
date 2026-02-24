import { beforeEach, describe, expect, it, vi } from 'vitest'
import { screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import AppShell from '../../src/AppShell'
import { useAppStore } from '../../src/store/useAppStore'
import { renderWithClient, resetAppStore } from '../testUtils'

vi.mock('../../src/app/AppRouter', () => ({
  AppRouter: () => <div data-testid="app-router">Router outlet</div>,
}))

vi.mock('../../src/commands/useCommandTracker', () => ({
  useCommandTracker: () => undefined,
}))

vi.mock('../../src/commands/CommandTrackerDrawer', () => ({
  CommandTrackerDrawer: () => <div data-testid="command-tracker-drawer" />,
}))

vi.mock('../../src/components/SettingsDialog', () => ({
  SettingsDialog: () => <div data-testid="settings-dialog" />,
}))

vi.mock('../../src/components/layout/InspectorDrawer', () => ({
  InspectorDrawer: () => <div data-testid="inspector-drawer" />,
}))

vi.mock('../../src/components/layout/SidebarRail', () => ({
  SidebarRail: () => <div data-testid="sidebar-rail" />,
}))

vi.mock('../../src/components/layout/SettingsPopoverContent', () => ({
  SettingsPopoverContent: () => <div data-testid="settings-popover-content" />,
}))

vi.mock('../../src/state/usePathname', () => ({
  usePathname: () => '/',
}))

describe('AppShell', () => {
  beforeEach(() => {
    resetAppStore()
  })

  it('renders app shell with no selected project', () => {
    useAppStore.setState({
      context: { project: null, branch: 'main', language: 'en' },
    })
    renderWithClient(<AppShell />)

    expect(screen.getByText('Spice OS')).toBeInTheDocument()
    expect(screen.getAllByText('No project selected').length).toBeGreaterThan(0)
    expect(screen.getAllByRole('button', { name: 'Projects' }).length).toBeGreaterThan(0)
    expect(screen.getByRole('button', { name: 'Settings' })).toBeInTheDocument()
    expect(screen.getByTestId('app-router')).toBeInTheDocument()
  })

  it('shows project navigation and opens settings via store action', async () => {
    const user = userEvent.setup()
    useAppStore.setState({
      context: { project: 'core', branch: 'main', language: 'en' },
      settingsOpen: false,
    })
    renderWithClient(<AppShell />)

    expect(screen.getAllByText('core').length).toBeGreaterThan(0)
    expect(screen.getByRole('button', { name: 'Overview' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Ontology' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Graph Explorer' })).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Settings' }))
    expect(useAppStore.getState().settingsOpen).toBe(true)
  })
})
