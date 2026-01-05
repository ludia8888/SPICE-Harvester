import { beforeEach, describe, expect, it } from 'vitest'
import { screen } from '@testing-library/react'
import { AppShell } from '../../src/app/AppShell'
import { useAppStore } from '../../src/state/store'
import { renderWithClient, resetAppStore } from '../testUtils'

describe('AppShell', () => {
  beforeEach(() => {
    resetAppStore()
  })

  it('renders the home view by default', () => {
    renderWithClient(<AppShell />)
    expect(screen.getByRole('heading', { name: 'Home' })).toBeInTheDocument()
  })

  it('renders a placeholder for unknown navigation keys', () => {
    useAppStore.setState({ activeNav: 'connectors' })
    renderWithClient(<AppShell />)
    expect(screen.getByRole('heading', { name: 'Connectors' })).toBeInTheDocument()
    expect(screen.getByText('Content coming soon.')).toBeInTheDocument()
  })
})
