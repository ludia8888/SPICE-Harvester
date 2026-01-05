import { beforeEach, describe, expect, it } from 'vitest'
import { useAppStore } from '../../src/state/store'

describe('useAppStore', () => {
  beforeEach(() => {
    useAppStore.setState({ activeNav: 'home', pipelineContext: null })
  })

  it('defaults to home with no pipeline context', () => {
    const state = useAppStore.getState()
    expect(state.activeNav).toBe('home')
    expect(state.pipelineContext).toBeNull()
  })

  it('updates navigation and pipeline context', () => {
    useAppStore.getState().setActiveNav('pipeline')
    useAppStore.getState().setPipelineContext({ folderId: 'db-1', folderName: 'Core' })

    const state = useAppStore.getState()
    expect(state.activeNav).toBe('pipeline')
    expect(state.pipelineContext).toEqual({ folderId: 'db-1', folderName: 'Core' })
  })
})
