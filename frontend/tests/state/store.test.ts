import { beforeEach, describe, expect, it } from 'vitest'
import { useAppStore } from '../../src/store/useAppStore'

describe('useAppStore', () => {
  beforeEach(() => {
    useAppStore.setState({
      context: { project: null, branch: 'main', language: 'en' },
      theme: 'light',
      adminToken: '',
      rememberToken: false,
      adminMode: false,
      settingsOpen: false,
      inspector: null,
      commands: {},
    })
  })

  it('updates project, branch, and language context', () => {
    const state = useAppStore.getState()

    state.setProject('core')
    expect(useAppStore.getState().context.project).toBe('core')

    state.setBranch('feature-1')
    expect(useAppStore.getState().context.branch).toBe('feature-1')

    state.setLanguage('ko')
    expect(useAppStore.getState().context.language).toBe('ko')
  })

  it('tracks and updates command lifecycle', () => {
    const state = useAppStore.getState()

    state.trackCommand({
      id: 'cmd-1',
      kind: 'CREATE_ONTOLOGY',
      target: { dbName: 'core', classId: 'Order' },
      context: { project: 'core', branch: 'main' },
      submittedAt: new Date().toISOString(),
      writePhase: 'SUBMITTED',
      indexPhase: 'UNKNOWN',
    })

    expect(useAppStore.getState().commands['cmd-1']?.writePhase).toBe('SUBMITTED')

    state.patchCommand('cmd-1', { writePhase: 'WRITE_DONE', indexPhase: 'VISIBLE_IN_SEARCH' })
    expect(useAppStore.getState().commands['cmd-1']?.writePhase).toBe('WRITE_DONE')
    expect(useAppStore.getState().commands['cmd-1']?.indexPhase).toBe('VISIBLE_IN_SEARCH')

    state.removeCommand('cmd-1')
    expect(useAppStore.getState().commands['cmd-1']).toBeUndefined()
  })
})
