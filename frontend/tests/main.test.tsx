import { describe, expect, it, beforeEach, vi } from 'vitest'

describe('main entrypoint', () => {
  beforeEach(() => {
    document.body.innerHTML = '<div id="root"></div>'
    vi.resetModules()
  })

  it('boots the application root', async () => {
    const render = vi.fn()
    const createRootMock = vi.fn(() => ({ render }))

    vi.doMock('react-dom/client', () => ({ createRoot: createRootMock }))
    vi.doMock('../src/app/AppShell', () => ({
      AppShell: () => <div>App Shell</div>,
    }))

    await import('../src/main')

    expect(createRootMock).toHaveBeenCalled()
    expect(render).toHaveBeenCalled()
  })
})
