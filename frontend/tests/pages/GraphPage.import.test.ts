import { describe, expect, it, vi } from 'vitest'

vi.mock('reactflow', () => ({
  __esModule: true,
  default: () => null,
  MiniMap: () => null,
  addEdge: (_connection: unknown, edges: unknown[]) => edges,
  Position: {
    Left: 'left',
    Right: 'right',
    Top: 'top',
    Bottom: 'bottom',
  },
  useNodesState: (initial: unknown[]) => [initial, () => undefined, () => undefined],
  useEdgesState: (initial: unknown[]) => [initial, () => undefined, () => undefined],
}))

describe('GraphPage module', () => {
  it('imports without side effects', async () => {
    const mod = await import('../../src/pages/GraphPage')
    expect(mod.GraphPage).toBeTypeOf('function')
  })
})

