import { describe, expect, test } from 'vitest'
import { createDefaultDefinition, createId } from './pipelineTypes'

describe('pipelineTypes', () => {
  test('createDefaultDefinition returns empty graph with defaults', () => {
    const def = createDefaultDefinition()
    expect(def.nodes).toEqual([])
    expect(def.edges).toEqual([])
    expect(def.settings?.engine).toBe('Batch')
    expect(def.settings?.schedule).toBe('Manual')
  })

  test('createId includes prefix', () => {
    const id = createId('node')
    expect(id.startsWith('node-')).toBe(true)
  })
})

