import { InputNode } from './InputNode'
import { TransformNode } from './TransformNode'
import { OutputNode } from './OutputNode'

/**
 * Custom node type map for ReactFlow.
 * IMPORTANT: This object must be defined OUTSIDE any component
 * to prevent ReactFlow from remounting nodes on every render.
 */
export const pipelineNodeTypes = {
  inputNode: InputNode,
  transformNode: TransformNode,
  outputNode: OutputNode,
} as const
