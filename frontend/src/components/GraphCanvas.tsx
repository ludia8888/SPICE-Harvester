import { useEffect, useRef } from 'react'
import cytoscape, { type Core } from 'cytoscape'

export type GraphCanvasNode = {
  id: string
  label: string
  status?: string
  raw: Record<string, unknown>
}

export type GraphCanvasEdge = {
  id: string
  source: string
  target: string
  label: string
  raw: Record<string, unknown>
}

export const GraphCanvas = ({
  nodes,
  edges,
  height = 420,
  onNodeSelect,
  onEdgeSelect,
}: {
  nodes: GraphCanvasNode[]
  edges: GraphCanvasEdge[]
  height?: number
  onNodeSelect?: (node: GraphCanvasNode) => void
  onEdgeSelect?: (edge: GraphCanvasEdge) => void
}) => {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const cyRef = useRef<Core | null>(null)

  useEffect(() => {
    if (!containerRef.current) {
      return
    }

    cyRef.current?.destroy()
    const elements = [
      ...nodes.map((node) => ({
        data: {
          id: node.id,
          label: node.label,
          status: node.status ?? 'UNKNOWN',
          raw: node,
        },
      })),
      ...edges.map((edge) => ({
        data: {
          id: edge.id,
          source: edge.source,
          target: edge.target,
          label: edge.label,
          raw: edge,
        },
      })),
    ]

    const cy = cytoscape({
      container: containerRef.current,
      elements,
      layout: { name: 'cose', animate: false, fit: true },
      style: [
        {
          selector: 'node',
          style: {
            'background-color': '#394b59',
            'label': 'data(label)',
            'color': '#f5f8fa',
            'text-valign': 'center',
            'text-halign': 'center',
            'font-size': '11px',
            'text-wrap': 'wrap',
            'text-max-width': '120px',
            'border-width': 1,
            'border-color': '#5c7080',
          },
        },
        {
          selector: 'node[status = "FULL"]',
          style: {
            'background-color': '#0f9960',
            'border-color': '#0a6640',
          },
        },
        {
          selector: 'node[status = "PARTIAL"]',
          style: {
            'background-color': '#d9822b',
            'border-color': '#a66321',
          },
        },
        {
          selector: 'node[status = "MISSING"]',
          style: {
            'background-color': '#db3737',
            'border-color': '#a82a2a',
          },
        },
        {
          selector: 'edge',
          style: {
            'curve-style': 'bezier',
            'width': 2,
            'line-color': '#5c7080',
            'target-arrow-shape': 'triangle',
            'target-arrow-color': '#5c7080',
            'label': 'data(label)',
            'font-size': '10px',
            'color': '#f5f8fa',
            'text-rotation': 'autorotate',
            'text-background-color': '#1f2933',
            'text-background-opacity': 0.7,
            'text-background-padding': '2px',
          },
        },
        {
          selector: '.selected',
          style: {
            'border-color': '#48aff0',
            'border-width': 2,
          },
        },
      ],
    })

    cy.on('tap', 'node', (event) => {
      const raw = event.target.data('raw') as GraphCanvasNode
      cy.elements().removeClass('selected')
      event.target.addClass('selected')
      onNodeSelect?.(raw)
    })
    cy.on('tap', 'edge', (event) => {
      const raw = event.target.data('raw') as GraphCanvasEdge
      cy.elements().removeClass('selected')
      event.target.addClass('selected')
      onEdgeSelect?.(raw)
    })

    cyRef.current = cy

    return () => {
      cyRef.current?.destroy()
      cyRef.current = null
    }
  }, [edges, nodes, onEdgeSelect, onNodeSelect])

  return (
    <div
      ref={containerRef}
      className="graph-canvas"
      style={{ height }}
    />
  )
}
