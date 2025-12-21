import CytoscapeComponent from 'react-cytoscapejs'
import type { Core, ElementDefinition, EventObject, LayoutOptions } from 'cytoscape'

export type GraphCanvasSelect = {
  kind: 'node' | 'edge'
  data: Record<string, unknown>
}

export const GraphCanvas = ({
  elements,
  onSelect,
  layout,
  className,
}: {
  elements: ElementDefinition[]
  onSelect?: (selection: GraphCanvasSelect) => void
  layout?: LayoutOptions
  className?: string
}) => {
  return (
    <CytoscapeComponent
      className={className ?? 'graph-canvas'}
      elements={elements}
      layout={layout ?? { name: 'cose', animate: false, padding: 20 }}
      stylesheet={
        [
        {
          selector: 'node',
          style: {
            'background-color': '#137cbd',
            color: '#1b2631',
            label: 'data(label)',
            'text-wrap': 'wrap',
            'text-max-width': 140,
            'font-size': 10,
            'text-valign': 'center',
            'text-halign': 'center',
          },
        },
        {
          selector: 'edge',
          style: {
            width: 2,
            'line-color': '#8fa8c2',
            'target-arrow-color': '#8fa8c2',
            'target-arrow-shape': 'triangle',
            label: 'data(label)',
            'font-size': 9,
            'text-rotation': 'autorotate',
            'text-background-color': '#ffffff',
            'text-background-opacity': 0.7,
            'text-background-padding': 2,
          },
        },
        {
          selector: '.status-full',
          style: { 'background-color': '#0f9960' },
        },
        {
          selector: '.status-partial',
          style: { 'background-color': '#d9822b' },
        },
        {
          selector: '.status-missing',
          style: { 'background-color': '#c23030' },
        },
      ]
      }
      cy={(cy: Core) => {
        if (!onSelect) {
          return
        }
        cy.on('tap', 'node', (event: EventObject) => {
          const data = event.target.data() as Record<string, unknown>
          onSelect({ kind: 'node', data })
        })
        cy.on('tap', 'edge', (event: EventObject) => {
          const data = event.target.data() as Record<string, unknown>
          onSelect({ kind: 'edge', data })
        })
      }}
    />
  )
}
