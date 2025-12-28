import CytoscapeComponent from 'react-cytoscapejs'
import { Card, H3, Text } from '@blueprintjs/core'

const elements = [
  { data: { id: 'a', label: 'Dataset A' } },
  { data: { id: 'b', label: 'Transform' } },
  { data: { id: 'c', label: 'Dataset B' } },
  { data: { source: 'a', target: 'b' } },
  { data: { source: 'b', target: 'c' } },
]

const style = [
  {
    selector: 'node',
    style: {
      label: 'data(label)',
      backgroundColor: '#137cbd',
      color: '#f5f8fa',
      textOutlineColor: '#137cbd',
      textOutlineWidth: 2,
      fontSize: 10,
    },
  },
  {
    selector: 'edge',
    style: {
      width: 2,
      lineColor: '#5c7080',
      targetArrowColor: '#5c7080',
      targetArrowShape: 'triangle',
      curveStyle: 'bezier',
    },
  },
]

export const GraphPage = () => (
  <div className="page">
    <H3>Pipeline Graph</H3>
    <Card className="card">
      <Text>This is a minimal Cytoscape canvas wired into the UI.</Text>
      <div className="graph-frame">
        <CytoscapeComponent elements={elements} stylesheet={style} style={{ width: '100%', height: '100%' }} />
      </div>
    </Card>
  </div>
)
