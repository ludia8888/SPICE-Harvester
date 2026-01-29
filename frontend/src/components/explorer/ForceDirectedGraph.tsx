import { useRef, useEffect, useCallback, useMemo } from 'react'
import * as d3 from 'd3'
import { Button, Icon } from '@blueprintjs/core'

export type GraphNode = {
  id: string
  label: string
  classType: string
  degree?: number
  x?: number
  y?: number
  fx?: number | null
  fy?: number | null
}

export type GraphEdge = {
  id: string
  source: string | GraphNode
  target: string | GraphNode
  predicate: string
  style?: 'solid' | 'dashed'
}

type ForceDirectedGraphProps = {
  nodes: GraphNode[]
  edges: GraphEdge[]
  selectedNodeId?: string | null
  onNodeClick?: (node: GraphNode) => void
  onNodeDoubleClick?: (node: GraphNode) => void
  width?: number
  height?: number
}

const CLASS_COLORS: Record<string, string> = {
  Person: '#137cbd',
  Account: '#0f9960',
  Transaction: '#d9822b',
  PhoneCall: '#9179f2',
  Organization: '#db3737',
  default: '#8a9ba8',
}

const getNodeColor = (classType: string): string => {
  return CLASS_COLORS[classType] || CLASS_COLORS.default
}

const getNodeRadius = (degree: number = 1): number => {
  return Math.min(30, Math.max(16, 12 + Math.sqrt(degree) * 4))
}

export const ForceDirectedGraph = ({
  nodes,
  edges,
  selectedNodeId,
  onNodeClick,
  onNodeDoubleClick,
  width = 800,
  height = 500,
}: ForceDirectedGraphProps) => {
  const svgRef = useRef<SVGSVGElement>(null)
  const zoomRef = useRef<d3.ZoomBehavior<SVGSVGElement, unknown> | null>(null)

  const uniqueClasses = useMemo(() => {
    const classes = new Set(nodes.map((n) => n.classType))
    return Array.from(classes)
  }, [nodes])

  const handleZoomIn = useCallback(() => {
    const svgEl = svgRef.current
    const zoom = zoomRef.current
    if (!svgEl || !zoom) return
    const svg = d3.select(svgEl)
    svg.transition().duration(300).call(zoom.scaleBy as never, 1.3)
  }, [])

  const handleZoomOut = useCallback(() => {
    const svgEl = svgRef.current
    const zoom = zoomRef.current
    if (!svgEl || !zoom) return
    const svg = d3.select(svgEl)
    svg.transition().duration(300).call(zoom.scaleBy as never, 0.7)
  }, [])

  const handleFitView = useCallback(() => {
    const svgEl = svgRef.current
    const zoom = zoomRef.current
    if (!svgEl || !zoom) return
    const svg = d3.select(svgEl)
    svg.transition().duration(300).call(
      zoom.transform as never,
      d3.zoomIdentity.translate(width / 2, height / 2).scale(0.8),
    )
  }, [width, height])

  useEffect(() => {
    if (!svgRef.current || nodes.length === 0) return

    const svg = d3.select(svgRef.current)
    svg.selectAll('*').remove()

    const g = svg.append('g').attr('class', 'graph-container')

    // Zoom behavior
    const zoom = d3.zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.1, 4])
      .on('zoom', (event) => {
        g.attr('transform', event.transform)
      })

    zoomRef.current = zoom
    svg.call(zoom)
    svg.call(zoom.transform, d3.zoomIdentity.translate(width / 2, height / 2))

    // Create node data with degree
    const nodeMap = new Map(nodes.map((n) => [n.id, { ...n, degree: 0 }]))
    edges.forEach((e) => {
      const sourceId = typeof e.source === 'string' ? e.source : e.source.id
      const targetId = typeof e.target === 'string' ? e.target : e.target.id
      const sourceNode = nodeMap.get(sourceId)
      const targetNode = nodeMap.get(targetId)
      if (sourceNode) sourceNode.degree = (sourceNode.degree || 0) + 1
      if (targetNode) targetNode.degree = (targetNode.degree || 0) + 1
    })

    const graphNodes = Array.from(nodeMap.values())
    const graphEdges = edges.map((e) => ({
      ...e,
      source: typeof e.source === 'string' ? e.source : e.source.id,
      target: typeof e.target === 'string' ? e.target : e.target.id,
    }))

    // Force simulation
    const simulation = d3.forceSimulation(graphNodes as d3.SimulationNodeDatum[])
      .force('link', d3.forceLink(graphEdges as d3.SimulationLinkDatum<d3.SimulationNodeDatum>[])
        .id((d: unknown) => (d as GraphNode).id)
        .distance(100))
      .force('charge', d3.forceManyBody().strength(-300))
      .force('center', d3.forceCenter(0, 0))
      .force('collision', d3.forceCollide().radius(50))

    // Arrow marker
    svg.append('defs').append('marker')
      .attr('id', 'arrowhead')
      .attr('viewBox', '-0 -5 10 10')
      .attr('refX', 25)
      .attr('refY', 0)
      .attr('orient', 'auto')
      .attr('markerWidth', 6)
      .attr('markerHeight', 6)
      .append('path')
      .attr('d', 'M 0,-5 L 10,0 L 0,5')
      .attr('fill', '#5c7080')

    // Links
    const link = g.append('g')
      .attr('class', 'links')
      .selectAll('line')
      .data(graphEdges)
      .enter()
      .append('line')
      .attr('class', 'graph-link')
      .attr('stroke', '#5c7080')
      .attr('stroke-width', 1.5)
      .attr('stroke-dasharray', (d) => d.style === 'dashed' ? '5,5' : 'none')
      .attr('marker-end', 'url(#arrowhead)')

    // Link labels
    const linkLabel = g.append('g')
      .attr('class', 'link-labels')
      .selectAll('text')
      .data(graphEdges)
      .enter()
      .append('text')
      .attr('class', 'graph-link-label')
      .attr('text-anchor', 'middle')
      .attr('fill', '#8a9ba8')
      .attr('font-size', '10px')
      .attr('opacity', 0)
      .text((d) => d.predicate)

    // Nodes
    const node = g.append('g')
      .attr('class', 'nodes')
      .selectAll('g')
      .data(graphNodes)
      .enter()
      .append('g')
      .attr('class', 'graph-node')
      .style('cursor', 'pointer')
      .call(d3.drag<SVGGElement, GraphNode>()
        .on('start', (event, d) => {
          if (!event.active) simulation.alphaTarget(0.3).restart()
          d.fx = d.x
          d.fy = d.y
        })
        .on('drag', (event, d) => {
          d.fx = event.x
          d.fy = event.y
        })
        .on('end', (event, d) => {
          if (!event.active) simulation.alphaTarget(0)
          d.fx = null
          d.fy = null
        }) as never)

    // Node circles
    node.append('circle')
      .attr('r', (d) => getNodeRadius(d.degree))
      .attr('fill', (d) => getNodeColor(d.classType))
      .attr('stroke', (d) => d.id === selectedNodeId ? '#48aff0' : '#293742')
      .attr('stroke-width', (d) => d.id === selectedNodeId ? 3 : 2)

    // Node labels
    node.append('text')
      .attr('class', 'graph-node-label')
      .attr('text-anchor', 'middle')
      .attr('dy', (d) => getNodeRadius(d.degree) + 14)
      .attr('fill', '#f5f8fa')
      .attr('font-size', '11px')
      .text((d) => d.label.length > 15 ? d.label.slice(0, 15) + '...' : d.label)

    // Hover effects
    node
      .on('mouseenter', function (event, d) {
        d3.select(this).select('circle')
          .transition()
          .duration(200)
          .attr('r', getNodeRadius(d.degree) + 4)

        link
          .attr('opacity', (l) => {
            const sourceId = typeof l.source === 'string' ? l.source : (l.source as GraphNode).id
            const targetId = typeof l.target === 'string' ? l.target : (l.target as GraphNode).id
            return sourceId === d.id || targetId === d.id ? 1 : 0.2
          })
          .attr('stroke-width', (l) => {
            const sourceId = typeof l.source === 'string' ? l.source : (l.source as GraphNode).id
            const targetId = typeof l.target === 'string' ? l.target : (l.target as GraphNode).id
            return sourceId === d.id || targetId === d.id ? 2.5 : 1.5
          })

        linkLabel
          .attr('opacity', (l) => {
            const sourceId = typeof l.source === 'string' ? l.source : (l.source as GraphNode).id
            const targetId = typeof l.target === 'string' ? l.target : (l.target as GraphNode).id
            return sourceId === d.id || targetId === d.id ? 1 : 0
          })
      })
      .on('mouseleave', function (event, d) {
        d3.select(this).select('circle')
          .transition()
          .duration(200)
          .attr('r', getNodeRadius(d.degree))

        link.attr('opacity', 1).attr('stroke-width', 1.5)
        linkLabel.attr('opacity', 0)
      })
      .on('click', (event, d) => {
        event.stopPropagation()
        onNodeClick?.(d)
      })
      .on('dblclick', (event, d) => {
        event.stopPropagation()
        onNodeDoubleClick?.(d)
      })

    // Tick function
    simulation.on('tick', () => {
      link
        .attr('x1', (d) => {
          const src = d.source as unknown as GraphNode
          return src.x || 0
        })
        .attr('y1', (d) => {
          const src = d.source as unknown as GraphNode
          return src.y || 0
        })
        .attr('x2', (d) => {
          const tgt = d.target as unknown as GraphNode
          return tgt.x || 0
        })
        .attr('y2', (d) => {
          const tgt = d.target as unknown as GraphNode
          return tgt.y || 0
        })

      linkLabel
        .attr('x', (d) => {
          const src = d.source as unknown as GraphNode
          const tgt = d.target as unknown as GraphNode
          return ((src.x || 0) + (tgt.x || 0)) / 2
        })
        .attr('y', (d) => {
          const src = d.source as unknown as GraphNode
          const tgt = d.target as unknown as GraphNode
          return ((src.y || 0) + (tgt.y || 0)) / 2
        })

      node.attr('transform', (d) => `translate(${d.x || 0},${d.y || 0})`)
    })

    return () => {
      simulation.stop()
      zoomRef.current = null
    }
  }, [nodes, edges, selectedNodeId, onNodeClick, onNodeDoubleClick, width, height])

  if (nodes.length === 0) {
    return (
      <div className="graph-empty">
        <Icon icon="graph" size={32} />
        <span>검색 결과를 선택하면 관계 그래프가 표시됩니다</span>
      </div>
    )
  }

  return (
    <div className="force-graph-container" style={{ width, height }}>
      <div className="force-graph-legend">
        {uniqueClasses.map((classType) => (
          <div key={classType} className="force-graph-legend-item">
            <span
              className="force-graph-legend-dot"
              style={{ backgroundColor: getNodeColor(classType) }}
            />
            <span className="force-graph-legend-label">{classType}</span>
          </div>
        ))}
      </div>
      <div className="force-graph-controls">
        <Button minimal icon="zoom-in" onClick={handleZoomIn} aria-label="확대" />
        <Button minimal icon="zoom-out" onClick={handleZoomOut} aria-label="축소" />
        <Button minimal icon="zoom-to-fit" onClick={handleFitView} aria-label="맞춤" />
      </div>
      <svg
        ref={svgRef}
        width="100%"
        height="100%"
        className="force-graph-svg"
      />
    </div>
  )
}
