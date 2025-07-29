import React, { useEffect, useRef } from 'react';
import { Card, Classes } from '@blueprintjs/core';
import * as d3 from 'd3';
import { Node } from 'reactflow';
import './MiniMap.scss';

interface MiniMapProps {
  nodes: Node[];
  width?: number;
  height?: number;
}

export const MiniMap: React.FC<MiniMapProps> = ({ 
  nodes, 
  width = 200, 
  height = 150 
}) => {
  const svgRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!svgRef.current || nodes.length === 0) return;

    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();

    // Calculate bounds
    const xExtent = d3.extent(nodes, d => d.position.x) as [number, number];
    const yExtent = d3.extent(nodes, d => d.position.y) as [number, number];
    
    const padding = 20;
    const xScale = d3.scaleLinear()
      .domain([xExtent[0] - padding, xExtent[1] + padding])
      .range([10, width - 10]);
    
    const yScale = d3.scaleLinear()
      .domain([yExtent[0] - padding, yExtent[1] + padding])
      .range([10, height - 10]);

    // Add background
    svg.append('rect')
      .attr('width', width)
      .attr('height', height)
      .attr('fill', 'rgba(16, 22, 26, 0.8)')
      .attr('stroke', '#394b59')
      .attr('stroke-width', 1)
      .attr('rx', 4);

    // Add nodes
    const nodeGroup = svg.append('g')
      .attr('class', 'minimap-nodes');

    nodeGroup.selectAll('rect')
      .data(nodes)
      .enter()
      .append('rect')
      .attr('x', d => xScale(d.position.x) - 4)
      .attr('y', d => yScale(d.position.y) - 3)
      .attr('width', 8)
      .attr('height', 6)
      .attr('fill', d => d.selected ? '#48aff0' : '#5c7080')
      .attr('rx', 1);

    // Add viewport indicator (simplified)
    const viewport = svg.append('rect')
      .attr('class', 'viewport-indicator')
      .attr('x', width * 0.25)
      .attr('y', height * 0.25)
      .attr('width', width * 0.5)
      .attr('height', height * 0.5)
      .attr('fill', 'none')
      .attr('stroke', '#48aff0')
      .attr('stroke-width', 2)
      .attr('stroke-dasharray', '4,2')
      .attr('rx', 2);

  }, [nodes, width, height]);

  return (
    <Card className={`minimap ${Classes.DARK}`} elevation={2}>
      <svg ref={svgRef} width={width} height={height} />
    </Card>
  );
};