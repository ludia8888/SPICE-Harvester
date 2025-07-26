import * as d3 from 'd3';
import { customColors } from '@design-system/tokens';
import { Colors } from '@blueprintjs/core';

// D3 visualization types
export enum D3VisualizationType {
  FORCE_GRAPH = 'forceGraph',
  TREE = 'tree',
  TREEMAP = 'treemap',
  SUNBURST = 'sunburst',
  SANKEY = 'sankey',
  CHORD = 'chord',
  HEATMAP = 'heatmap',
  NETWORK = 'network',
  HIERARCHY = 'hierarchy',
}

// Color scales based on design system
export const colorScales = {
  categorical: d3.scaleOrdinal([
    Colors.BLUE3,
    customColors.keys.primary,
    customColors.keys.title,
    customColors.keys.foreign,
    Colors.GREEN3,
    Colors.ORANGE3,
    Colors.RED3,
  ]),
  
  sequential: d3.scaleSequential()
    .domain([0, 100])
    .interpolator(d3.interpolateBlues),
  
  diverging: d3.scaleDiverging()
    .domain([-1, 0, 1])
    .interpolator(d3.interpolateRdBu),
  
  status: d3.scaleOrdinal()
    .domain(['active', 'experimental', 'deprecated'])
    .range([
      customColors.status.active,
      customColors.status.experimental,
      customColors.status.deprecated,
    ]),
};

// Force simulation configuration
export const forceSimulationConfig = {
  // Force strengths
  charge: {
    strength: -300,
    distanceMin: 20,
    distanceMax: 400,
  },
  
  link: {
    distance: 100,
    strength: 0.7,
  },
  
  collision: {
    radius: 30,
    strength: 0.7,
  },
  
  center: {
    strength: 0.1,
  },
  
  // Simulation settings
  alpha: 1,
  alphaMin: 0.001,
  alphaDecay: 0.0228,
  velocityDecay: 0.4,
};

// Tree layout configuration
export const treeLayoutConfig = {
  // Layout settings
  nodeSize: [150, 200],
  separation: (a: any, b: any) => (a.parent === b.parent ? 1 : 1.5),
  
  // Orientation
  orientation: 'vertical' as 'vertical' | 'horizontal',
  
  // Spacing
  levelSeparation: 100,
  siblineSeparation: 50,
  subtreeSeparation: 80,
};

// D3 helper functions
export const d3Helpers = {
  // Create SVG container
  createSvg: (
    container: HTMLElement,
    width: number,
    height: number,
    margin = { top: 20, right: 20, bottom: 20, left: 20 }
  ) => {
    const svg = d3.select(container)
      .append('svg')
      .attr('width', width)
      .attr('height', height)
      .attr('viewBox', `0 0 ${width} ${height}`)
      .attr('preserveAspectRatio', 'xMidYMid meet');
    
    const g = svg.append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);
    
    return { svg, g, width: width - margin.left - margin.right, height: height - margin.top - margin.bottom };
  },
  
  // Create zoom behavior
  createZoom: (svg: d3.Selection<SVGSVGElement, unknown, null, undefined>, g: d3.Selection<SVGGElement, unknown, null, undefined>) => {
    const zoom = d3.zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.1, 10])
      .on('zoom', (event) => {
        g.attr('transform', event.transform);
      });
    
    svg.call(zoom);
    
    return {
      zoom,
      reset: () => {
        svg.transition()
          .duration(750)
          .call(zoom.transform, d3.zoomIdentity);
      },
      zoomIn: () => {
        svg.transition()
          .duration(750)
          .call(zoom.scaleBy, 1.2);
      },
      zoomOut: () => {
        svg.transition()
          .duration(750)
          .call(zoom.scaleBy, 0.8);
      },
    };
  },
  
  // Create force simulation
  createForceSimulation: (nodes: any[], links: any[], config = forceSimulationConfig) => {
    const simulation = d3.forceSimulation(nodes)
      .force('link', d3.forceLink(links)
        .id((d: any) => d.id)
        .distance(config.link.distance)
        .strength(config.link.strength))
      .force('charge', d3.forceManyBody()
        .strength(config.charge.strength)
        .distanceMin(config.charge.distanceMin)
        .distanceMax(config.charge.distanceMax))
      .force('collision', d3.forceCollide()
        .radius((d: any) => d.radius || config.collision.radius)
        .strength(config.collision.strength))
      .force('center', d3.forceCenter()
        .strength(config.center.strength))
      .alpha(config.alpha)
      .alphaMin(config.alphaMin)
      .alphaDecay(config.alphaDecay)
      .velocityDecay(config.velocityDecay);
    
    return simulation;
  },
  
  // Create tree layout
  createTreeLayout: (data: any, config = treeLayoutConfig) => {
    const root = d3.hierarchy(data);
    
    const treeLayout = config.orientation === 'horizontal'
      ? d3.tree<any>().nodeSize([config.nodeSize[1], config.nodeSize[0]]).separation(config.separation)
      : d3.tree<any>().nodeSize(config.nodeSize).separation(config.separation);
    
    return treeLayout(root);
  },
  
  // Create tooltip
  createTooltip: (container: HTMLElement) => {
    const tooltip = d3.select(container)
      .append('div')
      .attr('class', 'app-d3-tooltip')
      .style('position', 'absolute')
      .style('visibility', 'hidden')
      .style('background-color', Colors.DARK_GRAY5)
      .style('color', Colors.WHITE)
      .style('padding', '8px 12px')
      .style('border-radius', '4px')
      .style('font-size', '12px')
      .style('font-family', '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif')
      .style('box-shadow', '0 4px 6px -1px rgba(0, 0, 0, 0.1)')
      .style('pointer-events', 'none')
      .style('z-index', '9999');
    
    return {
      show: (content: string, event: MouseEvent) => {
        tooltip
          .style('visibility', 'visible')
          .html(content)
          .style('left', `${event.pageX + 10}px`)
          .style('top', `${event.pageY - 10}px`);
      },
      hide: () => {
        tooltip.style('visibility', 'hidden');
      },
      update: (event: MouseEvent) => {
        tooltip
          .style('left', `${event.pageX + 10}px`)
          .style('top', `${event.pageY - 10}px`);
      },
    };
  },
  
  // Create legend
  createLegend: (container: HTMLElement, items: { label: string; color: string }[]) => {
    const legend = d3.select(container)
      .append('div')
      .attr('class', 'app-d3-legend')
      .style('display', 'flex')
      .style('flex-direction', 'column')
      .style('gap', '8px')
      .style('padding', '12px')
      .style('background-color', Colors.LIGHT_GRAY5)
      .style('border', `1px solid ${Colors.GRAY5}`)
      .style('border-radius', '4px');
    
    const legendItems = legend.selectAll('.legend-item')
      .data(items)
      .enter()
      .append('div')
      .attr('class', 'legend-item')
      .style('display', 'flex')
      .style('align-items', 'center')
      .style('gap', '8px');
    
    legendItems.append('div')
      .style('width', '16px')
      .style('height', '16px')
      .style('background-color', (d) => d.color)
      .style('border-radius', '2px');
    
    legendItems.append('span')
      .text((d) => d.label)
      .style('font-size', '12px')
      .style('color', Colors.DARK_GRAY1);
    
    return legend;
  },
  
  // Transition utilities
  transition: {
    duration: 750,
    ease: d3.easeCubicInOut,
    
    fadeIn: (selection: d3.Selection<any, any, any, any>) => {
      return selection
        .style('opacity', 0)
        .transition()
        .duration(750)
        .ease(d3.easeCubicInOut)
        .style('opacity', 1);
    },
    
    fadeOut: (selection: d3.Selection<any, any, any, any>) => {
      return selection
        .transition()
        .duration(750)
        .ease(d3.easeCubicInOut)
        .style('opacity', 0)
        .remove();
    },
    
    slideIn: (selection: d3.Selection<any, any, any, any>, from: 'left' | 'right' | 'top' | 'bottom' = 'bottom') => {
      const transforms = {
        left: 'translate(-100%, 0)',
        right: 'translate(100%, 0)',
        top: 'translate(0, -100%)',
        bottom: 'translate(0, 100%)',
      };
      
      return selection
        .style('transform', transforms[from])
        .style('opacity', 0)
        .transition()
        .duration(750)
        .ease(d3.easeCubicInOut)
        .style('transform', 'translate(0, 0)')
        .style('opacity', 1);
    },
  },
  
  // Export utilities
  exportSvg: (svg: SVGSVGElement) => {
    const serializer = new XMLSerializer();
    const source = serializer.serializeToString(svg);
    const blob = new Blob([source], { type: 'image/svg+xml;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    
    const link = document.createElement('a');
    link.href = url;
    link.download = 'visualization.svg';
    link.click();
    
    URL.revokeObjectURL(url);
  },
  
  exportPng: (svg: SVGSVGElement, scale = 2) => {
    const serializer = new XMLSerializer();
    const source = serializer.serializeToString(svg);
    const image = new Image();
    const canvas = document.createElement('canvas');
    const context = canvas.getContext('2d')!;
    
    const width = svg.getBoundingClientRect().width;
    const height = svg.getBoundingClientRect().height;
    
    canvas.width = width * scale;
    canvas.height = height * scale;
    context.scale(scale, scale);
    
    image.onload = () => {
      context.drawImage(image, 0, 0);
      canvas.toBlob((blob) => {
        if (blob) {
          const url = URL.createObjectURL(blob);
          const link = document.createElement('a');
          link.href = url;
          link.download = 'visualization.png';
          link.click();
          URL.revokeObjectURL(url);
        }
      }, 'image/png');
    };
    
    image.src = 'data:image/svg+xml;base64,' + btoa(unescape(encodeURIComponent(source)));
  },
};