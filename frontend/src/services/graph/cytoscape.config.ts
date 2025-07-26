import cytoscape, { Core, CytoscapeOptions, Stylesheet } from 'cytoscape';
import cola from 'cytoscape-cola';
import dagre from 'cytoscape-dagre';
import fcose from 'cytoscape-fcose';

// Register layout extensions
cytoscape.use(cola);
cytoscape.use(dagre);
cytoscape.use(fcose);

// Base styles following Palantir's design system
export const cytoscapeStyles: Stylesheet[] = [
  // Node styles
  {
    selector: 'node',
    style: {
      'background-color': '#0072FF',
      'border-color': '#005BCC',
      'border-width': 1,
      'label': 'data(label)',
      'text-valign': 'center',
      'text-halign': 'center',
      'font-size': '12px',
      'font-family': '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
      'color': '#383A3F',
      'text-outline-color': '#FFFFFF',
      'text-outline-width': 2,
      'width': 60,
      'height': 60,
      'overlay-padding': 6,
    },
  },
  
  // Edge styles
  {
    selector: 'edge',
    style: {
      'curve-style': 'bezier',
      'target-arrow-shape': 'triangle',
      'target-arrow-color': '#6E7179',
      'line-color': '#A7ABB6',
      'width': 2,
      'label': 'data(label)',
      'font-size': '11px',
      'text-rotation': 'autorotate',
      'text-margin-y': -10,
      'color': '#6E7179',
      'text-outline-color': '#FFFFFF',
      'text-outline-width': 2,
    },
  },
  
  // Selected state
  {
    selector: ':selected',
    style: {
      'background-color': '#338EFF',
      'border-color': '#0072FF',
      'border-width': 3,
      'line-color': '#0072FF',
      'target-arrow-color': '#0072FF',
    },
  },
  
  // Hover state
  {
    selector: ':active',
    style: {
      'overlay-color': '#0072FF',
      'overlay-opacity': 0.2,
    },
  },
  
  // Object type specific styles
  {
    selector: '.object-type',
    style: {
      'shape': 'round-rectangle',
      'background-color': '#7C3AED', // Purple for object types
      'border-color': '#6B21D3',
    },
  },
  
  // Link type specific styles
  {
    selector: '.link-type',
    style: {
      'shape': 'diamond',
      'background-color': '#3B82F6', // Blue for link types
      'border-color': '#2563EB',
    },
  },
  
  // Property specific styles
  {
    selector: '.property',
    style: {
      'shape': 'ellipse',
      'background-color': '#10B981', // Green for properties
      'border-color': '#059669',
      'width': 50,
      'height': 50,
    },
  },
  
  // Status indicators
  {
    selector: '.experimental',
    style: {
      'border-style': 'dashed',
      'border-color': '#FF8000', // Orange for experimental
    },
  },
  
  {
    selector: '.deprecated',
    style: {
      'opacity': 0.5,
      'border-color': '#A7ABB6', // Gray for deprecated
    },
  },
  
  // Locked state
  {
    selector: '.locked',
    style: {
      'background-blacken': 0.2,
      'border-width': 3,
      'border-color': '#FF3300', // Red for locked
    },
  },
  
  // Hidden elements
  {
    selector: '.hidden',
    style: {
      'display': 'none',
    },
  },
  
  // Highlighted elements
  {
    selector: '.highlighted',
    style: {
      'background-color': '#FFB347',
      'border-color': '#FF8000',
      'z-index': 9999,
    },
  },
];

// Layout configurations
export const layoutConfigs = {
  // Force-directed layout for general graphs
  fcose: {
    name: 'fcose',
    quality: 'default',
    randomize: true,
    animate: true,
    animationDuration: 1000,
    animationEasing: 'ease-out',
    fit: true,
    padding: 50,
    nodeDimensionsIncludeLabels: true,
    uniformNodeDimensions: false,
    packComponents: true,
    nodeRepulsion: 4500,
    idealEdgeLength: 50,
    edgeElasticity: 0.45,
    nestingFactor: 0.1,
    gravity: 0.25,
    numIter: 2500,
    tile: true,
    tilingPaddingVertical: 10,
    tilingPaddingHorizontal: 10,
    gravityRangeCompound: 1.5,
    gravityCompound: 1.0,
    gravityRange: 3.8,
    initialEnergyOnIncremental: 0.3,
  },
  
  // Hierarchical layout for tree-like structures
  dagre: {
    name: 'dagre',
    rankDir: 'TB', // Top to bottom
    align: 'center',
    rankSep: 100,
    nodeSep: 70,
    edgeSep: 20,
    animate: true,
    animationDuration: 500,
    animationEasing: 'ease-out',
    fit: true,
    padding: 50,
    spacingFactor: 1.2,
  },
  
  // Physics-based layout
  cola: {
    name: 'cola',
    animate: true,
    refresh: 1,
    maxSimulationTime: 4000,
    ungrabifyWhileSimulating: false,
    fit: true,
    padding: 30,
    nodeSpacing: 50,
    edgeLengthVal: 50,
    alignment: undefined,
    gapInequalities: undefined,
    centerGraph: true,
    convergenceThreshold: 0.01,
    handleDisconnected: true,
    randomize: false,
  },
  
  // Circular layout for overview
  circle: {
    name: 'circle',
    fit: true,
    padding: 50,
    avoidOverlap: true,
    startAngle: 0,
    sweep: undefined,
    clockwise: true,
    radius: undefined,
    animate: true,
    animationDuration: 500,
    animationEasing: 'ease-out',
  },
  
  // Grid layout for organized view
  grid: {
    name: 'grid',
    fit: true,
    padding: 50,
    avoidOverlap: true,
    avoidOverlapPadding: 10,
    condense: false,
    rows: undefined,
    cols: undefined,
    animate: true,
    animationDuration: 500,
    animationEasing: 'ease-out',
  },
};

// Default Cytoscape configuration
export const defaultCytoscapeConfig: CytoscapeOptions = {
  container: undefined, // Will be set when initializing
  elements: [],
  style: cytoscapeStyles,
  layout: layoutConfigs.fcose,
  
  // Interaction options
  minZoom: 0.1,
  maxZoom: 10,
  wheelSensitivity: 0.1,
  boxSelectionEnabled: true,
  panningEnabled: true,
  userPanningEnabled: true,
  zoomingEnabled: true,
  userZoomingEnabled: true,
  selectionType: 'single',
  
  // Performance options
  hideEdgesOnViewport: false,
  textureOnViewport: false,
  motionBlur: false,
  motionBlurOpacity: 0.2,
  pixelRatio: 'auto',
};

// Helper functions
export const cytoscapeHelpers = {
  // Initialize Cytoscape instance
  init: (container: HTMLElement, config?: Partial<CytoscapeOptions>): Core => {
    return cytoscape({
      ...defaultCytoscapeConfig,
      container,
      ...config,
    });
  },
  
  // Apply layout
  applyLayout: (cy: Core, layoutName: keyof typeof layoutConfigs) => {
    const layout = cy.layout(layoutConfigs[layoutName]);
    layout.run();
    return layout;
  },
  
  // Fit to viewport
  fit: (cy: Core, padding = 50) => {
    cy.fit(undefined, padding);
  },
  
  // Center on nodes
  center: (cy: Core, nodes?: cytoscape.NodeCollection) => {
    if (nodes && nodes.length > 0) {
      cy.animate({
        center: { eles: nodes },
        zoom: cy.zoom(),
        duration: 500,
      });
    } else {
      cy.center();
    }
  },
  
  // Export image
  exportImage: (cy: Core, options?: any) => {
    return cy.png({
      bg: '#FFFFFF',
      full: true,
      scale: 2,
      ...options,
    });
  },
  
  // Export JSON
  exportJSON: (cy: Core) => {
    return cy.json();
  },
  
  // Import JSON
  importJSON: (cy: Core, json: any) => {
    cy.json(json);
  },
};