import { Node, Edge, ConnectionMode, MarkerType } from 'reactflow';

// Custom node types for SPICE HARVESTER
export enum CustomNodeTypes {
  OBJECT_TYPE = 'objectType',
  LINK_TYPE = 'linkType',
  PROPERTY = 'property',
  ACTION = 'action',
  FUNCTION = 'function',
  DATA_SOURCE = 'dataSource',
  PIPELINE_STEP = 'pipelineStep',
  WIDGET = 'widget',
}

// Custom edge types
export enum CustomEdgeTypes {
  RELATION = 'relation',
  DEPENDENCY = 'dependency',
  DATA_FLOW = 'dataFlow',
  INHERITANCE = 'inheritance',
}

// Default node styles following Palantir's design
export const defaultNodeStyles = {
  objectType: {
    background: '#7C3AED',
    color: '#FFFFFF',
    border: '2px solid #6B21D3',
    borderRadius: '4px',
    fontSize: '12px',
    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
    padding: '10px 15px',
    minWidth: '150px',
    minHeight: '50px',
  },
  linkType: {
    background: '#3B82F6',
    color: '#FFFFFF',
    border: '2px solid #2563EB',
    borderRadius: '50%',
    fontSize: '12px',
    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
    padding: '15px',
    width: '80px',
    height: '80px',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
  },
  property: {
    background: '#10B981',
    color: '#FFFFFF',
    border: '2px solid #059669',
    borderRadius: '20px',
    fontSize: '11px',
    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
    padding: '8px 12px',
    minWidth: '120px',
  },
  dataSource: {
    background: '#F59E0B',
    color: '#FFFFFF',
    border: '2px solid #D97706',
    borderRadius: '4px',
    fontSize: '12px',
    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
    padding: '10px 15px',
    minWidth: '150px',
    minHeight: '50px',
  },
  pipelineStep: {
    background: '#8B5CF6',
    color: '#FFFFFF',
    border: '2px solid #7C3AED',
    borderRadius: '8px',
    fontSize: '12px',
    fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
    padding: '10px 15px',
    minWidth: '180px',
    minHeight: '60px',
  },
};

// Default edge styles
export const defaultEdgeStyles = {
  relation: {
    stroke: '#6E7179',
    strokeWidth: 2,
    markerEnd: {
      type: MarkerType.ArrowClosed,
      width: 20,
      height: 20,
      color: '#6E7179',
    },
  },
  dependency: {
    stroke: '#A7ABB6',
    strokeWidth: 2,
    strokeDasharray: '5,5',
    markerEnd: {
      type: MarkerType.Arrow,
      width: 20,
      height: 20,
      color: '#A7ABB6',
    },
  },
  dataFlow: {
    stroke: '#0072FF',
    strokeWidth: 3,
    animated: true,
    markerEnd: {
      type: MarkerType.ArrowClosed,
      width: 20,
      height: 20,
      color: '#0072FF',
    },
  },
  inheritance: {
    stroke: '#7C3AED',
    strokeWidth: 2,
    markerEnd: {
      type: MarkerType.Arrow,
      width: 25,
      height: 25,
      color: '#7C3AED',
    },
  },
};

// React Flow configuration
export const reactFlowConfig = {
  // General settings
  fitView: true,
  fitViewOptions: {
    padding: 0.2,
    includeHiddenNodes: false,
  },
  
  // Connection settings
  connectionMode: ConnectionMode.Loose,
  connectOnClick: true,
  connectionLineType: 'smoothstep' as const,
  connectionLineStyle: { stroke: '#A7ABB6', strokeWidth: 2 },
  
  // Interaction settings
  nodesDraggable: true,
  nodesConnectable: true,
  nodesFocusable: true,
  edgesFocusable: true,
  elementsSelectable: true,
  selectNodesOnDrag: false,
  panOnDrag: true,
  panOnScroll: false,
  zoomOnScroll: true,
  zoomOnPinch: true,
  zoomOnDoubleClick: true,
  preventScrolling: true,
  
  // Viewport settings
  minZoom: 0.1,
  maxZoom: 4,
  defaultZoom: 1,
  zoomActivationKeyCode: 'Meta',
  
  // Grid settings
  snapToGrid: true,
  snapGrid: [15, 15],
  
  // Controls
  deleteKeyCode: ['Backspace', 'Delete'],
  multiSelectionKeyCode: 'Shift',
  
  // Performance
  elevateNodesOnSelect: true,
  
  // Default styles
  defaultEdgeOptions: {
    type: 'smoothstep',
    animated: false,
    style: defaultEdgeStyles.relation,
  },
};

// Helper functions for React Flow
export const reactFlowHelpers = {
  // Create a new node
  createNode: (
    id: string,
    type: CustomNodeTypes,
    position: { x: number; y: number },
    data: any,
    style?: any
  ): Node => ({
    id,
    type,
    position,
    data,
    style: style || defaultNodeStyles[type as keyof typeof defaultNodeStyles],
    selected: false,
    draggable: true,
  }),
  
  // Create a new edge
  createEdge: (
    id: string,
    source: string,
    target: string,
    type: CustomEdgeTypes = CustomEdgeTypes.RELATION,
    data?: any,
    style?: any
  ): Edge => ({
    id,
    source,
    target,
    type,
    data,
    style: style || defaultEdgeStyles[type as keyof typeof defaultEdgeStyles],
    animated: type === CustomEdgeTypes.DATA_FLOW,
  }),
  
  // Auto layout nodes (simple grid layout)
  autoLayout: (nodes: Node[], columns = 4, spacing = { x: 200, y: 150 }) => {
    return nodes.map((node, index) => {
      const col = index % columns;
      const row = Math.floor(index / columns);
      return {
        ...node,
        position: {
          x: col * spacing.x,
          y: row * spacing.y,
        },
      };
    });
  },
  
  // Get connected nodes
  getConnectedNodes: (nodeId: string, edges: Edge[]) => {
    const connected = new Set<string>();
    edges.forEach((edge) => {
      if (edge.source === nodeId) connected.add(edge.target);
      if (edge.target === nodeId) connected.add(edge.source);
    });
    return Array.from(connected);
  },
  
  // Get node hierarchy
  getNodeHierarchy: (nodes: Node[], edges: Edge[]) => {
    const hierarchy: Map<string, string[]> = new Map();
    const roots: string[] = [];
    const children: Set<string> = new Set();
    
    edges.forEach((edge) => {
      children.add(edge.target);
      if (!hierarchy.has(edge.source)) {
        hierarchy.set(edge.source, []);
      }
      hierarchy.get(edge.source)!.push(edge.target);
    });
    
    nodes.forEach((node) => {
      if (!children.has(node.id)) {
        roots.push(node.id);
      }
    });
    
    return { hierarchy, roots };
  },
  
  // Export to JSON
  exportToJSON: (nodes: Node[], edges: Edge[]) => {
    return JSON.stringify({ nodes, edges }, null, 2);
  },
  
  // Import from JSON
  importFromJSON: (json: string): { nodes: Node[]; edges: Edge[] } => {
    try {
      const data = JSON.parse(json);
      return {
        nodes: data.nodes || [],
        edges: data.edges || [],
      };
    } catch (error) {
      console.error('Failed to parse JSON:', error);
      return { nodes: [], edges: [] };
    }
  },
};