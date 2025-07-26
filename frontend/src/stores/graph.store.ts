import { create } from 'zustand';
import { devtools } from 'zustand/middleware';
import { immer } from 'zustand/middleware/immer';
import type { Node, Edge } from 'reactflow';
import type { Core as CytoscapeCore } from 'cytoscape';

export interface IGraphNode extends Node {
  data: {
    label: string;
    type: 'objectType' | 'linkType' | 'property' | 'action' | 'function' | 'dataSource';
    status?: 'active' | 'experimental' | 'deprecated';
    icon?: string;
    color?: string;
    metadata?: Record<string, any>;
  };
}

export interface IGraphEdge extends Edge {
  data?: {
    label?: string;
    type: 'relation' | 'dependency' | 'dataFlow' | 'inheritance';
    cardinality?: string;
    metadata?: Record<string, any>;
  };
}

interface IGraphViewport {
  x: number;
  y: number;
  zoom: number;
}

interface IGraphFilter {
  nodeTypes?: string[];
  edgeTypes?: string[];
  status?: string[];
  search?: string;
}

interface IGraphLayout {
  type: 'force' | 'hierarchical' | 'circular' | 'grid' | 'dagre' | 'cola';
  options?: Record<string, any>;
}

interface IGraphState {
  // Graph data
  nodes: IGraphNode[];
  edges: IGraphEdge[];
  
  // Selection
  selectedNodes: string[];
  selectedEdges: string[];
  
  // Viewport
  viewport: IGraphViewport;
  
  // Filters
  filters: IGraphFilter;
  
  // Layout
  layout: IGraphLayout;
  
  // History for undo/redo
  history: {
    past: Array<{ nodes: IGraphNode[]; edges: IGraphEdge[] }>;
    future: Array<{ nodes: IGraphNode[]; edges: IGraphEdge[] }>;
  };
  
  // Cytoscape instance (for complex graph operations)
  cytoscapeInstance: CytoscapeCore | null;
  
  // Actions - Data management
  setNodes: (nodes: IGraphNode[]) => void;
  setEdges: (edges: IGraphEdge[]) => void;
  addNode: (node: IGraphNode) => void;
  updateNode: (nodeId: string, updates: Partial<IGraphNode>) => void;
  removeNode: (nodeId: string) => void;
  addEdge: (edge: IGraphEdge) => void;
  updateEdge: (edgeId: string, updates: Partial<IGraphEdge>) => void;
  removeEdge: (edgeId: string) => void;
  
  // Actions - Selection
  selectNodes: (nodeIds: string[]) => void;
  selectEdges: (edgeIds: string[]) => void;
  addNodeToSelection: (nodeId: string) => void;
  removeNodeFromSelection: (nodeId: string) => void;
  clearSelection: () => void;
  selectAll: () => void;
  
  // Actions - Viewport
  setViewport: (viewport: IGraphViewport) => void;
  fitView: () => void;
  zoomIn: () => void;
  zoomOut: () => void;
  resetView: () => void;
  
  // Actions - Filters
  setFilters: (filters: IGraphFilter) => void;
  clearFilters: () => void;
  
  // Actions - Layout
  setLayout: (layout: IGraphLayout) => void;
  applyLayout: () => void;
  
  // Actions - History
  undo: () => void;
  redo: () => void;
  saveSnapshot: () => void;
  
  // Actions - Cytoscape
  setCytoscapeInstance: (cy: CytoscapeCore | null) => void;
  
  // Actions - Import/Export
  importGraph: (data: { nodes: IGraphNode[]; edges: IGraphEdge[] }) => void;
  exportGraph: () => { nodes: IGraphNode[]; edges: IGraphEdge[] };
  
  // Getters
  getNode: (nodeId: string) => IGraphNode | undefined;
  getEdge: (edgeId: string) => IGraphEdge | undefined;
  getConnectedNodes: (nodeId: string) => IGraphNode[];
  getNodeEdges: (nodeId: string) => IGraphEdge[];
  getFilteredNodes: () => IGraphNode[];
  getFilteredEdges: () => IGraphEdge[];
}

export const useGraphStore = create<IGraphState>()(
  devtools(
    immer((set, get) => ({
      nodes: [],
      edges: [],
      selectedNodes: [],
      selectedEdges: [],
      viewport: { x: 0, y: 0, zoom: 1 },
      filters: {},
      layout: { type: 'force' },
      history: { past: [], future: [] },
      cytoscapeInstance: null,
      
      // Data management
      setNodes: (nodes) => {
        set((state) => {
          state.saveSnapshot();
          state.nodes = nodes;
        });
      },
      
      setEdges: (edges) => {
        set((state) => {
          state.saveSnapshot();
          state.edges = edges;
        });
      },
      
      addNode: (node) => {
        set((state) => {
          state.saveSnapshot();
          state.nodes.push(node);
        });
      },
      
      updateNode: (nodeId, updates) => {
        set((state) => {
          state.saveSnapshot();
          const index = state.nodes.findIndex((n) => n.id === nodeId);
          if (index !== -1) {
            state.nodes[index] = { ...state.nodes[index], ...updates };
          }
        });
      },
      
      removeNode: (nodeId) => {
        set((state) => {
          state.saveSnapshot();
          state.nodes = state.nodes.filter((n) => n.id !== nodeId);
          state.edges = state.edges.filter((e) => e.source !== nodeId && e.target !== nodeId);
          state.selectedNodes = state.selectedNodes.filter((id) => id !== nodeId);
        });
      },
      
      addEdge: (edge) => {
        set((state) => {
          state.saveSnapshot();
          state.edges.push(edge);
        });
      },
      
      updateEdge: (edgeId, updates) => {
        set((state) => {
          state.saveSnapshot();
          const index = state.edges.findIndex((e) => e.id === edgeId);
          if (index !== -1) {
            state.edges[index] = { ...state.edges[index], ...updates };
          }
        });
      },
      
      removeEdge: (edgeId) => {
        set((state) => {
          state.saveSnapshot();
          state.edges = state.edges.filter((e) => e.id !== edgeId);
          state.selectedEdges = state.selectedEdges.filter((id) => id !== edgeId);
        });
      },
      
      // Selection
      selectNodes: (nodeIds) => {
        set((state) => {
          state.selectedNodes = nodeIds;
          state.selectedEdges = [];
        });
      },
      
      selectEdges: (edgeIds) => {
        set((state) => {
          state.selectedEdges = edgeIds;
          state.selectedNodes = [];
        });
      },
      
      addNodeToSelection: (nodeId) => {
        set((state) => {
          if (!state.selectedNodes.includes(nodeId)) {
            state.selectedNodes.push(nodeId);
          }
        });
      },
      
      removeNodeFromSelection: (nodeId) => {
        set((state) => {
          state.selectedNodes = state.selectedNodes.filter((id) => id !== nodeId);
        });
      },
      
      clearSelection: () => {
        set((state) => {
          state.selectedNodes = [];
          state.selectedEdges = [];
        });
      },
      
      selectAll: () => {
        set((state) => {
          state.selectedNodes = state.nodes.map((n) => n.id);
          state.selectedEdges = state.edges.map((e) => e.id);
        });
      },
      
      // Viewport
      setViewport: (viewport) => {
        set((state) => {
          state.viewport = viewport;
        });
      },
      
      fitView: () => {
        const cy = get().cytoscapeInstance;
        if (cy) {
          cy.fit();
          const viewport = cy.viewport();
          set((state) => {
            state.viewport = {
              x: viewport.x,
              y: viewport.y,
              zoom: cy.zoom(),
            };
          });
        }
      },
      
      zoomIn: () => {
        set((state) => {
          state.viewport.zoom = Math.min(state.viewport.zoom * 1.2, 10);
        });
      },
      
      zoomOut: () => {
        set((state) => {
          state.viewport.zoom = Math.max(state.viewport.zoom / 1.2, 0.1);
        });
      },
      
      resetView: () => {
        set((state) => {
          state.viewport = { x: 0, y: 0, zoom: 1 };
        });
      },
      
      // Filters
      setFilters: (filters) => {
        set((state) => {
          state.filters = filters;
        });
      },
      
      clearFilters: () => {
        set((state) => {
          state.filters = {};
        });
      },
      
      // Layout
      setLayout: (layout) => {
        set((state) => {
          state.layout = layout;
        });
      },
      
      applyLayout: () => {
        const cy = get().cytoscapeInstance;
        if (cy) {
          const layout = cy.layout(get().layout as any);
          layout.run();
        }
      },
      
      // History
      undo: () => {
        set((state) => {
          const prev = state.history.past.pop();
          if (prev) {
            state.history.future.unshift({
              nodes: [...state.nodes],
              edges: [...state.edges],
            });
            state.nodes = prev.nodes;
            state.edges = prev.edges;
          }
        });
      },
      
      redo: () => {
        set((state) => {
          const next = state.history.future.shift();
          if (next) {
            state.history.past.push({
              nodes: [...state.nodes],
              edges: [...state.edges],
            });
            state.nodes = next.nodes;
            state.edges = next.edges;
          }
        });
      },
      
      saveSnapshot: () => {
        const current = {
          nodes: [...get().nodes],
          edges: [...get().edges],
        };
        
        set((state) => {
          state.history.past.push(current);
          state.history.future = [];
          
          // Limit history size
          if (state.history.past.length > 50) {
            state.history.past.shift();
          }
        });
      },
      
      // Cytoscape
      setCytoscapeInstance: (cy) => {
        set((state) => {
          state.cytoscapeInstance = cy;
        });
      },
      
      // Import/Export
      importGraph: (data) => {
        set((state) => {
          state.saveSnapshot();
          state.nodes = data.nodes;
          state.edges = data.edges;
          state.clearSelection();
        });
      },
      
      exportGraph: () => {
        return {
          nodes: get().nodes,
          edges: get().edges,
        };
      },
      
      // Getters
      getNode: (nodeId) => {
        return get().nodes.find((n) => n.id === nodeId);
      },
      
      getEdge: (edgeId) => {
        return get().edges.find((e) => e.id === edgeId);
      },
      
      getConnectedNodes: (nodeId) => {
        const state = get();
        const connectedIds = new Set<string>();
        
        state.edges.forEach((edge) => {
          if (edge.source === nodeId) connectedIds.add(edge.target);
          if (edge.target === nodeId) connectedIds.add(edge.source);
        });
        
        return state.nodes.filter((n) => connectedIds.has(n.id));
      },
      
      getNodeEdges: (nodeId) => {
        return get().edges.filter((e) => e.source === nodeId || e.target === nodeId);
      },
      
      getFilteredNodes: () => {
        const { nodes, filters } = get();
        
        return nodes.filter((node) => {
          if (filters.nodeTypes && !filters.nodeTypes.includes(node.data.type)) {
            return false;
          }
          
          if (filters.status && node.data.status && !filters.status.includes(node.data.status)) {
            return false;
          }
          
          if (filters.search) {
            const search = filters.search.toLowerCase();
            return node.data.label.toLowerCase().includes(search);
          }
          
          return true;
        });
      },
      
      getFilteredEdges: () => {
        const { edges, filters } = get();
        const filteredNodeIds = new Set(get().getFilteredNodes().map((n) => n.id));
        
        return edges.filter((edge) => {
          // Only show edges where both nodes are visible
          if (!filteredNodeIds.has(edge.source) || !filteredNodeIds.has(edge.target)) {
            return false;
          }
          
          if (filters.edgeTypes && edge.data?.type && !filters.edgeTypes.includes(edge.data.type)) {
            return false;
          }
          
          return true;
        });
      },
    })),
    {
      name: 'graph-store',
    }
  )
);

// Selectors
export const selectNodes = (state: IGraphState) => state.nodes;
export const selectEdges = (state: IGraphState) => state.edges;
export const selectSelectedNodes = (state: IGraphState) => state.selectedNodes;
export const selectSelectedEdges = (state: IGraphState) => state.selectedEdges;
export const selectViewport = (state: IGraphState) => state.viewport;
export const selectFilters = (state: IGraphState) => state.filters;
export const selectLayout = (state: IGraphState) => state.layout;
export const selectCanUndo = (state: IGraphState) => state.history.past.length > 0;
export const selectCanRedo = (state: IGraphState) => state.history.future.length > 0;