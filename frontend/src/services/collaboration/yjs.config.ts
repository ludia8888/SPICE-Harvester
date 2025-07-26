import * as Y from 'yjs';
import { WebsocketProvider } from 'y-websocket';
import { IndexeddbPersistence } from 'y-indexeddb';

export interface ICollaborationConfig {
  roomId: string;
  userId: string;
  userName: string;
  userColor: string;
  websocketUrl?: string;
  enablePersistence?: boolean;
  enableAwareness?: boolean;
}

export interface ICollaborationProvider {
  doc: Y.Doc;
  provider: WebsocketProvider;
  persistence?: IndexeddbPersistence;
  awareness: any;
}

export function createCollaborationProvider(
  config: ICollaborationConfig
): ICollaborationProvider {
  const doc = new Y.Doc();
  
  const wsUrl = config.websocketUrl || import.meta.env.VITE_WEBSOCKET_ENDPOINT || 'ws://localhost:4000';
  const provider = new WebsocketProvider(wsUrl, config.roomId, doc, {
    connect: true,
    params: {
      userId: config.userId,
      userName: config.userName,
    },
  });
  
  // Set user awareness state
  if (config.enableAwareness !== false) {
    provider.awareness.setLocalStateField('user', {
      id: config.userId,
      name: config.userName,
      color: config.userColor,
      cursor: null,
      selection: null,
    });
  }
  
  // Enable offline persistence
  let persistence: IndexeddbPersistence | undefined;
  if (config.enablePersistence !== false) {
    persistence = new IndexeddbPersistence(config.roomId, doc);
  }
  
  return {
    doc,
    provider,
    persistence,
    awareness: provider.awareness,
  };
}

// Shared types for different collaborative data structures
export const createSharedTypes = (doc: Y.Doc) => ({
  // Graph structure
  nodes: doc.getMap<any>('nodes'),
  edges: doc.getMap<any>('edges'),
  
  // Ontology structure
  objectTypes: doc.getMap<any>('objectTypes'),
  linkTypes: doc.getMap<any>('linkTypes'),
  properties: doc.getMap<any>('properties'),
  
  // UI state
  viewport: doc.getMap<any>('viewport'),
  selectedNodes: doc.getArray<string>('selectedNodes'),
  selectedEdges: doc.getArray<string>('selectedEdges'),
  
  // Comments and annotations
  comments: doc.getMap<any>('comments'),
  annotations: doc.getMap<any>('annotations'),
  
  // History and versions
  history: doc.getArray<any>('history'),
  versions: doc.getMap<any>('versions'),
});

// Helper functions for common operations
export const CollaborationHelpers = {
  // Add a node to the shared graph
  addNode: (nodes: Y.Map<any>, node: any) => {
    nodes.set(node.id, node);
  },
  
  // Update a node in the shared graph
  updateNode: (nodes: Y.Map<any>, nodeId: string, updates: any) => {
    const node = nodes.get(nodeId);
    if (node) {
      nodes.set(nodeId, { ...node, ...updates });
    }
  },
  
  // Delete a node from the shared graph
  deleteNode: (nodes: Y.Map<any>, nodeId: string) => {
    nodes.delete(nodeId);
  },
  
  // Add an edge to the shared graph
  addEdge: (edges: Y.Map<any>, edge: any) => {
    edges.set(edge.id, edge);
  },
  
  // Update an edge in the shared graph
  updateEdge: (edges: Y.Map<any>, edgeId: string, updates: any) => {
    const edge = edges.get(edgeId);
    if (edge) {
      edges.set(edgeId, { ...edge, ...updates });
    }
  },
  
  // Delete an edge from the shared graph
  deleteEdge: (edges: Y.Map<any>, edgeId: string) => {
    edges.delete(edgeId);
  },
  
  // Get all users in the room
  getUsers: (awareness: any) => {
    const users: any[] = [];
    awareness.getStates().forEach((state: any, clientId: number) => {
      if (state.user && clientId !== awareness.clientID) {
        users.push({ ...state.user, clientId });
      }
    });
    return users;
  },
  
  // Update cursor position
  updateCursor: (awareness: any, cursor: { x: number; y: number } | null) => {
    awareness.setLocalStateField('cursor', cursor);
  },
  
  // Update selection
  updateSelection: (awareness: any, selection: string[] | null) => {
    awareness.setLocalStateField('selection', selection);
  },
};