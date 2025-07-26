import { io, Socket } from 'socket.io-client';

export interface ISocketConfig {
  url: string;
  options?: {
    reconnection?: boolean;
    reconnectionDelay?: number;
    reconnectionDelayMax?: number;
    reconnectionAttempts?: number;
    timeout?: number;
    autoConnect?: boolean;
    transports?: string[];
    auth?: Record<string, any>;
  };
}

export const defaultSocketConfig: ISocketConfig = {
  url: import.meta.env.VITE_WEBSOCKET_ENDPOINT || 'ws://localhost:4000',
  options: {
    reconnection: true,
    reconnectionDelay: 1000,
    reconnectionDelayMax: 5000,
    reconnectionAttempts: 5,
    timeout: 20000,
    autoConnect: false,
    transports: ['websocket', 'polling'],
  },
};

export function createSocket(config: ISocketConfig = defaultSocketConfig): Socket {
  const token = localStorage.getItem('spice-harvester-token');
  
  const socketConfig = {
    ...config.options,
    auth: {
      ...config.options?.auth,
      token,
    },
  };
  
  return io(config.url, socketConfig);
}

export enum SocketEvents {
  // Connection events
  CONNECT = 'connect',
  DISCONNECT = 'disconnect',
  CONNECT_ERROR = 'connect_error',
  RECONNECT = 'reconnect',
  RECONNECT_ATTEMPT = 'reconnect_attempt',
  RECONNECT_ERROR = 'reconnect_error',
  RECONNECT_FAILED = 'reconnect_failed',
  
  // Collaboration events
  JOIN_ROOM = 'join_room',
  LEAVE_ROOM = 'leave_room',
  ROOM_USERS = 'room_users',
  USER_CURSOR = 'user_cursor',
  USER_SELECTION = 'user_selection',
  
  // Data sync events
  SYNC_REQUEST = 'sync_request',
  SYNC_UPDATE = 'sync_update',
  SYNC_COMPLETE = 'sync_complete',
  
  // Graph events
  GRAPH_UPDATE = 'graph_update',
  GRAPH_NODE_ADD = 'graph_node_add',
  GRAPH_NODE_UPDATE = 'graph_node_update',
  GRAPH_NODE_DELETE = 'graph_node_delete',
  GRAPH_EDGE_ADD = 'graph_edge_add',
  GRAPH_EDGE_UPDATE = 'graph_edge_update',
  GRAPH_EDGE_DELETE = 'graph_edge_delete',
  
  // Ontology events
  ONTOLOGY_UPDATE = 'ontology_update',
  ONTOLOGY_LOCK = 'ontology_lock',
  ONTOLOGY_UNLOCK = 'ontology_unlock',
  
  // Error events
  ERROR = 'error',
  UNAUTHORIZED = 'unauthorized',
}