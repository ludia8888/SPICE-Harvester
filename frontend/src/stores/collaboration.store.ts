import { create } from 'zustand';
import { devtools } from 'zustand/middleware';
import { immer } from 'zustand/middleware/immer';
import * as Y from 'yjs';
import { WebsocketProvider } from 'y-websocket';
import { createCollaborationProvider, ICollaborationProvider } from '@services/collaboration/yjs.config';

interface ICollaborator {
  id: string;
  clientId: number;
  name: string;
  color: string;
  cursor?: { x: number; y: number };
  selection?: string[];
  isOnline: boolean;
  lastSeen: Date;
}

interface ICollaborationRoom {
  id: string;
  name: string;
  type: 'ontology' | 'workshop' | 'pipeline' | 'graph';
  provider: ICollaborationProvider;
  collaborators: Map<number, ICollaborator>;
  isConnected: boolean;
}

interface ICollaborationState {
  // Current user
  userId: string;
  userName: string;
  userColor: string;
  
  // Active rooms
  rooms: Map<string, ICollaborationRoom>;
  activeRoomId: string | null;
  
  // Actions
  setUser: (userId: string, userName: string, userColor: string) => void;
  joinRoom: (roomId: string, roomName: string, roomType: ICollaborationRoom['type']) => void;
  leaveRoom: (roomId: string) => void;
  setActiveRoom: (roomId: string) => void;
  
  // Cursor and selection
  updateCursor: (roomId: string, cursor: { x: number; y: number } | null) => void;
  updateSelection: (roomId: string, selection: string[] | null) => void;
  
  // Get helpers
  getRoom: (roomId: string) => ICollaborationRoom | undefined;
  getActiveRoom: () => ICollaborationRoom | undefined;
  getCollaborators: (roomId: string) => ICollaborator[];
  
  // Cleanup
  cleanup: () => void;
}

export const useCollaborationStore = create<ICollaborationState>()(
  devtools(
    immer((set, get) => ({
      userId: '',
      userName: '',
      userColor: '#0072FF',
      rooms: new Map(),
      activeRoomId: null,
      
      setUser: (userId, userName, userColor) => {
        set((state) => {
          state.userId = userId;
          state.userName = userName;
          state.userColor = userColor;
        });
      },
      
      joinRoom: (roomId, roomName, roomType) => {
        const state = get();
        
        // Check if already in room
        if (state.rooms.has(roomId)) {
          state.setActiveRoom(roomId);
          return;
        }
        
        // Create collaboration provider
        const provider = createCollaborationProvider({
          roomId,
          userId: state.userId,
          userName: state.userName,
          userColor: state.userColor,
        });
        
        // Set up awareness listeners
        const collaborators = new Map<number, ICollaborator>();
        
        provider.awareness.on('change', ({ added, updated, removed }: any) => {
          set((state) => {
            const room = state.rooms.get(roomId);
            if (!room) return;
            
            // Handle added users
            added.forEach((clientId: number) => {
              const userState = provider.awareness.getStates().get(clientId);
              if (userState?.user) {
                room.collaborators.set(clientId, {
                  ...userState.user,
                  clientId,
                  isOnline: true,
                  lastSeen: new Date(),
                });
              }
            });
            
            // Handle updated users
            updated.forEach((clientId: number) => {
              const userState = provider.awareness.getStates().get(clientId);
              if (userState?.user) {
                const existing = room.collaborators.get(clientId);
                room.collaborators.set(clientId, {
                  ...existing,
                  ...userState.user,
                  clientId,
                  isOnline: true,
                  lastSeen: new Date(),
                });
              }
            });
            
            // Handle removed users
            removed.forEach((clientId: number) => {
              const existing = room.collaborators.get(clientId);
              if (existing) {
                room.collaborators.set(clientId, {
                  ...existing,
                  isOnline: false,
                  lastSeen: new Date(),
                });
              }
            });
          });
        });
        
        // Handle connection status
        provider.provider.on('status', ({ status }: { status: string }) => {
          set((state) => {
            const room = state.rooms.get(roomId);
            if (room) {
              room.isConnected = status === 'connected';
            }
          });
        });
        
        // Create room
        set((state) => {
          state.rooms.set(roomId, {
            id: roomId,
            name: roomName,
            type: roomType,
            provider,
            collaborators,
            isConnected: false,
          });
          state.activeRoomId = roomId;
        });
      },
      
      leaveRoom: (roomId) => {
        const room = get().rooms.get(roomId);
        if (!room) return;
        
        // Destroy provider
        room.provider.provider.destroy();
        room.provider.persistence?.destroy();
        
        set((state) => {
          state.rooms.delete(roomId);
          
          // Update active room
          if (state.activeRoomId === roomId) {
            const remainingRooms = Array.from(state.rooms.keys());
            state.activeRoomId = remainingRooms.length > 0 ? remainingRooms[0] : null;
          }
        });
      },
      
      setActiveRoom: (roomId) => {
        set((state) => {
          if (state.rooms.has(roomId)) {
            state.activeRoomId = roomId;
          }
        });
      },
      
      updateCursor: (roomId, cursor) => {
        const room = get().rooms.get(roomId);
        if (!room) return;
        
        room.provider.awareness.setLocalStateField('cursor', cursor);
      },
      
      updateSelection: (roomId, selection) => {
        const room = get().rooms.get(roomId);
        if (!room) return;
        
        room.provider.awareness.setLocalStateField('selection', selection);
      },
      
      getRoom: (roomId) => {
        return get().rooms.get(roomId);
      },
      
      getActiveRoom: () => {
        const state = get();
        return state.activeRoomId ? state.rooms.get(state.activeRoomId) : undefined;
      },
      
      getCollaborators: (roomId) => {
        const room = get().rooms.get(roomId);
        if (!room) return [];
        
        return Array.from(room.collaborators.values()).filter(
          (c) => c.clientId !== room.provider.provider.awareness.clientID
        );
      },
      
      cleanup: () => {
        const state = get();
        
        // Leave all rooms
        state.rooms.forEach((room) => {
          room.provider.provider.destroy();
          room.provider.persistence?.destroy();
        });
        
        set((state) => {
          state.rooms.clear();
          state.activeRoomId = null;
        });
      },
    })),
    {
      name: 'collaboration-store',
    }
  )
);

// Selectors
export const selectActiveRoom = (state: ICollaborationState) => state.getActiveRoom();
export const selectCollaborators = (state: ICollaborationState) => 
  state.activeRoomId ? state.getCollaborators(state.activeRoomId) : [];
export const selectIsConnected = (state: ICollaborationState) => 
  state.getActiveRoom()?.isConnected ?? false;

// Utility functions
export const generateUserColor = () => {
  const colors = [
    '#0072FF', // Blue
    '#7C3AED', // Purple
    '#10B981', // Green
    '#F59E0B', // Yellow
    '#EF4444', // Red
    '#EC4899', // Pink
    '#8B5CF6', // Violet
    '#06B6D4', // Cyan
  ];
  
  return colors[Math.floor(Math.random() * colors.length)];
};