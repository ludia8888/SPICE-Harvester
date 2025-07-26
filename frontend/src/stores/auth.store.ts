import { create } from 'zustand';
import { devtools, persist } from 'zustand/middleware';
import { immer } from 'zustand/middleware/immer';
import { updateAuthToken, resetRelayStore } from '../relay/environment';

interface IUser {
  id: string;
  username: string;
  email: string;
  displayName: string;
  avatar?: string;
  roles: string[];
  groups: string[];
}

interface IAuthState {
  user: IUser | null;
  token: string | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  error: string | null;
  
  // Actions
  login: (email: string, password: string) => Promise<void>;
  logout: () => void;
  refreshToken: () => Promise<void>;
  setUser: (user: IUser) => void;
  setToken: (token: string) => void;
  setError: (error: string | null) => void;
  clearError: () => void;
}

export const useAuthStore = create<IAuthState>()(
  devtools(
    persist(
      immer((set, get) => ({
        user: null,
        token: null,
        isAuthenticated: false,
        isLoading: false,
        error: null,
        
        login: async (email: string, password: string) => {
          set((state) => {
            state.isLoading = true;
            state.error = null;
          });
          
          try {
            const response = await fetch('/api/auth/login', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ email, password }),
            });
            
            if (!response.ok) {
              const error = await response.json();
              throw new Error(error.message || 'Login failed');
            }
            
            const { token, user } = await response.json();
            
            set((state) => {
              state.user = user;
              state.token = token;
              state.isAuthenticated = true;
              state.isLoading = false;
            });
            
            // Update Relay environment with new token
            updateAuthToken(token);
          } catch (error) {
            set((state) => {
              state.error = error instanceof Error ? error.message : 'Login failed';
              state.isLoading = false;
            });
            throw error;
          }
        },
        
        logout: () => {
          set((state) => {
            state.user = null;
            state.token = null;
            state.isAuthenticated = false;
            state.error = null;
          });
          
          // Clear auth token and reset Relay store
          updateAuthToken(null);
          resetRelayStore();
          
          // Clear persisted state
          localStorage.removeItem('spice-harvester-auth');
        },
        
        refreshToken: async () => {
          const currentToken = get().token;
          if (!currentToken) return;
          
          try {
            const response = await fetch('/api/auth/refresh', {
              method: 'POST',
              headers: {
                'Authorization': `Bearer ${currentToken}`,
              },
            });
            
            if (!response.ok) {
              throw new Error('Token refresh failed');
            }
            
            const { token } = await response.json();
            
            set((state) => {
              state.token = token;
            });
            
            updateAuthToken(token);
          } catch (error) {
            // If refresh fails, logout
            get().logout();
            throw error;
          }
        },
        
        setUser: (user: IUser) => {
          set((state) => {
            state.user = user;
            state.isAuthenticated = true;
          });
        },
        
        setToken: (token: string) => {
          set((state) => {
            state.token = token;
          });
          updateAuthToken(token);
        },
        
        setError: (error: string | null) => {
          set((state) => {
            state.error = error;
          });
        },
        
        clearError: () => {
          set((state) => {
            state.error = null;
          });
        },
      })),
      {
        name: 'spice-harvester-auth',
        partialize: (state) => ({
          user: state.user,
          token: state.token,
          isAuthenticated: state.isAuthenticated,
        }),
      }
    ),
    {
      name: 'auth-store',
    }
  )
);

// Selectors
export const selectUser = (state: IAuthState) => state.user;
export const selectIsAuthenticated = (state: IAuthState) => state.isAuthenticated;
export const selectAuthError = (state: IAuthState) => state.error;
export const selectAuthLoading = (state: IAuthState) => state.isLoading;

// Initialize auth on app load
export const initializeAuth = async () => {
  const store = useAuthStore.getState();
  
  if (store.token && store.isAuthenticated) {
    try {
      // Verify token is still valid
      await store.refreshToken();
    } catch {
      // Token is invalid, logout
      store.logout();
    }
  }
};