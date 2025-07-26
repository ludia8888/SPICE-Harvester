import { create } from 'zustand';
import { devtools, persist } from 'zustand/middleware';
import { immer } from 'zustand/middleware/immer';

interface ISidebarState {
  isOpen: boolean;
  isPinned: boolean;
  width: number;
  isCollapsed: boolean;
}

interface IModalState {
  isOpen: boolean;
  component: React.ComponentType<any> | null;
  props?: any;
}

interface IToast {
  id: string;
  message: string;
  intent: 'none' | 'primary' | 'success' | 'warning' | 'danger';
  icon?: string;
  action?: {
    text: string;
    onClick: () => void;
  };
  timeout?: number;
}

interface IBreadcrumb {
  text: string;
  href?: string;
  icon?: string;
}

interface IUIState {
  // Theme
  theme: 'light' | 'dark';
  
  // Sidebar
  sidebar: ISidebarState;
  
  // Modals
  modals: IModalState[];
  
  // Toasts
  toasts: IToast[];
  
  // Breadcrumbs
  breadcrumbs: IBreadcrumb[];
  
  // Loading states
  globalLoading: boolean;
  loadingMessage?: string;
  
  // Actions
  setTheme: (theme: 'light' | 'dark') => void;
  toggleTheme: () => void;
  
  setSidebarOpen: (isOpen: boolean) => void;
  toggleSidebar: () => void;
  setSidebarPinned: (isPinned: boolean) => void;
  setSidebarWidth: (width: number) => void;
  setSidebarCollapsed: (isCollapsed: boolean) => void;
  
  openModal: (component: React.ComponentType<any>, props?: any) => void;
  closeModal: (index?: number) => void;
  closeAllModals: () => void;
  
  showToast: (toast: Omit<IToast, 'id'>) => void;
  removeToast: (id: string) => void;
  
  setBreadcrumbs: (breadcrumbs: IBreadcrumb[]) => void;
  
  setGlobalLoading: (loading: boolean, message?: string) => void;
}

let toastId = 0;

export const useUIStore = create<IUIState>()(
  devtools(
    persist(
      immer((set) => ({
        // Initial state
        theme: 'light',
        
        sidebar: {
          isOpen: true,
          isPinned: true,
          width: 240,
          isCollapsed: false,
        },
        
        modals: [],
        toasts: [],
        breadcrumbs: [],
        globalLoading: false,
        loadingMessage: undefined,
        
        // Actions
        setTheme: (theme) => {
          set((state) => {
            state.theme = theme;
          });
          
          // Apply theme to document
          if (theme === 'dark') {
            document.documentElement.classList.add('bp5-dark');
          } else {
            document.documentElement.classList.remove('bp5-dark');
          }
        },
        
        toggleTheme: () => {
          set((state) => {
            const newTheme = state.theme === 'light' ? 'dark' : 'light';
            state.theme = newTheme;
            
            // Apply theme to document
            if (newTheme === 'dark') {
              document.documentElement.classList.add('bp5-dark');
            } else {
              document.documentElement.classList.remove('bp5-dark');
            }
          });
        },
        
        setSidebarOpen: (isOpen) => {
          set((state) => {
            state.sidebar.isOpen = isOpen;
          });
        },
        
        toggleSidebar: () => {
          set((state) => {
            state.sidebar.isOpen = !state.sidebar.isOpen;
          });
        },
        
        setSidebarPinned: (isPinned) => {
          set((state) => {
            state.sidebar.isPinned = isPinned;
          });
        },
        
        setSidebarWidth: (width) => {
          set((state) => {
            state.sidebar.width = width;
          });
        },
        
        setSidebarCollapsed: (isCollapsed) => {
          set((state) => {
            state.sidebar.isCollapsed = isCollapsed;
          });
        },
        
        openModal: (component, props) => {
          set((state) => {
            state.modals.push({
              isOpen: true,
              component,
              props,
            });
          });
        },
        
        closeModal: (index) => {
          set((state) => {
            if (index !== undefined) {
              state.modals.splice(index, 1);
            } else {
              state.modals.pop();
            }
          });
        },
        
        closeAllModals: () => {
          set((state) => {
            state.modals = [];
          });
        },
        
        showToast: (toast) => {
          const id = `toast-${++toastId}`;
          const timeout = toast.timeout ?? 5000;
          
          set((state) => {
            state.toasts.push({
              ...toast,
              id,
            });
          });
          
          // Auto remove toast after timeout
          if (timeout > 0) {
            setTimeout(() => {
              useUIStore.getState().removeToast(id);
            }, timeout);
          }
        },
        
        removeToast: (id) => {
          set((state) => {
            const index = state.toasts.findIndex((t) => t.id === id);
            if (index !== -1) {
              state.toasts.splice(index, 1);
            }
          });
        },
        
        setBreadcrumbs: (breadcrumbs) => {
          set((state) => {
            state.breadcrumbs = breadcrumbs;
          });
        },
        
        setGlobalLoading: (loading, message) => {
          set((state) => {
            state.globalLoading = loading;
            state.loadingMessage = message;
          });
        },
      })),
      {
        name: 'spice-harvester-ui',
        partialize: (state) => ({
          theme: state.theme,
          sidebar: state.sidebar,
        }),
      }
    ),
    {
      name: 'ui-store',
    }
  )
);

// Selectors
export const selectTheme = (state: IUIState) => state.theme;
export const selectSidebar = (state: IUIState) => state.sidebar;
export const selectSidebarCollapsed = (state: IUIState) => state.sidebar.isCollapsed;
export const selectModals = (state: IUIState) => state.modals;
export const selectToasts = (state: IUIState) => state.toasts;
export const selectBreadcrumbs = (state: IUIState) => state.breadcrumbs;
export const selectGlobalLoading = (state: IUIState) => state.globalLoading;

// Utility functions
export const showSuccessToast = (message: string, action?: IToast['action']) => {
  useUIStore.getState().showToast({
    message,
    intent: 'success',
    icon: 'tick-circle',
    action,
  });
};

export const showErrorToast = (message: string, action?: IToast['action']) => {
  useUIStore.getState().showToast({
    message,
    intent: 'danger',
    icon: 'error',
    action,
  });
};

export const showWarningToast = (message: string, action?: IToast['action']) => {
  useUIStore.getState().showToast({
    message,
    intent: 'warning',
    icon: 'warning-sign',
    action,
  });
};

export const showInfoToast = (message: string, action?: IToast['action']) => {
  useUIStore.getState().showToast({
    message,
    intent: 'primary',
    icon: 'info-sign',
    action,
  });
};