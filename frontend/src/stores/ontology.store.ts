import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';
import { subscribeWithSelector } from 'zustand/middleware';

// Types for ontology entities based on backend API
export interface Property {
  name: string;
  label: string;
  type: string;
  required: boolean;
  constraints?: Record<string, any>;
  description?: string;
}

export interface ObjectType {
  id: string;
  label: string;
  description?: string;
  properties: Property[];
  metadata?: {
    created_at: string;
    updated_at: string;
    version: number;
  };
}

export interface LinkType {
  id: string;
  label: string;
  description?: string;
  from_class: string;
  to_class: string;
  cardinality: 'one_to_one' | 'one_to_many' | 'many_to_one' | 'many_to_many';
  properties?: Property[];
}

export interface Database {
  name: string;
  description: string;
  created_at: string;
}

export interface Branch {
  name: string;
  is_current: boolean;
  commit_count: number;
  last_commit?: {
    id: string;
    message: string;
    timestamp: string;
    author: string;
  };
}

export interface OntologyState {
  // Current context
  currentDatabase: string | null;
  currentBranch: string;
  
  // Data
  databases: Database[];
  branches: Branch[];
  objectTypes: ObjectType[];
  linkTypes: LinkType[];
  
  // UI State
  selectedObjectType: string | null;
  selectedLinkType: string | null;
  selectedProperty: string | null;
  isEditing: boolean;
  isSaving: boolean;
  isLoading: boolean;
  
  // Panel states
  showPropertiesPanel: boolean;
  showRelationshipsPanel: boolean;
  showHistoryPanel: boolean;
  showBranchPanel: boolean;
  
  // Search and filters
  searchQuery: string;
  filterByType: 'all' | 'objects' | 'links';
  
  // Error handling
  errors: Record<string, string>;
  
  // Actions
  setCurrentDatabase: (dbName: string) => void;
  setCurrentBranch: (branch: string) => void;
  
  // Database operations
  setDatabases: (databases: Database[]) => void;
  addDatabase: (database: Database) => void;
  
  // Branch operations
  setBranches: (branches: Branch[]) => void;
  createBranch: (name: string) => void;
  
  // ObjectType operations
  setObjectTypes: (objectTypes: ObjectType[]) => void;
  addObjectType: (objectType: ObjectType) => void;
  updateObjectType: (id: string, updates: Partial<ObjectType>) => void;
  deleteObjectType: (id: string) => void;
  
  // LinkType operations
  setLinkTypes: (linkTypes: LinkType[]) => void;
  addLinkType: (linkType: LinkType) => void;
  updateLinkType: (id: string, updates: Partial<LinkType>) => void;
  deleteLinkType: (id: string) => void;
  
  // Property operations
  addProperty: (objectTypeId: string, property: Property) => void;
  updateProperty: (objectTypeId: string, propertyName: string, updates: Partial<Property>) => void;
  deleteProperty: (objectTypeId: string, propertyName: string) => void;
  
  // Selection
  selectObjectType: (id: string | null) => void;
  selectLinkType: (id: string | null) => void;
  selectProperty: (name: string | null) => void;
  
  // UI State
  setEditing: (editing: boolean) => void;
  setSaving: (saving: boolean) => void;
  setLoading: (loading: boolean) => void;
  
  // Panel toggles
  togglePropertiesPanel: () => void;
  toggleRelationshipsPanel: () => void;
  toggleHistoryPanel: () => void;
  toggleBranchPanel: () => void;
  
  // Search and filters
  setSearchQuery: (query: string) => void;
  setFilterByType: (filter: 'all' | 'objects' | 'links') => void;
  
  // Error handling
  setError: (key: string, error: string) => void;
  clearError: (key: string) => void;
  clearAllErrors: () => void;
  
  // Computed getters
  getFilteredObjectTypes: () => ObjectType[];
  getFilteredLinkTypes: () => LinkType[];
  getCurrentObjectType: () => ObjectType | null;
  getCurrentLinkType: () => LinkType | null;
}

export const useOntologyStore = create<OntologyState>()(
  subscribeWithSelector(
    immer((set, get) => ({
      // Initial state
      currentDatabase: null,
      currentBranch: 'main',
      databases: [],
      branches: [],
      objectTypes: [],
      linkTypes: [],
      selectedObjectType: null,
      selectedLinkType: null,
      selectedProperty: null,
      isEditing: false,
      isSaving: false,
      isLoading: false,
      showPropertiesPanel: true,
      showRelationshipsPanel: true,
      showHistoryPanel: false,
      showBranchPanel: false,
      searchQuery: '',
      filterByType: 'all',
      errors: {},

      // Context actions
      setCurrentDatabase: (dbName) => set((state) => {
        state.currentDatabase = dbName;
        // Reset selections when changing database
        state.selectedObjectType = null;
        state.selectedLinkType = null;
        state.selectedProperty = null;
      }),

      setCurrentBranch: (branch) => set((state) => {
        state.currentBranch = branch;
      }),

      // Database operations
      setDatabases: (databases) => set((state) => {
        state.databases = databases;
      }),

      addDatabase: (database) => set((state) => {
        state.databases.push(database);
      }),

      // Branch operations
      setBranches: (branches) => set((state) => {
        state.branches = branches;
      }),

      createBranch: (name) => set((state) => {
        const newBranch: Branch = {
          name,
          is_current: false,
          commit_count: 0,
        };
        state.branches.push(newBranch);
      }),

      // ObjectType operations
      setObjectTypes: (objectTypes) => set((state) => {
        state.objectTypes = objectTypes;
      }),

      addObjectType: (objectType) => set((state) => {
        state.objectTypes.push(objectType);
      }),

      updateObjectType: (id, updates) => set((state) => {
        const index = state.objectTypes.findIndex(obj => obj.id === id);
        if (index !== -1) {
          Object.assign(state.objectTypes[index], updates);
        }
      }),

      deleteObjectType: (id) => set((state) => {
        state.objectTypes = state.objectTypes.filter(obj => obj.id !== id);
        if (state.selectedObjectType === id) {
          state.selectedObjectType = null;
        }
      }),

      // LinkType operations
      setLinkTypes: (linkTypes) => set((state) => {
        state.linkTypes = linkTypes;
      }),

      addLinkType: (linkType) => set((state) => {
        state.linkTypes.push(linkType);
      }),

      updateLinkType: (id, updates) => set((state) => {
        const index = state.linkTypes.findIndex(link => link.id === id);
        if (index !== -1) {
          Object.assign(state.linkTypes[index], updates);
        }
      }),

      deleteLinkType: (id) => set((state) => {
        state.linkTypes = state.linkTypes.filter(link => link.id !== id);
        if (state.selectedLinkType === id) {
          state.selectedLinkType = null;
        }
      }),

      // Property operations
      addProperty: (objectTypeId, property) => set((state) => {
        const objectType = state.objectTypes.find(obj => obj.id === objectTypeId);
        if (objectType) {
          objectType.properties.push(property);
        }
      }),

      updateProperty: (objectTypeId, propertyName, updates) => set((state) => {
        const objectType = state.objectTypes.find(obj => obj.id === objectTypeId);
        if (objectType) {
          const propertyIndex = objectType.properties.findIndex(prop => prop.name === propertyName);
          if (propertyIndex !== -1) {
            Object.assign(objectType.properties[propertyIndex], updates);
          }
        }
      }),

      deleteProperty: (objectTypeId, propertyName) => set((state) => {
        const objectType = state.objectTypes.find(obj => obj.id === objectTypeId);
        if (objectType) {
          objectType.properties = objectType.properties.filter(prop => prop.name !== propertyName);
          if (state.selectedProperty === propertyName) {
            state.selectedProperty = null;
          }
        }
      }),

      // Selection actions
      selectObjectType: (id) => set((state) => {
        state.selectedObjectType = id;
        state.selectedLinkType = null; // Clear other selections
        state.selectedProperty = null;
      }),

      selectLinkType: (id) => set((state) => {
        state.selectedLinkType = id;
        state.selectedObjectType = null; // Clear other selections
        state.selectedProperty = null;
      }),

      selectProperty: (name) => set((state) => {
        state.selectedProperty = name;
      }),

      // UI State actions
      setEditing: (editing) => set((state) => {
        state.isEditing = editing;
      }),

      setSaving: (saving) => set((state) => {
        state.isSaving = saving;
      }),

      setLoading: (loading) => set((state) => {
        state.isLoading = loading;
      }),

      // Panel toggles
      togglePropertiesPanel: () => set((state) => {
        state.showPropertiesPanel = !state.showPropertiesPanel;
      }),

      toggleRelationshipsPanel: () => set((state) => {
        state.showRelationshipsPanel = !state.showRelationshipsPanel;
      }),

      toggleHistoryPanel: () => set((state) => {
        state.showHistoryPanel = !state.showHistoryPanel;
      }),

      toggleBranchPanel: () => set((state) => {
        state.showBranchPanel = !state.showBranchPanel;
      }),

      // Search and filters
      setSearchQuery: (query) => set((state) => {
        state.searchQuery = query;
      }),

      setFilterByType: (filter) => set((state) => {
        state.filterByType = filter;
      }),

      // Error handling
      setError: (key, error) => set((state) => {
        state.errors[key] = error;
      }),

      clearError: (key) => set((state) => {
        delete state.errors[key];
      }),

      clearAllErrors: () => set((state) => {
        state.errors = {};
      }),

      // Computed getters
      getFilteredObjectTypes: () => {
        const state = get();
        let filtered = state.objectTypes;
        
        if (state.searchQuery) {
          const query = state.searchQuery.toLowerCase();
          filtered = filtered.filter(obj => 
            obj.label.toLowerCase().includes(query) ||
            obj.id.toLowerCase().includes(query) ||
            obj.description?.toLowerCase().includes(query)
          );
        }
        
        return filtered;
      },

      getFilteredLinkTypes: () => {
        const state = get();
        let filtered = state.linkTypes;
        
        if (state.searchQuery) {
          const query = state.searchQuery.toLowerCase();
          filtered = filtered.filter(link => 
            link.label.toLowerCase().includes(query) ||
            link.id.toLowerCase().includes(query) ||
            link.description?.toLowerCase().includes(query)
          );
        }
        
        return filtered;
      },

      getCurrentObjectType: () => {
        const state = get();
        return state.selectedObjectType 
          ? state.objectTypes.find(obj => obj.id === state.selectedObjectType) || null
          : null;
      },

      getCurrentLinkType: () => {
        const state = get();
        return state.selectedLinkType 
          ? state.linkTypes.find(link => link.id === state.selectedLinkType) || null
          : null;
      },
    }))
  )
);

// Selectors for commonly used derived state
export const useCurrentDatabase = () => useOntologyStore(state => state.currentDatabase);
export const useCurrentBranch = () => useOntologyStore(state => state.currentBranch);
export const useSelectedObjectType = () => useOntologyStore(state => state.getCurrentObjectType());
export const useSelectedLinkType = () => useOntologyStore(state => state.getCurrentLinkType());
export const useFilteredObjectTypes = () => useOntologyStore(state => state.getFilteredObjectTypes());
export const useFilteredLinkTypes = () => useOntologyStore(state => state.getFilteredLinkTypes());