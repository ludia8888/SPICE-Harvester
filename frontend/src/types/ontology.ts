// Ontology TypeScript Types
// Based on backend models

export interface Property {
  name: string;
  type: string;
  label: string;
  required: boolean;
  default?: any;
  description?: string;
  constraints?: Record<string, any>;
  target?: string;
  linkTarget?: string;
  isRelationship?: boolean;
  cardinality?: string;
  items?: Record<string, any>;
}

export interface Relationship {
  predicate: string;
  target: string;
  label: string;
  cardinality: string;
  description?: string;
  inverse_predicate?: string;
  inverse_label?: string;
}

export interface OntologyClass {
  id: string;
  label: string;
  description?: string;
  created_at?: string;
  updated_at?: string;
  parent_class?: string;
  abstract: boolean;
  properties: Property[];
  relationships: Relationship[];
  metadata?: Record<string, any>;
}

export interface OntologyCreateRequest {
  id?: string;
  label: string;
  description?: string;
  parent_class?: string;
  abstract?: boolean;
  properties?: Property[];
  relationships?: Relationship[];
  metadata?: Record<string, any>;
}

export interface OntologyUpdateRequest {
  label?: string;
  description?: string;
  parent_class?: string;
  abstract?: boolean;
  properties?: Property[];
  relationships?: Relationship[];
  metadata?: Record<string, any>;
}

export type Cardinality = '1:1' | '1:n' | 'n:1' | 'n:m';

export interface QueryOperator {
  name: string;
  symbol: string;
  description: string;
  applies_to: string[];
}

// UI specific types
export interface OntologyEditorState {
  classes: OntologyClass[];
  selectedClass?: OntologyClass;
  selectedProperty?: Property;
  selectedRelationship?: Relationship;
  editMode: 'view' | 'edit' | 'create';
  sidebarOpen: boolean;
  searchQuery: string;
  filters: {
    showAbstract: boolean;
    showProperties: boolean;
    showRelationships: boolean;
  };
}

export interface TabPanelProps {
  children?: React.ReactNode;
  index: number;
  value: number;
}