import { ApiResponse, fetchApi } from './spiceHarvesterClient';
import { ObjectType, LinkType, Property, Database, Branch } from '../stores/ontology.store';

// Base URLs for different services
const BFF_BASE_URL = 'http://localhost:8002/api/v1';
const FUNNEL_BASE_URL = 'http://localhost:8004/api/v1';

// Custom error class for ontology operations
export class OntologyApiError extends Error {
  constructor(message: string, public details?: string[], public code?: string) {
    super(message);
    this.name = 'OntologyApiError';
  }
}

// Enhanced fetch function with ontology-specific error handling
async function fetchOntologyApi<T>(url: string, options: RequestInit = {}): Promise<T> {
  try {
    const response = await fetch(url, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        'Accept-Language': 'ko',
        ...options.headers,
      },
    });

    const responseData: ApiResponse<T> = await response.json();

    if (response.status >= 400 || !responseData.success) {
      throw new OntologyApiError(
        responseData.message || 'An error occurred',
        responseData.data as string[] || [],
        response.status === 409 ? 'CONFLICT' : response.status === 404 ? 'NOT_FOUND' : 'UNKNOWN'
      );
    }

    return responseData.data as T;
  } catch (error) {
    if (error instanceof OntologyApiError) {
      throw error;
    }
    throw new OntologyApiError('Network error occurred', [], 'NETWORK_ERROR');
  }
}

// Database Operations
export const databaseApi = {
  // List all databases
  async list(): Promise<Database[]> {
    return fetchOntologyApi<Database[]>(`${BFF_BASE_URL}/databases`);
  },

  // Create new database
  async create(name: string, description: string): Promise<Database> {
    return fetchOntologyApi<Database>(`${BFF_BASE_URL}/database`, {
      method: 'POST',
      body: JSON.stringify({ name, description }),
    });
  },

  // Check if database exists
  async exists(dbName: string): Promise<boolean> {
    try {
      await fetchOntologyApi<boolean>(`${BFF_BASE_URL}/database/${dbName}/exists`);
      return true;
    } catch (error) {
      if (error instanceof OntologyApiError && error.code === 'NOT_FOUND') {
        return false;
      }
      throw error;
    }
  },

  // Delete database
  async delete(dbName: string): Promise<void> {
    return fetchOntologyApi<void>(`${BFF_BASE_URL}/database/${dbName}`, {
      method: 'DELETE',
    });
  },
};

// Branch Operations (Git-like version control)
export const branchApi = {
  // List all branches
  async list(dbName: string): Promise<{ branches: Branch[]; current: string }> {
    return fetchOntologyApi<{ branches: Branch[]; current: string }>(`${BFF_BASE_URL}/database/${dbName}/branches`);
  },

  // Create new branch
  async create(dbName: string, branchName: string, fromBranch?: string): Promise<Branch> {
    return fetchOntologyApi<Branch>(`${BFF_BASE_URL}/database/${dbName}/branch`, {
      method: 'POST',
      body: JSON.stringify({ 
        name: branchName, 
        from_branch: fromBranch || 'main' 
      }),
    });
  },

  // Switch to branch
  async checkout(dbName: string, branchName: string): Promise<void> {
    return fetchOntologyApi<void>(`${BFF_BASE_URL}/database/${dbName}/checkout`, {
      method: 'POST',
      body: JSON.stringify({ branch: branchName }),
    });
  },

  // Commit changes
  async commit(dbName: string, message: string, author: string): Promise<{ commit_id: string }> {
    return fetchOntologyApi<{ commit_id: string }>(`${BFF_BASE_URL}/database/${dbName}/commit`, {
      method: 'POST',
      body: JSON.stringify({ message, author }),
    });
  },

  // Get commit history
  async history(dbName: string, limit?: number): Promise<Array<{
    id: string;
    message: string;
    author: string;
    timestamp: string;
    branch: string;
  }>> {
    const params = limit ? `?limit=${limit}` : '';
    return fetchOntologyApi<Array<{
      id: string;
      message: string;
      author: string;
      timestamp: string;
      branch: string;
    }>>(`${BFF_BASE_URL}/database/${dbName}/history${params}`);
  },

  // Get diff between refs
  async diff(dbName: string, fromRef: string, toRef: string): Promise<{
    added: any[];
    modified: any[];
    deleted: any[];
  }> {
    return fetchOntologyApi<{
      added: any[];
      modified: any[];
      deleted: any[];
    }>(`${BFF_BASE_URL}/database/${dbName}/diff?from_ref=${fromRef}&to_ref=${toRef}`);
  },

  // Merge branches
  async merge(dbName: string, sourceBranch: string, targetBranch: string, message: string): Promise<{
    merge_commit_id: string;
    conflicts?: any[];
  }> {
    return fetchOntologyApi<{
      merge_commit_id: string;
      conflicts?: any[];
    }>(`${BFF_BASE_URL}/database/${dbName}/merge`, {
      method: 'POST',
      body: JSON.stringify({
        source_branch: sourceBranch,
        target_branch: targetBranch,
        message,
      }),
    });
  },
};

// ObjectType Operations
export const objectTypeApi = {
  // List all object types in database
  async list(dbName: string): Promise<ObjectType[]> {
    return fetchOntologyApi<ObjectType[]>(`${BFF_BASE_URL}/database/${dbName}/ontologies`);
  },

  // Get specific object type
  async get(dbName: string, classLabel: string): Promise<ObjectType> {
    return fetchOntologyApi<ObjectType>(`${BFF_BASE_URL}/database/${dbName}/ontology/${classLabel}`);
  },

  // Create new object type
  async create(dbName: string, objectType: Omit<ObjectType, 'metadata'>): Promise<ObjectType> {
    return fetchOntologyApi<ObjectType>(`${BFF_BASE_URL}/database/${dbName}/ontology`, {
      method: 'POST',
      body: JSON.stringify({
        id: objectType.id,
        label: objectType.label,
        description: objectType.description,
        properties: objectType.properties,
      }),
    });
  },

  // Update object type
  async update(dbName: string, classLabel: string, updates: Partial<ObjectType>): Promise<ObjectType> {
    return fetchOntologyApi<ObjectType>(`${BFF_BASE_URL}/database/${dbName}/ontology/${classLabel}`, {
      method: 'PUT',
      body: JSON.stringify(updates),
    });
  },

  // Delete object type
  async delete(dbName: string, classLabel: string): Promise<void> {
    return fetchOntologyApi<void>(`${BFF_BASE_URL}/database/${dbName}/ontology/${classLabel}`, {
      method: 'DELETE',
    });
  },
};

// Cardinality mapping helper
const mapCardinalityToBackend = (cardinality: string): string => {
  const mapping: Record<string, string> = {
    'one_to_one': '1:1',
    'one_to_many': '1:n', 
    'many_to_one': 'n:1',
    'many_to_many': 'n:n'
  };
  return mapping[cardinality] || '1:n';
};

const mapCardinalityFromBackend = (cardinality: string): string => {
  const mapping: Record<string, string> = {
    '1:1': 'one_to_one',
    '1:n': 'one_to_many',
    'n:1': 'many_to_one', 
    'n:n': 'many_to_many',
    'n:m': 'many_to_many'
  };
  return mapping[cardinality] || 'one_to_many';
};

// LinkType Operations - Links are created as relationships within ontologies
export const linkTypeApi = {
  // Create new link type as a relationship-focused ontology
  async create(dbName: string, linkType: LinkType): Promise<LinkType> {
    // Create ontology with relationship using the advanced endpoint
    const response = await fetchOntologyApi<{
      id: string;
      label: string;
      description?: string;
      relationships: Array<{
        predicate: string;
        target: string;
        label: string;
        cardinality: string;
        description?: string;
        inverse_predicate?: string;
        inverse_label?: string;
      }>;
      metadata: any;
    }>(`${BFF_BASE_URL}/database/${dbName}/ontology-advanced`, {
      method: 'POST',
      body: JSON.stringify({
        id: linkType.from_class,
        label: `${linkType.from_class} (with ${linkType.label} relationship)`,
        description: `${linkType.from_class} class with ${linkType.label} relationship to ${linkType.to_class}`,
        properties: [],
        relationships: [{
          predicate: linkType.id,
          target: linkType.to_class,
          label: linkType.label,
          cardinality: mapCardinalityToBackend(linkType.cardinality),
          description: linkType.description,
          inverse_predicate: `inverse_${linkType.id}`,
          inverse_label: `Inverse ${linkType.label}`
        }]
      }),
    });

    // Convert response back to LinkType format
    const relationship = response.relationships[0];
    return {
      id: relationship.predicate,
      label: relationship.label,
      description: relationship.description,
      from_class: linkType.from_class,
      to_class: relationship.target,
      cardinality: mapCardinalityFromBackend(relationship.cardinality) as any,
      properties: linkType.properties || []
    };
  },

  // List all relationships across ontologies - Extract from properties (Palantir style)
  async list(dbName: string): Promise<LinkType[]> {
    // Get all ontologies and extract their relationships from properties
    const response = await fetchOntologyApi<{ontologies: any[]}>(`${BFF_BASE_URL}/database/${dbName}/ontologies`);
    const ontologies = response.ontologies || response;
    const linkTypes: LinkType[] = [];
    
    ontologies.forEach(ontology => {
      if (ontology.properties) {
        ontology.properties.forEach(prop => {
          // Check if this property is a class reference (relationship)
          const isClassReference = !prop.type.startsWith('xsd:') && 
                                   prop.type !== 'string' && 
                                   prop.type !== 'integer' && 
                                   prop.type !== 'boolean' &&
                                   prop.type !== 'email' &&
                                   prop.type !== 'phone' &&
                                   prop.type !== 'money';
          
          if (isClassReference) {
            linkTypes.push({
              id: `${ontology.id}_${prop.name}`,
              label: prop.display_label || prop.label || prop.name,
              description: `${ontology.label || ontology.id} ${prop.display_label || prop.name} ${prop.type}`,
              from_class: ontology.id,
              to_class: prop.type,
              cardinality: 'many_to_one' as any, // Default for object properties
              properties: []
            });
          }
        });
      }
      
      // Also check explicit relationships array if it exists
      if (ontology.relationships) {
        ontology.relationships.forEach(rel => {
          linkTypes.push({
            id: rel.predicate || rel.name,
            label: rel.label,
            description: rel.description,
            from_class: ontology.id,
            to_class: rel.target || rel.linkTarget,
            cardinality: mapCardinalityFromBackend(rel.cardinality || '1:n') as any,
            properties: []
          });
        });
      }
    });
    
    return linkTypes;
  },

  // Get specific link type (mock implementation)
  async get(dbName: string, linkId: string): Promise<LinkType> {
    const allLinks = await this.list(dbName);
    const link = allLinks.find(l => l.id === linkId);
    if (!link) {
      throw new OntologyApiError(`Link type ${linkId} not found`, [], 'NOT_FOUND');
    }
    return link;
  },

  // Update link type (requires updating the ontology that contains it)
  async update(dbName: string, linkId: string, updates: Partial<LinkType>): Promise<LinkType> {
    throw new OntologyApiError('Link updates not implemented - modify the source ontology instead', [], 'NOT_IMPLEMENTED');
  },

  // Delete link type (requires updating the ontology that contains it)
  async delete(dbName: string, linkId: string): Promise<void> {
    throw new OntologyApiError('Link deletion not implemented - modify the source ontology instead', [], 'NOT_IMPLEMENTED');
  },
};

// Property validation and type inference using Funnel service
export const typeInferenceApi = {
  // Suggest schema from raw data
  async suggestFromData(data: any[]): Promise<{
    suggested_schema: {
      object_types: Array<{
        id: string;
        label: string;
        properties: Array<{
          name: string;
          type: string;
          confidence: number;
          constraints?: Record<string, any>;
        }>;
      }>;
    };
  }> {
    // First analyze the data to get column types
    const columns = Object.keys(data[0] || {});
    const rows = data.map(item => columns.map(col => item[col]));
    
    const analysisResponse = await fetchOntologyApi<{
      columns: Array<{
        column_name: string;
        inferred_type: {
          type: string;
          confidence: number;
          reason?: string;
          metadata?: Record<string, any>;
        };
        sample_values: any[];
        null_count: number;
        unique_count: number;
      }>;
      analysis_metadata: Record<string, any>;
      timestamp: string;
    }>(`${FUNNEL_BASE_URL}/funnel/analyze`, {
      method: 'POST',
      body: JSON.stringify({ 
        data: rows,
        columns: columns,
        include_complex_types: true
      }),
    });

    // Then convert analysis to schema suggestion
    return fetchOntologyApi<{
      suggested_schema: {
        object_types: Array<{
          id: string;
          label: string;
          properties: Array<{
            name: string;
            type: string;
            confidence: number;
            constraints?: Record<string, any>;
          }>;
        }>;
      };
    }>(`${FUNNEL_BASE_URL}/funnel/suggest-schema`, {
      method: 'POST',
      body: JSON.stringify(analysisResponse),
    });
  },

  // Suggest schema from Google Sheets
  async suggestFromGoogleSheets(sheetUrl: string, range?: string): Promise<{
    suggested_schema: {
      object_types: Array<{
        id: string;
        label: string;
        properties: Array<{
          name: string;
          type: string;
          confidence: number;
          constraints?: Record<string, any>;
        }>;
      }>;
    };
  }> {
    return fetchOntologyApi<{
      suggested_schema: {
        object_types: Array<{
          id: string;
          label: string;
          properties: Array<{
            name: string;
            type: string;
            confidence: number;
            constraints?: Record<string, any>;
          }>;
        }>;
      };
    }>(`${FUNNEL_BASE_URL}/funnel/suggest-schema`, {
      method: 'POST',
      body: JSON.stringify({ 
        sheet_url: sheetUrl,
        range: range || 'A1:Z1000'
      }),
    });
  },

  // Validate property type
  async validateProperty(propertyType: string, value: any, constraints?: Record<string, any>): Promise<{
    valid: boolean;
    errors: string[];
    suggestions?: string[];
  }> {
    return fetchOntologyApi<{
      valid: boolean;
      errors: string[];
      suggestions?: string[];
    }>(`${FUNNEL_BASE_URL}/funnel/validate`, {
      method: 'POST',
      body: JSON.stringify({
        type: propertyType,
        value,
        constraints,
      }),
    });
  },
};

// Comprehensive ontology operations combining all services
export const ontologyApi = {
  database: databaseApi,
  branch: branchApi,
  objectType: objectTypeApi,
  linkType: linkTypeApi,
  typeInference: typeInferenceApi,

  // High-level operations
  async initializeWorkspace(dbName: string): Promise<{
    database: Database;
    branches: Branch[];
    objectTypes: ObjectType[];
    linkTypes: LinkType[];
  }> {
    try {
      const [branches, objectTypes, linkTypes] = await Promise.all([
        branchApi.list(dbName),
        objectTypeApi.list(dbName),
        linkTypeApi.list(dbName),
      ]);

      return {
        database: { name: dbName, description: '', created_at: new Date().toISOString() },
        branches: branches.branches,
        objectTypes,
        linkTypes,
      };
    } catch (error) {
      throw new OntologyApiError(
        `Failed to initialize workspace for database: ${dbName}`,
        [],
        'INITIALIZATION_ERROR'
      );
    }
  },

  // Export ontology schema
  async exportSchema(dbName: string): Promise<{
    database: string;
    branch: string;
    object_types: ObjectType[];
    link_types: LinkType[];
    exported_at: string;
  }> {
    const [objectTypes, linkTypes, branches] = await Promise.all([
      objectTypeApi.list(dbName),
      linkTypeApi.list(dbName),
      branchApi.list(dbName),
    ]);

    return {
      database: dbName,
      branch: branches.current,
      object_types: objectTypes,
      link_types: linkTypes,
      exported_at: new Date().toISOString(),
    };
  },

  // Import ontology schema
  async importSchema(dbName: string, schema: {
    object_types: ObjectType[];
    link_types: LinkType[];
  }): Promise<{
    imported_objects: number;
    imported_links: number;
    errors: string[];
  }> {
    const errors: string[] = [];
    let importedObjects = 0;
    let importedLinks = 0;

    // Import object types first
    for (const objectType of schema.object_types) {
      try {
        await objectTypeApi.create(dbName, objectType);
        importedObjects++;
      } catch (error) {
        errors.push(`Failed to import object type ${objectType.id}: ${error instanceof Error ? error.message : 'Unknown error'}`);
      }
    }

    // Import link types after object types
    for (const linkType of schema.link_types) {
      try {
        await linkTypeApi.create(dbName, linkType);
        importedLinks++;
      } catch (error) {
        errors.push(`Failed to import link type ${linkType.id}: ${error instanceof Error ? error.message : 'Unknown error'}`);
      }
    }

    return {
      imported_objects: importedObjects,
      imported_links: importedLinks,
      errors,
    };
  },
};