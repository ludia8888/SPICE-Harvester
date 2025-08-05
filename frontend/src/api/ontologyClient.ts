import { ApiResponse } from './spiceHarvesterClient';
import { ObjectType, LinkType, Database, Branch } from '../stores/ontology.store';

// Base URLs for different services from environment variables
const BFF_BASE_URL = import.meta.env.VITE_BFF_BASE_URL || 'http://localhost:8002/api/v1';
const FUNNEL_BASE_URL = import.meta.env.VITE_FUNNEL_BASE_URL || 'http://localhost:8004/api/v1';

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
    console.log('🔍 API Request Details:', {
      url,
      method: options.method || 'GET',
      headers: options.headers,
      body: options.body
    });
    
    const response = await fetch(url, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        'Accept-Language': 'ko',
        ...options.headers,
      },
    });
    
    console.log('📡 API Response:', {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers
    });

    // Handle response based on status code
    if (!response.ok) {
      let errorData: any = {};
      try {
        errorData = await response.json();
      } catch {
        // If JSON parsing fails, use status text
        errorData = { message: response.statusText || 'An error occurred' };
      }
      
      throw new OntologyApiError(
        errorData.message || errorData.detail || 'An error occurred',
        errorData.data as string[] || [],
        response.status === 409 ? 'CONFLICT' : response.status === 404 ? 'NOT_FOUND' : 'UNKNOWN'
      );
    }

    const responseData: any = await response.json();
    return responseData.data as T;
  } catch (error) {
    if (error instanceof OntologyApiError) {
      throw error;
    }
    
    // Check for various network errors
    if (error instanceof TypeError && error.message.includes('fetch')) {
      // Network connection failed
      throw new OntologyApiError('Cannot connect to server', [], 'NETWORK_ERROR');
    }
    
    if (error instanceof Error && (
      error.name === 'NetworkError' || 
      error.message.includes('Failed to fetch') ||
      error.message.includes('Network request failed') ||
      error.message.includes('ECONNREFUSED')
    )) {
      throw new OntologyApiError('Network connection failed', [], 'NETWORK_ERROR');
    }
    
    // Other unknown errors
    throw new OntologyApiError('Network error occurred', [], 'NETWORK_ERROR');
  }
}

// Database Operations
export const databaseApi = {
  // List all databases
  async list(): Promise<Database[]> {
    const response = await fetchOntologyApi<{databases: Database[]}>(`${BFF_BASE_URL}/databases`);
    return response.databases;
  },

  // Create new database
  async create(name: string, description: string): Promise<Database> {
    return fetchOntologyApi<Database>(`${BFF_BASE_URL}/databases`, {
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
    return fetchOntologyApi<void>(`${BFF_BASE_URL}/databases/${dbName}`, {
      method: 'DELETE',
    });
  },
};

// Branch Operations (Git-like version control)
export const branchApi = {
  // List all branches
  async list(dbName: string): Promise<{ branches: Branch[]; current: string }> {
    // Use correct BFF database router path
    const response = await fetch(`${BFF_BASE_URL}/databases/${dbName}/branches`, {
      headers: {
        'Content-Type': 'application/json',
        'Accept-Language': 'ko',
      },
    });
    
    if (!response.ok) {
      throw new OntologyApiError(`Failed to fetch branches: ${response.statusText}`, [], response.status.toString());
    }
    
    const data = await response.json();
    
    // Transform backend response to match frontend interface
    const transformedBranches = data.branches.map((branch: any) => ({
      name: branch.name,
      is_current: branch.current || false,
      commit_count: branch.commit_count || 0,
      last_commit: branch.last_commit || null
    }));
    
    // Find current branch name
    const currentBranch = transformedBranches.find((b: any) => b.is_current)?.name || 'main';
    
    return {
      branches: transformedBranches,
      current: currentBranch
    };
  },

  // Create new branch
  async create(dbName: string, branchName: string, fromBranch?: string): Promise<Branch> {
    // Use correct BFF database router path
    const response = await fetch(`${BFF_BASE_URL}/databases/${dbName}/branches`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept-Language': 'ko',
      },
      body: JSON.stringify({ 
        name: branchName, 
        from_branch: fromBranch || 'main' 
      }),
    });
    
    if (!response.ok) {
      const errorData = await response.json().catch(() => ({ detail: 'Unknown error' }));
      throw new OntologyApiError(
        errorData.detail || `Failed to create branch: ${response.statusText}`, 
        [], 
        response.status.toString()
      );
    }
    
    await response.json(); // Consume response
    
    return {
      name: branchName,
      is_current: false,
      commit_count: 0,
      last_commit: undefined
    };
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
    
    // Use direct fetch to handle the actual API response format
    const response = await fetch(`${BFF_BASE_URL}/databases/${dbName}/versions${params}`, {
      headers: {
        'Content-Type': 'application/json',
        'Accept-Language': 'ko',
      },
    });
    
    if (!response.ok) {
      throw new OntologyApiError(`Failed to fetch history: ${response.statusText}`, [], response.status.toString());
    }
    
    const responseData = await response.json();
    
    // Extract versions from BFF response structure
    const versions = responseData.versions || [];
    
    return versions.map((version: any) => ({
      id: version.version_id || version.identifier,
      message: version.message || version.description || 'No message',
      author: version.author || 'Unknown',
      timestamp: version.timestamp ? new Date(version.timestamp * 1000).toISOString() : new Date().toISOString(),
      branch: version.branch || dbName
    }));
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
    // This endpoint returns data directly without BFF wrapper
    const response = await fetch(`${BFF_BASE_URL}/database/${dbName}/ontology/list`, {
      headers: {
        'Content-Type': 'application/json',
        'Accept-Language': 'ko',
      },
    });
    
    if (!response.ok) {
      throw new OntologyApiError(`Failed to fetch ontologies: ${response.statusText}`, [], response.status.toString());
    }
    
    const data: {total: number, ontologies: any[]} = await response.json();
    
    // Transform backend response to frontend format
    return data.ontologies.map(ont => {
      // Extract properties from TerminusDB format
      const properties: any[] = [];
      if (ont.properties && typeof ont.properties === 'object') {
        Object.entries(ont.properties).forEach(([name, propDef]: [string, any]) => {
          let type = 'string';
          let required = false;
          
          if (typeof propDef === 'string') {
            // Simple property: "name": "xsd:string"
            type = propDef.replace('xsd:', '').replace('custom:', '');
            required = true; // Non-optional properties are required
          } else if (propDef && typeof propDef === 'object') {
            // Complex property: {"@class": "xsd:string", "@type": "Optional"}
            type = (propDef['@class'] || 'xsd:string').replace('xsd:', '').replace('custom:', '');
            required = propDef['@type'] !== 'Optional';
          }
          
          properties.push({
            name,
            label: name.charAt(0).toUpperCase() + name.slice(1), // Capitalize first letter
            type,
            required,
            constraints: required ? { primary_key: name === 'id' || name.includes('id') } : {}
          });
        });
      }
      
      return {
        id: ont.id,
        label: ont.label || ont.id,
        description: ont['@documentation']?.['@comment'] || ont.description || '',
        properties,
        metadata: {
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
          version: 1
        }
      };
    });
  },

  // Get specific object type
  async get(dbName: string, classLabel: string): Promise<ObjectType> {
    return fetchOntologyApi<ObjectType>(`${BFF_BASE_URL}/database/${dbName}/ontology/${classLabel}`);
  },

  // Create new object type
  async create(dbName: string, objectType: Omit<ObjectType, 'metadata'>): Promise<ObjectType> {
    // Use direct fetch to handle the actual API response format
    const response = await fetch(`${BFF_BASE_URL}/database/${dbName}/ontology`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept-Language': 'ko',
      },
      body: JSON.stringify({
        id: objectType.id,
        label: objectType.label,
        description: objectType.description,
        properties: objectType.properties,
      }),
    });
    
    if (!response.ok) {
      const errorData = await response.json().catch(() => ({ detail: 'Unknown error' }));
      throw new OntologyApiError(
        errorData.detail || `Failed to create ontology: ${response.statusText}`, 
        [], 
        response.status.toString()
      );
    }
    
    const responseData = await response.json();
    
    // Transform the response to match frontend ObjectType format
    return {
      id: responseData.id,
      label: responseData.label,
      description: responseData.description || '',
      properties: responseData.properties || objectType.properties,
      metadata: {
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
        version: 1
      }
    };
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
    const response = await fetch(`${BFF_BASE_URL}/database/${dbName}/ontology/list`, {
      headers: {
        'Content-Type': 'application/json',
        'Accept-Language': 'ko',
      },
    });
    
    if (!response.ok) {
      throw new OntologyApiError(`Failed to fetch ontologies for links: ${response.statusText}`, [], response.status.toString());
    }
    
    const responseData = await response.json();
    const ontologies = responseData.ontologies || [];
    const linkTypes: LinkType[] = [];
    
    ontologies.forEach(ontology => {
      if (ontology.properties && typeof ontology.properties === 'object') {
        // Properties is an object, not an array - use Object.entries
        Object.entries(ontology.properties).forEach(([propName, propDef]: [string, any]) => {
          let propType = '';
          
          // Extract type from property definition
          if (typeof propDef === 'string') {
            propType = propDef;
          } else if (propDef && propDef['@class']) {
            propType = propDef['@class'];
          }
          
          // Check if this property is a class reference (relationship)
          const isClassReference = propType && 
                                   !propType.startsWith('xsd:') && 
                                   !['string', 'integer', 'boolean', 'email', 'phone', 'money'].includes(propType.replace('custom:', ''));
          
          if (isClassReference) {
            linkTypes.push({
              id: `${ontology.id}_${propName}`,
              label: propName.charAt(0).toUpperCase() + propName.slice(1),
              description: `${ontology.label || ontology.id} ${propName} ${propType}`,
              from_class: ontology.id,
              to_class: propType,
              cardinality: 'many_to_one' as any, // Default for object properties
              properties: []
            });
          }
        });
      }
      
      // Also check explicit relationships array if it exists
      if (ontology.relationships) {
        ontology.relationships.forEach((rel: any) => {
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
  async update(_dbName: string, _linkId: string, _updates: Partial<LinkType>): Promise<LinkType> {
    throw new OntologyApiError('Link updates not implemented - modify the source ontology instead', [], 'NOT_IMPLEMENTED');
  },

  // Delete link type (requires updating the ontology that contains it)
  async delete(_dbName: string, _linkId: string): Promise<void> {
    throw new OntologyApiError('Link deletion not implemented - modify the source ontology instead', [], 'NOT_IMPLEMENTED');
  },
};

// Property validation and type inference using Funnel service
export const typeInferenceApi = {
  // Suggest schema from raw data with optional profiling
  async suggestFromData(data: any[], profile?: {
    totalRows: number;
    columns: Array<{
      name: string;
      nullCount: number;
      nullRatio: number;
      uniqueCount: number;
      uniqueRatio: number;
      sampleValues: any[];
      totalValues: number;
    }>;
  }): Promise<{
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
    // Prepare data in the format expected by the BFF endpoint
    const columns = Object.keys(data[0] || {});
    const rows = data.map(item => columns.map(col => item[col]));
    
    // Call the BFF schema suggestion endpoint
    const response = await fetch(`${BFF_BASE_URL}/database/test_db/suggest-schema-from-data`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept-Language': 'ko',
      },
      body: JSON.stringify({ 
        data: rows,
        columns: columns,
        include_complex_types: true,
        profile: profile // Include profiling data if available
      }),
    });
    
    if (!response.ok) {
      throw new OntologyApiError(`Failed to suggest schema: ${response.statusText}`, [], response.status.toString());
    }
    
    const responseData = await response.json();
    
    // Transform BFF response to expected frontend format
    const suggestedSchema = responseData.suggested_schema;
    
    // Convert single schema object to object_types array format
    const objectType = {
      id: suggestedSchema.id,
      label: typeof suggestedSchema.label === 'object' 
        ? (suggestedSchema.label.ko || suggestedSchema.label.en || suggestedSchema.id)
        : suggestedSchema.label,
      properties: suggestedSchema.properties.map((prop: any) => ({
        name: prop.name,
        type: prop.type,
        confidence: prop.metadata?.inferred_confidence || 1.0,
        constraints: prop.constraints || {}
      }))
    };
    
    return {
      suggested_schema: {
        object_types: [objectType]
      }
    };
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
    // Call the BFF Google Sheets endpoint
    const response = await fetch(`${BFF_BASE_URL}/database/test_db/suggest-schema-from-google-sheets`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept-Language': 'ko',
      },
      body: JSON.stringify({ 
        sheet_url: sheetUrl,
        worksheet_name: range || 'Sheet1'
      }),
    });
    
    if (!response.ok) {
      throw new OntologyApiError(`Failed to analyze Google Sheets: ${response.statusText}`, [], response.status.toString());
    }
    
    const responseData = await response.json();
    
    // Transform BFF response to expected frontend format
    const suggestedSchema = responseData.suggested_schema;
    
    if (!suggestedSchema) {
      throw new OntologyApiError('No schema suggestion received from Google Sheets analysis', []);
    }
    
    // Convert single schema object to object_types array format
    const objectType = {
      id: suggestedSchema.id,
      label: typeof suggestedSchema.label === 'object' 
        ? (suggestedSchema.label.ko || suggestedSchema.label.en || suggestedSchema.id)
        : suggestedSchema.label,
      properties: suggestedSchema.properties.map((prop: any) => ({
        name: prop.name,
        type: prop.type,
        confidence: prop.metadata?.inferred_confidence || 1.0,
        constraints: prop.constraints || {}
      }))
    };
    
    return {
      suggested_schema: {
        object_types: [objectType]
      }
    };
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