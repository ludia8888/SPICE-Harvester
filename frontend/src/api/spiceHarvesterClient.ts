// Base API client for SPICE HARVESTER system
// Provides standardized response handling and error management

const BFF_BASE_URL = 'http://localhost:8002/api/v1';

// Standardized response type from backend
export interface ApiResponse<T> {
  success: boolean;
  message: string;
  data: T | null;
}

// Custom error for API failures
export class ApiError extends Error {
  constructor(message: string, public details?: string[], public code?: string) {
    super(message);
    this.name = 'ApiError';
  }
}

// Base fetch function with standardized error handling
export async function fetchApi<T>(url: string, options: RequestInit = {}): Promise<T> {
  try {
    const response = await fetch(url, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        'Accept-Language': 'ko', // Default to Korean
        ...options.headers,
      },
    });

    const responseData: ApiResponse<T> = await response.json();

    if (response.status >= 400 || !responseData.success) {
      throw new ApiError(
        responseData.message || 'API request failed',
        responseData.data as string[] || [],
        response.status.toString()
      );
    }

    return responseData.data as T;
  } catch (error) {
    if (error instanceof ApiError) {
      throw error;
    }
    
    // Handle network errors
    if (error instanceof TypeError && error.message.includes('fetch')) {
      throw new ApiError('Network connection failed. Please check if the backend services are running.', [], 'NETWORK_ERROR');
    }
    
    throw new ApiError('Unexpected error occurred', [], 'UNKNOWN_ERROR');
  }
}

// Database Management APIs
export const createDatabase = (name: string, description: string) => {
  return fetchApi<any>(`${BFF_BASE_URL}/database`, {
    method: 'POST',
    body: JSON.stringify({ name, description }),
  });
};

export const getDatabases = () => {
  return fetchApi<any[]>(`${BFF_BASE_URL}/databases`);
};

export const checkDatabaseExists = (dbName: string) => {
  return fetchApi<boolean>(`${BFF_BASE_URL}/database/${dbName}/exists`);
};

export const deleteDatabase = (dbName: string) => {
  return fetchApi<void>(`${BFF_BASE_URL}/database/${dbName}`, {
    method: 'DELETE',
  });
};

// Ontology Management APIs
export const createOntology = (dbName: string, ontologyData: any) => {
  return fetchApi<any>(`${BFF_BASE_URL}/database/${dbName}/ontology`, {
    method: 'POST',
    body: JSON.stringify(ontologyData),
  });
};

export const getOntology = (dbName: string, classLabel: string) => {
  return fetchApi<any>(`${BFF_BASE_URL}/database/${dbName}/ontology/${classLabel}`);
};

export const listOntologies = (dbName: string) => {
  return fetchApi<any[]>(`${BFF_BASE_URL}/database/${dbName}/ontologies`);
};

export const updateOntology = (dbName: string, classLabel: string, updates: any) => {
  return fetchApi<any>(`${BFF_BASE_URL}/database/${dbName}/ontology/${classLabel}`, {
    method: 'PUT',
    body: JSON.stringify(updates),
  });
};

export const deleteOntology = (dbName: string, classLabel: string) => {
  return fetchApi<void>(`${BFF_BASE_URL}/database/${dbName}/ontology/${classLabel}`, {
    method: 'DELETE',
  });
};

// Version Control APIs
export const listBranches = (dbName: string) => {
  return fetchApi<{ branches: any[]; current: string }>(`${BFF_BASE_URL}/database/${dbName}/branches`);
};

export const createBranch = (dbName: string, branchName: string, fromBranch?: string) => {
  return fetchApi<any>(`${BFF_BASE_URL}/database/${dbName}/branch`, {
    method: 'POST',
    body: JSON.stringify({ 
      name: branchName, 
      from_branch: fromBranch || 'main' 
    }),
  });
};

export const checkoutBranch = (dbName: string, branchName: string) => {
  return fetchApi<void>(`${BFF_BASE_URL}/database/${dbName}/checkout`, {
    method: 'POST',
    body: JSON.stringify({ branch: branchName }),
  });
};

export const commitChanges = (dbName: string, message: string, author: string) => {
  return fetchApi<{ commit_id: string }>(`${BFF_BASE_URL}/database/${dbName}/commit`, {
    method: 'POST',
    body: JSON.stringify({ message, author }),
  });
};

export const getHistory = (dbName: string, limit?: number) => {
  const params = limit ? `?limit=${limit}` : '';
  return fetchApi<any[]>(`${BFF_BASE_URL}/database/${dbName}/history${params}`);
};

export const getDiff = (dbName: string, fromRef: string, toRef: string) => {
  return fetchApi<any>(`${BFF_BASE_URL}/database/${dbName}/diff?from_ref=${fromRef}&to_ref=${toRef}`);
};

export const mergeBranches = (dbName: string, sourceBranch: string, targetBranch: string, message: string) => {
  return fetchApi<any>(`${BFF_BASE_URL}/database/${dbName}/merge`, {
    method: 'POST',
    body: JSON.stringify({
      source_branch: sourceBranch,
      target_branch: targetBranch,
      message,
    }),
  });
};

// Health check
export const healthCheck = async () => {
  try {
    const response = await fetch(`${BFF_BASE_URL}/health`);
    return response.ok;
  } catch {
    return false;
  }
};