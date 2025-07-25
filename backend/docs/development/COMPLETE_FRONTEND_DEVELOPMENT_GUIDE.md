# 🔥 SPICE HARVESTER Backend - Complete Frontend Development Guide

This guide provides frontend developers with everything needed to integrate with the SPICE HARVESTER backend. It reflects the latest code implementation, including the standardized `ApiResponse` model and clarified service responsibilities.

## 1. Core Concepts for Frontend

- **BFF is Your Entry Point**: Always interact with the **BFF (Backend for Frontend)** service at `http://localhost:8002`. It is designed to be the single, stable entry point for all client applications.
- **Standardized Responses**: Every API call returns a consistent JSON object: `{ "status": "success" | "error", "message": string, "data": any | null }`. Always check the `status` field first.
- **Label-based Interaction**: Use human-readable labels (e.g., "제품", "Product") when sending data to the BFF. The backend handles the conversion to internal IDs.
- **Language Preference**: Use the `Accept-Language` HTTP header (e.g., `ko`, `en`) to receive localized data.

## 2. System Architecture Overview

The backend consists of three main services:

1.  **BFF (Backend for Frontend)** - Port `8002`: Your primary integration point. It simplifies backend complexity.
2.  **OMS (Ontology Management Service)** - Port `8000`: The core engine for data and version control. You will typically not call this directly.
3.  **Funnel Service** - Port `8003`: Handles AI-powered schema generation from data. Called via the BFF.

## 3. API Integration Examples (React)

### 3.1. API Client with Standardized Error Handling

Here is a robust API client that handles the standardized `ApiResponse` format.

```typescript
// src/api/spiceHarvesterClient.ts

const BFF_BASE_URL = 'http://localhost:8002/api/v1';

// Define a standard response type for your frontend
export interface ApiResponse<T> {
  status: 'success' | 'error';
  message: string;
  data: T | null;
  errors?: string[];
}

// Custom error for API failures
export class ApiError extends Error {
  constructor(message: string, public details?: string[]) {
    super(message);
    this.name = 'ApiError';
  }
}

async function fetchApi<T>(url: string, options: RequestInit = {}): Promise<T> {
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      'Accept-Language': 'ko',
      ...options.headers,
    },
  });

  const responseData: ApiResponse<T> = await response.json();

  if (response.status >= 400 || responseData.status === 'error') {
    throw new ApiError(responseData.message, responseData.errors);
  }

  return responseData.data as T;
}

// --- API Functions ---

export const createDatabase = (name: string, description: string) => {
  return fetchApi<any>(`${BFF_BASE_URL}/database`, {
    method: 'POST',
    body: JSON.stringify({ name, description }),
  });
};

export const createOntology = (dbName: string, ontologyData: any) => {
  return fetchApi<any>(`${BFF_BASE_URL}/database/${dbName}/ontology`, {
    method: 'POST',
    body: JSON.stringify(ontologyData),
  });
};

export const getOntology = (dbName: string, classLabel: string) => {
  return fetchApi<any>(`${BFF_BASE_URL}/database/${dbName}/ontology/${classLabel}`);
};

export const listBranches = (dbName: string) => {
  return fetchApi<{ branches: any[]; current: string }>(`${BFF_BASE_URL}/database/${dbName}/branches`);
};
```

### 3.2. React Component Example

This example shows how to use the API client in a React component with proper state and error handling.

```typescript
// src/components/OntologyManager.tsx
import React, { useState, useEffect } from 'react';
import { createOntology, getOntology, ApiError } from '../api/spiceHarvesterClient';

const OntologyManager = ({ dbName }: { dbName: string }) => {
  const [ontology, setOntology] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  const handleCreate = async () => {
    setIsLoading(true);
    setError(null);
    try {
      const newOntologyData = {
        label: { ko: '새로운 제품', en: 'New Product' },
        description: { ko: '테스트 제품입니다', en: 'This is a test product' },
        properties: [
          { name: 'price', type: 'xsd:decimal', label: { ko: '가격', en: 'Price' } }
        ]
      };
      const result = await createOntology(dbName, newOntologyData);
      alert(`Ontology created with ID: ${result.id}`);
      handleFetch(result.id);
    } catch (err) {
      if (err instanceof ApiError) {
        setError(`Error: ${err.message}`);
      } else {
        setError('An unknown error occurred.');
      }
    }
    setIsLoading(false);
  };

  const handleFetch = async (classId: string) => {
    setIsLoading(true);
    setError(null);
    try {
      const result = await getOntology(dbName, classId);
      setOntology(result);
    } catch (err) {
      if (err instanceof ApiError) {
        setError(`Error: ${err.message}`);
      } else {
        setError('An unknown error occurred.');
      }
    }
    setIsLoading(false);
  };

  return (
    <div>
      <h2>Ontology Manager for '{dbName}'</h2>
      <button onClick={handleCreate} disabled={isLoading}>
        {isLoading ? 'Creating...' : 'Create New Ontology'}
      </button>
      {error && <p style={{ color: 'red' }}>{error}</p>}
      {ontology && (
        <pre>{JSON.stringify(ontology, null, 2)}</pre>
      )}
    </div>
  );
};

export default OntologyManager;
```

## 4. Key API Endpoints (BFF Service)

Always use the BFF service (`http://localhost:8002`) for these operations.

### Database Management
- `POST /api/v1/database`: Create a new database.
- `GET /api/v1/databases`: List all available databases.
- `GET /api/v1/database/{db_name}/exists`: Check if a database exists.

### Ontology Management
- `POST /api/v1/database/{db_name}/ontology`: Create a new ontology class.
- `GET /api/v1/database/{db_name}/ontology/{class_label_or_id}`: Get details of an ontology class.
- `GET /api/v1/database/{db_name}/ontologies`: List all ontology classes in a database.
- `PUT /api/v1/database/{db_name}/ontology/{class_label_or_id}`: Update an ontology class.
- `DELETE /api/v1/database/{db_name}/ontology/{class_label_or_id}`: Delete an ontology class.

### Version Control
- `GET /api/v1/database/{db_name}/branches`: List all branches.
- `POST /api/v1/database/{db_name}/branch`: Create a new branch.
- `POST /api/v1/database/{db_name}/commit`: Commit changes.
- `GET /api/v1/database/{db_name}/history`: Get commit history for a branch.
- `GET /api/v1/database/{db_name}/diff?from_ref=...&to_ref=...`: Get differences between two refs.
- `POST /api/v1/database/{db_name}/merge`: Merge branches.

### Merge Conflict Resolution
- `POST /api/v1/database/{db_name}/merge/simulate`: Simulate a merge to detect conflicts without executing it.
- `POST /api/v1/database/{db_name}/merge/resolve`: Provide resolutions to merge conflicting branches.

### Schema Inference (Funnel)
- `POST /api/v1/funnel/suggest-schema-from-data`: Get a schema suggestion from raw data.
- `POST /api/v1/funnel/suggest-schema-from-google-sheets`: Get a schema suggestion from a Google Sheets URL.

## 5. Unimplemented Features (Frontend Considerations)

Be aware of the following features that are planned but not yet fully implemented in the backend:

- **Role-Based Access Control (RBAC)**: Currently, there are no user roles or permissions enforced. All operations are permitted.
- **Query History**: The `/query/history` endpoint exists but returns dummy data. It does not yet store or retrieve user query history.
- **WebSockets for Real-time Updates**: While planned, WebSocket endpoints for real-time notifications are not yet active.

This guide reflects the current, stable, and tested state of the SPICE HARVESTER backend. By following these patterns, you can ensure a smooth and efficient integration.