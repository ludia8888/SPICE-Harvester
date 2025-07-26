import {
  Environment,
  Network,
  RecordSource,
  Store,
  FetchFunction,
  SubscribeFunction,
  Observable,
} from 'relay-runtime';
import { createClient } from 'graphql-ws';

// WebSocket client for subscriptions
const wsClient = createClient({
  url: import.meta.env.VITE_WEBSOCKET_ENDPOINT || 'ws://localhost:4000/graphql',
  connectionParams: () => {
    const token = localStorage.getItem('spice-harvester-token');
    return {
      authToken: token,
    };
  },
});

// Fetch function for queries and mutations
const fetchFn: FetchFunction = async (request, variables, cacheConfig, uploadables) => {
  const token = localStorage.getItem('spice-harvester-token');
  
  const headers: HeadersInit = {
    'Content-Type': 'application/json',
  };
  
  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }
  
  // Handle file uploads
  if (uploadables) {
    const formData = new FormData();
    formData.append('operations', JSON.stringify({
      query: request.text,
      variables,
    }));
    
    const map: Record<string, string[]> = {};
    Object.keys(uploadables).forEach((key) => {
      map[key] = [`variables.${key}`];
    });
    formData.append('map', JSON.stringify(map));
    
    Object.keys(uploadables).forEach((key) => {
      formData.append(key, uploadables[key] as File);
    });
    
    const response = await fetch(
      import.meta.env.VITE_GRAPHQL_ENDPOINT || '/graphql',
      {
        method: 'POST',
        headers: token ? { Authorization: `Bearer ${token}` } : {},
        body: formData,
      }
    );
    
    return response.json();
  }
  
  // Regular queries and mutations
  const response = await fetch(
    import.meta.env.VITE_GRAPHQL_ENDPOINT || '/graphql',
    {
      method: 'POST',
      headers,
      body: JSON.stringify({
        query: request.text,
        variables,
      }),
    }
  );
  
  const json = await response.json();
  
  // Handle errors
  if (!response.ok) {
    throw new Error(json.message || `HTTP ${response.status}: ${response.statusText}`);
  }
  
  return json;
};

// Subscribe function for subscriptions
const subscribeFn: SubscribeFunction = (request, variables) => {
  return Observable.create((sink) => {
    if (!request.text) {
      sink.error(new Error('Subscription must have query text'));
      return;
    }
    
    const dispose = wsClient.subscribe(
      {
        query: request.text,
        variables,
      },
      sink as any
    );
    
    return () => {
      dispose();
    };
  });
};

// Create network layer
const network = Network.create(fetchFn, subscribeFn);

// Create store
const store = new Store(new RecordSource());

// Create environment
export const RelayEnvironment = new Environment({
  network,
  store,
  // Enable relay dev tools in development
  ...(import.meta.env.DEV && { 
    log: (event) => {
      console.log('[Relay]', event);
    },
  }),
});

// Helper to reset relay store (useful for logout)
export const resetRelayStore = () => {
  RelayEnvironment.getStore().publish(new RecordSource());
  RelayEnvironment.getStore().notify();
};

// Helper to update auth token
export const updateAuthToken = (token: string | null) => {
  if (token) {
    localStorage.setItem('spice-harvester-token', token);
  } else {
    localStorage.removeItem('spice-harvester-token');
  }
  
  // Reset WebSocket connection with new auth
  wsClient.dispose();
  wsClient.lazy = false;
};