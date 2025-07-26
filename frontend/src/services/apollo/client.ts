import { ApolloClient, InMemoryCache, createHttpLink, ApolloLink } from '@apollo/client';
import { setContext } from '@apollo/client/link/context';
import { onError } from '@apollo/client/link/error';

// HTTP connection to the API
const httpLink = createHttpLink({
  uri: import.meta.env.VITE_GRAPHQL_ENDPOINT || '/graphql',
});

// Authentication middleware
const authLink = setContext((_, { headers }) => {
  // Get the authentication token from local storage if it exists
  const token = localStorage.getItem('spice-harvester-token');
  
  // Return the headers to the context so httpLink can read them
  return {
    headers: {
      ...headers,
      authorization: token ? `Bearer ${token}` : '',
    },
  };
});

// Error handling
const errorLink = onError(({ graphQLErrors, networkError, operation, forward }) => {
  if (graphQLErrors) {
    graphQLErrors.forEach(({ message, locations, path }) => {
      console.error(
        `[GraphQL error]: Message: ${message}, Location: ${locations}, Path: ${path}`,
      );
    });
  }

  if (networkError) {
    console.error(`[Network error]: ${networkError}`);
    
    // Handle authentication errors
    if ('statusCode' in networkError && networkError.statusCode === 401) {
      // Clear token and redirect to login
      localStorage.removeItem('spice-harvester-token');
      window.location.href = '/login';
    }
  }
});

// Create the Apollo Client
export const apolloClient = new ApolloClient({
  link: ApolloLink.from([errorLink, authLink, httpLink]),
  cache: new InMemoryCache({
    typePolicies: {
      Query: {
        fields: {
          // Define field policies for pagination
          objectTypes: {
            keyArgs: ['filter', 'orderBy'],
            merge(existing = { edges: [] }, incoming) {
              return {
                ...incoming,
                edges: [...existing.edges, ...incoming.edges],
              };
            },
          },
          linkTypes: {
            keyArgs: ['filter', 'orderBy'],
            merge(existing = { edges: [] }, incoming) {
              return {
                ...incoming,
                edges: [...existing.edges, ...incoming.edges],
              };
            },
          },
        },
      },
      ObjectType: {
        keyFields: ['id'],
      },
      LinkType: {
        keyFields: ['id'],
      },
      Property: {
        keyFields: ['id'],
      },
    },
  }),
  defaultOptions: {
    watchQuery: {
      fetchPolicy: 'cache-and-network',
    },
  },
});