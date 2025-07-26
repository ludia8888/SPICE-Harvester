/**
 * Test utilities for React components
 * Provides custom render methods with all necessary providers
 */

import React, { ReactElement } from 'react';
import { render, RenderOptions } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { RelayEnvironmentProvider } from 'react-relay';
import { Environment, Network, RecordSource, Store } from 'relay-runtime';
import { ThemeProvider } from '../design-system/theme/ThemeProvider';
import { vi } from 'vitest';

// Re-export everything from testing library
export * from '@testing-library/react';
export { default as userEvent } from '@testing-library/user-event';

/**
 * Create a mock Relay environment for testing
 */
export function createMockRelayEnvironment() {
  const source = new RecordSource();
  const store = new Store(source);
  
  const fetchQuery = vi.fn(() =>
    Promise.resolve({
      data: {},
    })
  );
  
  const network = Network.create(fetchQuery);
  
  return new Environment({
    network,
    store,
  });
}

interface AllTheProvidersProps {
  children: React.ReactNode;
  relayEnvironment?: Environment;
}

/**
 * Wraps components with all necessary providers for testing
 */
const AllTheProviders: React.FC<AllTheProvidersProps> = ({ 
  children,
  relayEnvironment = createMockRelayEnvironment(),
}) => {
  return (
    <RelayEnvironmentProvider environment={relayEnvironment}>
      <BrowserRouter>
        <ThemeProvider>
          {children}
        </ThemeProvider>
      </BrowserRouter>
    </RelayEnvironmentProvider>
  );
};

/**
 * Custom render method that includes all providers
 */
export function renderWithProviders(
  ui: ReactElement,
  options?: Omit<RenderOptions, 'wrapper'> & {
    relayEnvironment?: Environment;
  }
) {
  const { relayEnvironment, ...renderOptions } = options || {};
  
  return render(ui, {
    wrapper: ({ children }) => (
      <AllTheProviders relayEnvironment={relayEnvironment}>
        {children}
      </AllTheProviders>
    ),
    ...renderOptions,
  });
}

/**
 * Mock data generators for common entities
 */
export const mockData = {
  user: (overrides = {}) => ({
    id: '1',
    name: 'Test User',
    email: 'test@example.com',
    role: 'admin',
    ...overrides,
  }),
  
  objectType: (overrides = {}) => ({
    id: 'obj-1',
    name: 'Test Object',
    displayName: 'Test Object',
    pluralName: 'Test Objects',
    description: 'A test object type',
    icon: 'cube',
    color: '#0084FF',
    ...overrides,
  }),
  
  property: (overrides = {}) => ({
    id: 'prop-1',
    name: 'testProperty',
    displayName: 'Test Property',
    type: 'string',
    isRequired: false,
    isPrimaryKey: false,
    isTitleKey: false,
    ...overrides,
  }),
  
  linkType: (overrides = {}) => ({
    id: 'link-1',
    name: 'testLink',
    displayName: 'Test Link',
    sourceObjectType: 'obj-1',
    targetObjectType: 'obj-2',
    cardinality: 'one-to-many',
    ...overrides,
  }),
};

/**
 * Utility to wait for async operations
 */
export async function waitForElementToBeRemoved(
  callback: () => HTMLElement | HTMLElement[] | null,
  options?: Parameters<typeof import('@testing-library/react').waitForElementToBeRemoved>[1]
) {
  const { waitForElementToBeRemoved: originalWaitFor } = await import('@testing-library/react');
  return originalWaitFor(callback, options);
}