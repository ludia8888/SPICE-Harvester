import { ReactNode, Suspense } from 'react';
import { RelayEnvironmentProvider } from 'react-relay';
import { Spinner } from '@blueprintjs/core';
import { RelayEnvironment } from './environment';

interface IRelayProviderProps {
  children: ReactNode;
}

export function RelayProvider({ children }: IRelayProviderProps) {
  return (
    <RelayEnvironmentProvider environment={RelayEnvironment}>
      <Suspense fallback={<RelayFallback />}>
        {children}
      </Suspense>
    </RelayEnvironmentProvider>
  );
}

function RelayFallback() {
  return (
    <div className="bp5-dialog-container">
      <div className="bp5-dialog">
        <div className="bp5-dialog-body">
          <Spinner size={50} />
          <p style={{ marginTop: '20px', textAlign: 'center' }}>Loading...</p>
        </div>
      </div>
    </div>
  );
}