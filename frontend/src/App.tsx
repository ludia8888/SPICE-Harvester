import React from 'react';
import { FocusStyleManager } from '@blueprintjs/core';
import { BrowserRouter } from 'react-router-dom';
import clsx from 'clsx';
import { ThemeProvider } from './design-system/theme/ThemeProvider';
import { GlobalSidebar } from './components/layout/GlobalSidebar';
import { useUIStore } from './stores/ui.store';
import './App.scss';

// Enable focus styles only when using keyboard navigation
FocusStyleManager.onlyShowFocusOnTabs();

function App(): JSX.Element {
  const sidebarCollapsed = useUIStore((state) => state.sidebar.isCollapsed);
  
  return (
    <ThemeProvider>
      <BrowserRouter>
        <div className={clsx('app-container', 'bp5-dark', {
          'sidebar-collapsed': sidebarCollapsed,
        })}>
          <GlobalSidebar />
          <main className="app-main">
            <div className="app-content">
              <h1>SPICE HARVESTER</h1>
              <p>Palantir Foundry Style Enterprise Data Platform</p>
            </div>
          </main>
        </div>
      </BrowserRouter>
    </ThemeProvider>
  );
}

export default App;