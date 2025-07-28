import React from 'react';
import { FocusStyleManager } from '@blueprintjs/core';
import { BrowserRouter } from 'react-router-dom';
import clsx from 'clsx';
import { ThemeProvider } from './design-system/theme/ThemeProvider';
import { GlobalSidebar } from './components/layout/GlobalSidebar';
import { AppRouter } from './router';
import { useUIStore } from './stores/ui.store';
import './App.scss';

// Enable focus styles only when using keyboard navigation
FocusStyleManager.onlyShowFocusOnTabs();

function App(): JSX.Element {
  const sidebarCollapsed = useUIStore((state) => state.sidebar.isCollapsed);
  
  return (
    <ThemeProvider>
      <BrowserRouter
        future={{
          v7_startTransition: true,
          v7_relativeSplatPath: true,
        }}
      >
        <div className={clsx('app-container', 'bp5-dark', {
          'sidebar-collapsed': sidebarCollapsed,
        })}>
          <GlobalSidebar />
          <main className="app-main">
            <AppRouter />
          </main>
        </div>
      </BrowserRouter>
    </ThemeProvider>
  );
}

export default App;