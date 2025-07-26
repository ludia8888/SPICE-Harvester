import React from 'react';
import { Classes, Icon } from '@blueprintjs/core';
import clsx from 'clsx';
import { useLocation } from 'react-router-dom';
import { useUIStore } from '../../../stores/ui.store';
import { NavItem } from './NavItem';
import { NavSection } from './NavSection';
import type { GlobalSidebarProps, NavigationSection } from './types';
import './GlobalSidebar.scss';

/**
 * Global sidebar navigation component
 * Following Palantir Foundry's navigation patterns with Blueprint.js
 */
export const GlobalSidebar: React.FC<GlobalSidebarProps> = ({
  className,
  collapsed = false,
  onCollapsedChange,
}) => {
  const location = useLocation();
  const sidebarCollapsed = useUIStore((state) => state.sidebar.isCollapsed);
  const setSidebarCollapsed = useUIStore((state) => state.setSidebarCollapsed);
  
  // Use prop value or store value
  const isCollapsed = collapsed ?? sidebarCollapsed;
  
  // Determine active section based on current route
  const getActiveSection = (): NavigationSection => {
    const path = location.pathname;
    if (path.startsWith('/editor')) return 'editor';
    if (path.startsWith('/analysis')) return 'analysis';
    if (path.startsWith('/actions')) return 'actions';
    if (path.startsWith('/my')) return 'my';
    return 'home';
  };
  
  const activeSection = getActiveSection();
  
  const handleCollapse = () => {
    const newState = !isCollapsed;
    setSidebarCollapsed(newState);
    onCollapsedChange?.(newState);
  };
  
  return (
    <nav
      className={clsx(
        'global-sidebar',
        Classes.DARK,
        {
          'global-sidebar--collapsed': isCollapsed,
        },
        className
      )}
      role="navigation"
      aria-label="Main navigation"
    >
      <div className="global-sidebar__content">
        {/* Main navigation section */}
        <NavSection 
          className="global-sidebar__main"
        >
          <NavItem
            icon="home"
            label="Home"
            route="/home"
            isActive={activeSection === 'home'}
            isCollapsed={isCollapsed}
          />
          <NavItem
            icon="wrench"
            label="Ontology Editor"
            route="/editor"
            isActive={activeSection === 'editor'}
            isCollapsed={isCollapsed}
          />
          <NavItem
            icon="chart"
            label="Analysis"
            route="/analysis"
            isActive={activeSection === 'analysis'}
            isCollapsed={isCollapsed}
          />
          <NavItem
            icon="flash"
            label="Action Center"
            route="/actions"
            isActive={activeSection === 'actions'}
            isCollapsed={isCollapsed}
          />
        </NavSection>
        
        {/* Bottom section - fixed to bottom */}
        <NavSection 
          className="global-sidebar__bottom"
          role="complementary"
        >
          <NavItem
            icon="user"
            label="My Page"
            route="/my"
            isActive={activeSection === 'my'}
            isCollapsed={isCollapsed}
          />
        </NavSection>
      </div>
      
      {/* Optional collapse toggle button */}
      {onCollapsedChange && (
        <button
          className={clsx(
            'global-sidebar__collapse-toggle',
            Classes.BUTTON,
            Classes.MINIMAL
          )}
          onClick={handleCollapse}
          aria-label={isCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
          title={isCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        >
          <Icon 
            icon={isCollapsed ? 'chevron-right' : 'chevron-left'} 
            size={16}
          />
        </button>
      )}
    </nav>
  );
};