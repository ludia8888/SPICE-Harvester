import React from 'react';
import { Classes } from '@blueprintjs/core';
import clsx from 'clsx';
import type { NavSectionProps } from './types';

/**
 * Navigation section component for grouping nav items
 */
export const NavSection: React.FC<NavSectionProps> = ({
  children,
  title,
  role = 'group',
  className,
}) => {
  return (
    <div 
      className={clsx('nav-section', className)}
      role={role}
      aria-label={title || (role === 'navigation' ? 'Main navigation' : role === 'complementary' ? 'Additional navigation' : undefined)}
    >
      {title && (
        <div className={clsx('nav-section-title', Classes.MENU_HEADER)}>
          <h6 className={Classes.HEADING}>{title}</h6>
        </div>
      )}
      <div 
        className="nav-section-items"
        role="menu"
        aria-orientation="vertical"
      >
        {children}
      </div>
    </div>
  );
};