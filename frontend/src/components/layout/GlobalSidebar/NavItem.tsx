import React from 'react';
import { NavLink } from 'react-router-dom';
import { Icon, Classes, Tooltip, Position, type IconName } from '@blueprintjs/core';
import clsx from 'clsx';
import type { NavItemProps } from './types';

/**
 * Navigation item component for GlobalSidebar
 * Uses Blueprint.js Icon component with minimal, modern styling
 */
export const NavItem: React.FC<NavItemProps> = ({
  icon,
  label,
  route,
  isActive,
  isCollapsed = false,
  onClick,
  className,
}) => {
  const renderNavLink = (additionalProps?: any) => {
    // Ensure our aria-expanded is not overridden
    const { 'aria-expanded': _, ...propsWithoutAriaExpanded } = additionalProps || {};
    
    return (
      <NavLink
        to={route}
        className={({ isActive: routeActive }) =>
          clsx(
            'nav-item',
            Classes.MENU_ITEM,
            {
              [Classes.ACTIVE]: isActive || routeActive,
              [Classes.INTENT_PRIMARY]: isActive || routeActive,
            },
            className
          )
        }
        onClick={onClick}
        role="menuitem"
        aria-current={isActive ? 'page' : undefined}
        aria-label={label}
        {...propsWithoutAriaExpanded}
        aria-expanded={isCollapsed ? 'false' : 'true'}
      >
        <Icon 
          icon={icon as IconName} 
          size={20}
          className="nav-item-icon"
          aria-hidden="true"
        />
        <span 
          className="nav-item-label"
          aria-hidden={isCollapsed}
        >
          {label}
        </span>
      </NavLink>
    );
  };

  // Use renderTarget when collapsed to preserve aria-expanded
  if (isCollapsed) {
    return (
      <Tooltip
        content={label}
        position={Position.RIGHT}
        className="nav-item-tooltip"
        disabled={!isCollapsed}
        hoverOpenDelay={200}
        minimal
        renderTarget={(tooltipProps) => renderNavLink(tooltipProps)}
      />
    );
  }

  return renderNavLink();
};