/**
 * Types for GlobalSidebar navigation components
 */

export interface NavItemProps {
  /**
   * Blueprint icon name (e.g., "home", "wrench", "chart")
   */
  icon: string;
  
  /**
   * Display label for the navigation item
   */
  label: string;
  
  /**
   * Route path for React Router
   */
  route: string;
  
  /**
   * Whether this item is currently active
   */
  isActive?: boolean;
  
  /**
   * Whether the sidebar is collapsed (icon-only mode)
   */
  isCollapsed?: boolean;
  
  /**
   * Optional click handler
   */
  onClick?: () => void;
  
  /**
   * Optional class name for custom styling
   */
  className?: string;
}

export interface NavSectionProps {
  /**
   * Child navigation items
   */
  children: React.ReactNode;
  
  /**
   * Optional section title
   */
  title?: string;
  
  /**
   * ARIA role for the section (e.g., "navigation", "complementary")
   */
  role?: string;
  
  /**
   * Optional class name
   */
  className?: string;
}

export interface GlobalSidebarProps {
  /**
   * Optional class name
   */
  className?: string;
  
  /**
   * Whether sidebar is collapsed (icon-only mode)
   */
  collapsed?: boolean;
  
  /**
   * Callback when collapse state changes
   */
  onCollapsedChange?: (collapsed: boolean) => void;
}

export type NavigationSection = 'home' | 'editor' | 'analysis' | 'actions' | 'my';