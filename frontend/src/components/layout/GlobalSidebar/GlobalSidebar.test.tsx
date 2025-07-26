import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { GlobalSidebar } from './GlobalSidebar';
import { renderWithProviders } from '../../../utils/test-utils';
import { useUIStore } from '../../../stores/ui.store';

// Mock location for testing
const mockUseLocation = vi.fn(() => ({ pathname: '/home' }));

// Mock react-router-dom
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useLocation: () => mockUseLocation(),
    NavLink: (props: any) => {
      const { children, to, className, onClick, role, 'aria-current': ariaCurrent, 'aria-label': ariaLabel, 'aria-expanded': ariaExpanded, ...rest } = props;
      const currentPath = mockUseLocation().pathname;
      const isActive = currentPath.startsWith(to);
      const classNameResult = typeof className === 'function' 
        ? className({ isActive })
        : className;
      
      // Return a simple anchor with all aria props explicitly set
      return (
        <a 
          href={to}
          className={classNameResult}
          onClick={(e) => {
            e.preventDefault();
            onClick?.(e);
          }}
          role={role}
          aria-current={ariaCurrent}
          aria-label={ariaLabel}
          aria-expanded={ariaExpanded}
          {...rest}
        >
          {children}
        </a>
      );
    },
  };
});

describe('GlobalSidebar', () => {
  beforeEach(() => {
    // Reset mocks
    mockUseLocation.mockReset();
    mockUseLocation.mockReturnValue({ pathname: '/home' });
    
    // Reset store state before each test
    useUIStore.setState({
      sidebar: {
        isOpen: true,
        isPinned: true,
        width: 240,
        isCollapsed: false,
      },
    });
  });
  
  it('renders all navigation items', () => {
    renderWithProviders(<GlobalSidebar />);
    
    // Check main navigation items
    expect(screen.getByRole('menuitem', { name: 'Home' })).toBeInTheDocument();
    expect(screen.getByRole('menuitem', { name: 'Ontology Editor' })).toBeInTheDocument();
    expect(screen.getByRole('menuitem', { name: 'Analysis' })).toBeInTheDocument();
    expect(screen.getByRole('menuitem', { name: 'Action Center' })).toBeInTheDocument();
    
    // Check bottom section
    expect(screen.getByRole('menuitem', { name: 'My Page' })).toBeInTheDocument();
  });
  
  it('marks current route as active', () => {
    renderWithProviders(<GlobalSidebar />);
    
    const homeItem = screen.getByRole('menuitem', { name: 'Home' });
    expect(homeItem).toHaveClass('bp5-active');
  });
  
  it('handles collapse toggle', async () => {
    const onCollapsedChange = vi.fn();
    renderWithProviders(
      <GlobalSidebar onCollapsedChange={onCollapsedChange} />
    );
    
    const toggleButton = screen.getByRole('button', { name: /collapse sidebar/i });
    await userEvent.click(toggleButton);
    
    expect(onCollapsedChange).toHaveBeenCalledWith(true);
  });
  
  it('applies collapsed class when collapsed', () => {
    const { container } = renderWithProviders(
      <GlobalSidebar collapsed={true} />
    );
    
    const sidebar = container.querySelector('.global-sidebar');
    expect(sidebar).toHaveClass('global-sidebar--collapsed');
  });
  
  it('shows tooltips when collapsed', async () => {
    renderWithProviders(<GlobalSidebar collapsed={true} />);
    
    // Check that labels are hidden via aria-hidden
    const labels = screen.getAllByText(/Home|Ontology Editor|Analysis|Action Center|My Page/);
    labels.forEach(label => {
      expect(label).toHaveAttribute('aria-hidden', 'true');
    });
  });
  
  it('has proper accessibility attributes', () => {
    renderWithProviders(<GlobalSidebar />);
    
    // Check main nav element
    const nav = screen.getByRole('navigation', { name: 'Main navigation' });
    expect(nav).toBeInTheDocument();
    expect(nav).toHaveClass('global-sidebar');
    
    // Check bottom section has complementary role
    const bottomSection = screen.getByRole('complementary');
    expect(bottomSection).toBeInTheDocument();
    
    // Check active item has aria-current
    const homeItem = screen.getByRole('menuitem', { name: 'Home' });
    expect(homeItem).toHaveAttribute('aria-current', 'page');
  });
  
  it('updates store when collapsed state changes', async () => {
    renderWithProviders(<GlobalSidebar onCollapsedChange={() => {}} />);
    
    const toggleButton = screen.getByRole('button', { name: /collapse sidebar/i });
    await userEvent.click(toggleButton);
    
    expect(useUIStore.getState().sidebar.isCollapsed).toBe(true);
  });
  
  it('syncs with URL path for active section', () => {
    // Mock different route
    mockUseLocation.mockReturnValue({ pathname: '/editor' });
    
    renderWithProviders(<GlobalSidebar />);
    
    const editorItem = screen.getByRole('menuitem', { name: 'Ontology Editor' });
    expect(editorItem).toHaveClass('bp5-active');
  });
  
  it('maintains state on refresh via URL sync', () => {
    // Mock being on analysis page
    mockUseLocation.mockReturnValue({ pathname: '/analysis/dashboard' });
    
    renderWithProviders(<GlobalSidebar />);
    
    const analysisItem = screen.getByRole('menuitem', { name: 'Analysis' });
    expect(analysisItem).toHaveClass('bp5-active');
  });
  
  it('provides proper aria-labels for screen readers', () => {
    renderWithProviders(<GlobalSidebar />);
    
    // All menu items should have aria-labels
    const menuItems = screen.getAllByRole('menuitem');
    menuItems.forEach(item => {
      expect(item).toHaveAttribute('aria-label');
    });
    
    // Check aria-expanded attribute
    const homeItem = menuItems.find(item => item.getAttribute('aria-label') === 'Home');
    expect(homeItem).toBeDefined();
    expect(homeItem).toHaveAttribute('aria-expanded', 'true');
  });
  
  it('updates aria-expanded when collapsed', () => {
    // First test with collapsed=false
    const { rerender } = renderWithProviders(<GlobalSidebar collapsed={false} />);
    
    let menuItems = screen.getAllByRole('menuitem');
    let homeItem = menuItems.find(item => item.getAttribute('aria-label') === 'Home');
    
    expect(homeItem).toBeDefined();
    expect(homeItem).toHaveAttribute('aria-expanded', 'true');
    
    // Now test with collapsed=true
    rerender(<GlobalSidebar collapsed={true} />);
    
    menuItems = screen.getAllByRole('menuitem');
    homeItem = menuItems.find(item => item.getAttribute('aria-label') === 'Home');
    
    expect(homeItem).toBeDefined();
    expect(homeItem).toHaveAttribute('aria-expanded', 'false');
  });
});