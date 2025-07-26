import { createContext, useContext, useState, useEffect, type ReactNode } from 'react';

export type Theme = 'light' | 'dark';

interface IThemeContext {
  theme: Theme;
  toggleTheme: () => void;
}

const ThemeContext = createContext<IThemeContext | undefined>(undefined);

interface IThemeProviderProps {
  children: ReactNode;
  defaultTheme?: Theme;
}

export function ThemeProvider({ children, defaultTheme = 'light' }: IThemeProviderProps): JSX.Element {
  const [theme, setTheme] = useState<Theme>(() => {
    const saved = localStorage.getItem('spice-harvester-theme');
    return (saved as Theme) || defaultTheme;
  });

  useEffect(() => {
    localStorage.setItem('spice-harvester-theme', theme);
    document.documentElement.dataset.theme = theme;
    
    // Apply theme class for Blueprint theming
    if (theme === 'dark') {
      document.documentElement.classList.add('bp5-dark');
    } else {
      document.documentElement.classList.remove('bp5-dark');
    }
  }, [theme]);

  const toggleTheme = (): void => {
    setTheme((prev) => (prev === 'light' ? 'dark' : 'light'));
  };

  const value: IThemeContext = {
    theme,
    toggleTheme,
  };

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

export function useTheme(): IThemeContext {
  const context = useContext(ThemeContext);
  if (!context) {
    throw new Error('useTheme must be used within a ThemeProvider');
  }
  return context;
}