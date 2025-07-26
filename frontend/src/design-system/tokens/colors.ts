/**
 * SPICE HARVESTER Custom Color Tokens
 * Application-specific colors that extend Blueprint's color system
 */

// Application-specific colors for data visualization and special UI elements
export const customColors = {
  // Key indicators (Palantir-style)
  keys: {
    primary: '#7C3AED', // Purple for primary keys
    title: '#10B981', // Green for title keys
    foreign: '#3B82F6', // Blue for foreign keys
  },
  
  // Status colors for ontology elements
  status: {
    experimental: '#FF8000', // Orange
    deprecated: '#A7ABB6', // Gray
    active: '#00AF00', // Green
  },
  
  // Data visualization palette
  visualization: {
    categorical: [
      '#0072FF', // Blue
      '#7C3AED', // Purple
      '#10B981', // Green
      '#F59E0B', // Yellow
      '#EF4444', // Red
      '#EC4899', // Pink
      '#8B5CF6', // Violet
      '#06B6D4', // Cyan
    ],
  },
} as const;

export type CustomColorToken = typeof customColors;