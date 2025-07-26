/**
 * SPICE HARVESTER Design System - Token Exports
 * Application-specific tokens that extend Blueprint's design system
 */

export * from './colors';

// Import only application-specific tokens
import { customColors } from './colors';

export const tokens = {
  customColors,
} as const;

export type DesignTokens = typeof tokens;