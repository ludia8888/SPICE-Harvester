# SPICE HARVESTER Frontend Setup Documentation

## Overview
Complete frontend development environment setup following Palantir Foundry's design principles with Blueprint.js integration.

## Technology Stack

### Core Framework
- **React 18** - UI library with concurrent features
- **TypeScript 5** - Type-safe development
- **Vite** - Fast build tool and dev server

### UI Framework
- **Blueprint.js 5** - Palantir's React UI toolkit
  - `@blueprintjs/core` - Core components
  - `@blueprintjs/datetime` - Date/time components
  - `@blueprintjs/icons` - Icon library
  - `@blueprintjs/select` - Advanced select components
  - `@blueprintjs/table` - Data table component

### State Management
- **Zustand** - Lightweight state management
  - With **immer** middleware for immutable updates
  - DevTools integration
  - Persist middleware for local storage
- **Relay** - GraphQL client (Facebook's approach)
- **Apollo Client** - Alternative GraphQL client
- **React Query** - Server state management

### Visualization Libraries
- **Cytoscape.js** - Graph/network visualization
- **D3.js** - Data visualization
- **React Flow** - Node-based editor
- **Three.js** - 3D graphics
- **Konva/React Konva** - 2D canvas graphics
- **Pixi.js** - WebGL renderer

### Real-time Collaboration
- **Socket.io** - WebSocket communication
- **Yjs** - CRDT for real-time collaboration
- **y-websocket** - WebSocket provider for Yjs

### Development Tools
- **ESLint** - Code linting with TypeScript support
- **Prettier** - Code formatting
- **Stylelint** - SCSS/CSS linting
- **Sass** - CSS preprocessor
- **React Router** - Routing
- **Vitest** - Unit testing framework
- **MSW** - API mocking for tests
- **Storybook** - Component development and documentation
- **React Query DevTools** - Query debugging

## Project Structure

```
frontend/
├── src/
│   ├── components/          # React components
│   │   ├── common/         # Reusable UI components
│   │   ├── layout/         # Page structure
│   │   ├── visualization/  # Data viz components
│   │   ├── ontology/       # Ontology editor
│   │   ├── data-management/ # Data handling
│   │   └── collaboration/  # Real-time features
│   ├── design-system/      # Design tokens & theme
│   │   ├── styles/         # Global styles
│   │   ├── theme/          # Theme provider
│   │   └── tokens/         # Design tokens
│   ├── services/           # API & external services
│   │   ├── api/           # API clients
│   │   ├── graph/         # Graph utilities
│   │   ├── visualization/ # Viz helpers
│   │   ├── websocket/     # Real-time connections
│   │   └── collaboration/ # Collab features
│   ├── stores/            # Zustand stores
│   ├── utils/             # Utility functions
│   ├── hooks/             # Custom React hooks
│   └── types/             # TypeScript types
├── public/                # Static assets
├── data/                  # GraphQL schema
└── tests/                 # Test files
```

## Key Configurations

### Vite Configuration (`vite.config.ts`)
- React plugin with Fast Refresh
- Path aliases for clean imports
- Environment variable handling
- Build optimizations

### TypeScript Configuration (`tsconfig.json`)
- Strict type checking with some relaxed rules:
  - `noUncheckedIndexedAccess`: false (for easier array/object access)
  - `exactOptionalPropertyTypes`: false (for flexible optional props)
- Path mappings for clean imports (@components, @services, etc.)
- JSX support for React 18
- Vitest globals included in types

### GraphQL Setup
- Schema definition in `data/schema.graphql`
- Relay compiler configuration
- Apollo Client with cache setup

## Design System Integration

### Blueprint.js Theme
- Dark mode support via `bp5-dark` class
- Blueprint SCSS variables imported in `variables.scss`
- Focus management with `FocusStyleManager`
- All Blueprint CSS modules imported

### Simplified Token System
- **Removed**: typography, spacing, shadows, borders, animations tokens
- **Kept**: Custom colors for application-specific needs
  - Visualization palette
  - Key indicators (primary, title, foreign)
  - Status colors (experimental, deprecated, active)
- Using Blueprint's built-in design system for everything else

### SCSS Architecture
- Import Blueprint variables: `@import '@blueprintjs/core/lib/scss/variables'`
- Define only app-specific overrides
- Custom layout dimensions (sidebar, header, etc.)
- Responsive breakpoints aligned with standards

## Service Architecture

### API Services
- GraphQL client setup (Relay + Apollo)
- REST API utilities
- Error handling middleware
- Request/response interceptors

### Visualization Services
- D3.js helpers and configurations
- Cytoscape layout algorithms
- React Flow custom nodes
- WebGL optimizations

### Real-time Services
- Socket.io connection management
- Yjs document synchronization
- Presence awareness
- Conflict resolution

## State Management

### Store Structure
- `auth.store.ts` - Authentication state
- `ui.store.ts` - UI preferences
- `graph.store.ts` - Graph data
- `collaboration.store.ts` - Real-time state

### Zustand Features
- **Immer middleware** for immutable updates
- **Persist middleware** for local storage
- **DevTools middleware** for debugging
- TypeScript support with proper typing

### Best Practices
- Use immer for complex state updates
- Export selectors for computed values
- Handle side effects in store actions
- Proper error handling in async actions

## Development Workflow

### Getting Started
```bash
cd frontend
npm install
npm run dev
```

### Available Scripts
- `npm run dev` - Start development server
- `npm run build` - Production build
- `npm run preview` - Preview production build
- `npm run typecheck` - TypeScript checking
- `npm run lint` - Run ESLint
- `npm run lint:fix` - Auto-fix ESLint issues
- `npm run format` - Format code with Prettier
- `npm run format:check` - Check code formatting
- `npm run stylelint` - Lint SCSS files
- `npm run stylelint:fix` - Auto-fix SCSS issues
- `npm run test` - Run tests with Vitest
- `npm run test:ui` - Run tests with UI
- `npm run test:coverage` - Generate coverage report
- `npm run storybook` - Start Storybook
- `npm run build-storybook` - Build Storybook
- `npm run codegen` - Generate GraphQL types
- `npm run relay` - Compile Relay queries
- `npm run relay:watch` - Watch mode for Relay

### Environment Variables
Create `.env.local` for local development:
```
VITE_GRAPHQL_ENDPOINT=http://localhost:4000/graphql
VITE_WEBSOCKET_ENDPOINT=ws://localhost:4000
VITE_API_BASE_URL=http://localhost:8080/api
VITE_APP_VERSION=0.1.0
VITE_ENVIRONMENT=development
VITE_ENABLE_MOCK_DATA=false
VITE_ENABLE_DEV_TOOLS=true
VITE_PUBLIC_URL=/
```

## Performance Optimizations

### Code Splitting
- Route-based splitting
- Lazy loading for heavy components
- Dynamic imports for libraries

### Visualization Performance
- Canvas rendering for large datasets
- WebGL acceleration
- Virtual scrolling
- Debounced updates

### Bundle Optimization
- Tree shaking enabled
- Vendor chunk splitting
- Asset optimization
- Compression

## Testing Strategy

### Test Infrastructure
- **Framework**: Vitest (fast, Vite-native test runner)
- **Testing Library**: React Testing Library + Jest DOM
- **API Mocking**: MSW (Mock Service Worker) v2
- **Test Utilities**: Custom render with providers

### Unit Testing
- Component testing with `renderWithProviders`
- Hook testing with `renderHook`
- Store testing with Zustand
- Pure function testing

### Integration Testing
- MSW for GraphQL/REST mocking
- Relay environment mocking
- Full provider wrapping
- User interaction testing

### Test Setup
- Global setup in `src/test/setup.ts`
- Test utilities in `src/utils/test-utils.tsx`
- Mock generators for common entities
- Automatic cleanup after tests

## Security Considerations

### Content Security Policy
- Strict CSP headers
- XSS prevention
- CSRF protection

### Authentication
- JWT token management
- Secure storage
- Session handling

### Data Protection
- Input sanitization
- Output encoding
- Secure communications

## Deployment

### Build Process
```bash
npm run build
# Output in dist/ directory
```

### Production Optimizations
- Minification
- Source map generation
- Asset fingerprinting
- CDN integration

### Docker Support
- Multi-stage builds
- Nginx configuration
- Environment injection

## Maintenance

### Dependency Updates
- Regular security updates
- Blueprint.js version alignment
- Breaking change management

### Code Quality
- ESLint rules
- Prettier formatting
- TypeScript strict mode
- Code review process

## Resources

### Documentation
- [Blueprint.js Documentation](https://blueprintjs.com/docs/)
- [React Documentation](https://react.dev/)
- [TypeScript Handbook](https://www.typescriptlang.org/docs/)

### Internal Docs
- `/docs/DesignSystem.md` - Design principles
- `/docs/UIUX.md` - UI/UX guidelines
- `/src/components/README.md` - Component guidelines