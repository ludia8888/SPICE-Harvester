# SPICE HARVESTER Frontend

Enterprise data platform frontend built with React, TypeScript, and Palantir Blueprint.

## Technology Stack

### Core
- **React 18** - UI framework
- **TypeScript 5** - Type safety
- **Vite 5** - Build tool
- **Blueprint.js** - Palantir's UI component library

### State Management
- **Relay Modern** - GraphQL state management (Facebook's approach)
- **Zustand** - Local state management with immer middleware
- **Apollo Client** - Alternative GraphQL client
- **React Query** - Server state caching with DevTools

### Visualization & Graph
- **Cytoscape.js** - Large-scale graph visualization
- **React Flow** - Node-based workflow editor
- **D3.js** - Custom data visualizations
- **Three.js** - 3D visualizations (optional)
- **Konva** - Canvas-based graphics
- **Pixi.js** - WebGL renderer

### Real-time Collaboration
- **Yjs** - CRDT-based collaboration
- **Socket.io** - WebSocket communication
- **y-websocket** - Yjs WebSocket provider

### Drag & Drop
- **React DnD** - Drag and drop framework
- **React Draggable** - Draggable components
- **React Dropzone** - File drop zone

### Development Tools
- **ESLint** - Code linting with TypeScript support
- **Prettier** - Code formatting
- **Stylelint** - CSS/SCSS linting
- **Vitest** - Fast unit testing framework
- **MSW** - API mocking for tests
- **Storybook** - Component development and documentation
- **React Testing Library** - Component testing utilities

## Getting Started

### Prerequisites
- Node.js v18.0.0+
- npm v9.0.0+

### Installation

```bash
# Install dependencies
npm install

# Create environment file
cp .env.example .env
```

### Development

```bash
# Start development server
npm run dev

# Run in different modes
npm run dev -- --host  # Expose to network
npm run dev -- --port 3001  # Use different port
```

### Building

```bash
# Type check
npm run typecheck

# Build for production
npm run build

# Preview production build
npm run preview
```

### Testing

```bash
# Run tests
npm run test

# Run tests with UI
npm run test:ui

# Generate coverage
npm run test:coverage
```

### Code Quality

```bash
# Lint code
npm run lint
npm run lint:fix

# Format code
npm run format
npm run format:check

# Lint styles
npm run stylelint
npm run stylelint:fix
```

### GraphQL

```bash
# Generate GraphQL types
npm run codegen

# Run Relay compiler
npm run relay
npm run relay:watch
```

### Storybook

```bash
# Start Storybook
npm run storybook

# Build Storybook
npm run build-storybook
```

## Project Structure

```
frontend/
├── src/
│   ├── __generated__/      # Relay generated files
│   ├── components/         # React components (organized by domain)
│   │   ├── collaboration/  # Real-time collaboration components
│   │   ├── common/        # Reusable UI components
│   │   ├── data-management/ # Data handling components
│   │   ├── layout/        # Layout components
│   │   ├── ontology/      # Ontology editor components
│   │   └── visualization/ # Data visualization components
│   ├── design-system/      # Simplified design system
│   │   ├── styles/        # SCSS with Blueprint integration
│   │   ├── theme/         # Theme provider
│   │   └── tokens/        # Custom color tokens only
│   ├── relay/              # Relay environment and provider
│   ├── services/           # Service configurations
│   │   ├── apollo/        # Apollo client setup
│   │   ├── collaboration/ # Yjs configuration
│   │   ├── graph/         # Graph library configs
│   │   ├── interaction/   # Drag-drop configuration
│   │   ├── visualization/ # D3 configuration
│   │   └── websocket/     # Socket.io setup
│   ├── stores/             # Zustand stores with immer
│   ├── test/               # Test setup and utilities
│   ├── types/              # TypeScript type definitions
│   └── utils/              # Utility functions and test helpers
├── data/                   # GraphQL schema (to be added)
├── public/                 # Public assets
└── .storybook/            # Storybook configuration
```

## Environment Variables

Create a `.env` file based on `.env.example`:

```env
# GraphQL API endpoint
VITE_GRAPHQL_ENDPOINT=http://localhost:4000/graphql

# WebSocket endpoint for real-time collaboration
VITE_WEBSOCKET_ENDPOINT=ws://localhost:4000

# REST API base URL
VITE_API_BASE_URL=http://localhost:8080/api

# Application version
VITE_APP_VERSION=0.1.0

# Environment
VITE_ENVIRONMENT=development

# Feature flags
VITE_ENABLE_MOCK_DATA=false
VITE_ENABLE_DEV_TOOLS=true
```

## Design System

The application leverages Blueprint.js as the primary design system with minimal custom extensions:

- **Blueprint Integration**: Full Blueprint.js component library with dark mode support
- **Custom Colors Only**: Application-specific colors for data visualization and domain entities
- **Simplified Tokens**: Removed duplicate tokens, using Blueprint's built-in system
- **SCSS Architecture**: Direct import of Blueprint variables for consistency
- **Focus Management**: Keyboard-only focus styles via FocusStyleManager

## Key Features

1. **Ontology Management**: Visual editor for data models
2. **Graph Visualization**: Interactive node/link diagrams
3. **Real-time Collaboration**: Multi-user editing with presence
4. **Drag & Drop**: Intuitive interactions
5. **Dark Mode**: Full theme support
6. **Responsive**: Desktop-optimized with responsive capabilities

## Browser Support

- Chrome/Edge (latest)
- Firefox (latest)
- Safari (latest)
- No IE11 support

## Performance Considerations

- Virtual scrolling for large datasets
- Code splitting by route
- Lazy loading of heavy components
- WebGL acceleration for graphs
- Optimistic UI updates

## Security

- JWT-based authentication
- CSRF protection
- Content Security Policy
- Input sanitization
- Secure WebSocket connections

## Contributing

1. Follow the existing code style
2. Write tests for new features
3. Update documentation
4. Create descriptive commit messages
5. Submit PR with detailed description

## Current Status

The frontend infrastructure is fully configured with:
- ✅ Complete development environment setup
- ✅ Testing framework (Vitest + MSW + Testing Library)
- ✅ Blueprint.js design system integration
- ✅ GraphQL clients (Apollo + Relay)
- ✅ State management (Zustand with immer)
- ✅ All development tooling configured

**Note**: Component implementation is in progress. The foundation is ready for rapid development.

## License

MIT