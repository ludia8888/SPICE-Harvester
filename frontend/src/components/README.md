# SPICE HARVESTER Component Architecture

This directory contains all React components organized by feature and functionality, following Palantir's design philosophy and Blueprint.js patterns.

## Directory Structure

```
components/
├── common/          # Reusable UI components
├── layout/          # Page structure components
├── visualization/   # Data visualization components
├── ontology/        # Ontology editor components
├── data-management/ # Data handling components
└── collaboration/   # Real-time collaboration components
```

## Component Guidelines

### 1. Common Components (`/common`)
- Generic, reusable UI elements
- Extend Blueprint components with custom functionality
- Examples: Enhanced buttons, specialized form controls, custom modals

### 2. Layout Components (`/layout`)
- Application structure and navigation
- Page containers and responsive layouts
- Examples: Sidebar, Header, Footer, PageContainer

### 3. Visualization Components (`/visualization`)
- Data visualization using D3.js, Cytoscape, React Flow
- Interactive graphs and charts
- Examples: NetworkGraph, TimeSeriesChart, DataGrid

### 4. Ontology Components (`/ontology`)
- Ontology creation and editing tools
- Visual relationship mapping
- Examples: OntologyEditor, ClassHierarchy, PropertyPanel

### 5. Data Management Components (`/data-management`)
- Data import/export interfaces
- Pipeline builders and processors
- Examples: DataImporter, PipelineBuilder, DataValidator

### 6. Collaboration Components (`/collaboration`)
- Real-time collaboration features
- Presence indicators and commenting
- Examples: CollaborativeEditor, PresenceIndicator, CommentThread

## Component Standards

### File Naming
- Components: PascalCase (e.g., `DataGrid.tsx`)
- Utilities: camelCase (e.g., `formatData.ts`)
- Styles: Same name as component (e.g., `DataGrid.scss`)

### Component Structure
```typescript
// DataGrid.tsx
import { FC, memo } from 'react';
import { Classes } from '@blueprintjs/core';
import './DataGrid.scss';

interface IDataGridProps {
  // Props definition
}

export const DataGrid: FC<IDataGridProps> = memo(({ ... }) => {
  // Component implementation
});
```

### Using Blueprint Components
- Always extend Blueprint components when possible
- Use Blueprint's Classes constants for styling
- Follow Blueprint's accessibility patterns

### Performance Considerations
- Use React.memo for expensive components
- Implement virtualization for large data sets
- Lazy load visualization libraries

## State Management
- Local state: useState/useReducer
- Shared state: Zustand stores
- Server state: Relay/Apollo

## Testing
- Unit tests with React Testing Library
- Visual regression tests for visualizations
- E2E tests for critical workflows