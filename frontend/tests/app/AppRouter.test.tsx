import { beforeEach, describe, expect, it, vi } from 'vitest'
import { screen } from '@testing-library/react'
import { AppRouter } from '../../src/app/AppRouter'
import { resetAppStore, renderWithClient } from '../testUtils'

let mockPathname = '/'

vi.mock('../../src/state/usePathname', () => ({
  usePathname: () => mockPathname,
}))

vi.mock('../../src/pages/NotFoundPage', () => ({
  NotFoundPage: () => <div data-testid="not-found-page">not-found</div>,
}))
vi.mock('../../src/pages/DatabasesPage', () => ({
  DatabasesPage: () => <div data-testid="databases-page">databases</div>,
}))
vi.mock('../../src/pages/OverviewPage', () => ({
  OverviewPage: ({ dbName }: { dbName: string }) => <div data-testid="overview-page">{dbName}</div>,
}))
vi.mock('../../src/pages/OntologyPage', () => ({
  OntologyPage: () => <div data-testid="ontology-page">ontology</div>,
}))
vi.mock('../../src/pages/MappingsPage', () => ({
  MappingsPage: () => <div data-testid="mappings-page">mappings</div>,
}))
vi.mock('../../src/pages/InstancesPage', () => ({
  InstancesPage: () => <div data-testid="instances-page">instances</div>,
}))
vi.mock('../../src/pages/GraphExplorerPage', () => ({
  GraphExplorerPage: () => <div data-testid="graph-page">graph</div>,
}))
vi.mock('../../src/pages/QueryBuilderPage', () => ({
  QueryBuilderPage: () => <div data-testid="query-page">query</div>,
}))
vi.mock('../../src/pages/AuditPage', () => ({
  AuditPage: () => <div data-testid="audit-page">audit</div>,
}))
vi.mock('../../src/pages/LineagePage', () => ({
  LineagePage: () => <div data-testid="lineage-page">lineage</div>,
}))
vi.mock('../../src/pages/TasksPage', () => ({
  TasksPage: () => <div data-testid="tasks-page">tasks</div>,
}))
vi.mock('../../src/pages/AdminPage', () => ({
  AdminPage: () => <div data-testid="admin-page">admin</div>,
}))

describe('AppRouter', () => {
  beforeEach(() => {
    resetAppStore()
    mockPathname = '/'
  })

  it('rejects legacy data tools path', () => {
    mockPathname = '/db/core/data/legacy-view'
    renderWithClient(<AppRouter />)

    expect(screen.getByTestId('not-found-page')).toBeInTheDocument()
  })

  it('rejects legacy branches path', () => {
    mockPathname = '/db/core/branches'
    renderWithClient(<AppRouter />)

    expect(screen.getByTestId('not-found-page')).toBeInTheDocument()
  })

  it('keeps active ontology route', () => {
    mockPathname = '/db/core/ontology'
    renderWithClient(<AppRouter />)

    expect(screen.getByTestId('ontology-page')).toBeInTheDocument()
  })
})
