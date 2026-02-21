import { useEffect, useMemo } from 'react'
import { usePathname } from '../state/usePathname'
import { useAppStore } from '../store/useAppStore'
import { NotFoundPage } from '../pages/NotFoundPage'
import { DatabasesPage } from '../pages/DatabasesPage'
import { OverviewPage } from '../pages/OverviewPage'
import { OntologyPage } from '../pages/OntologyPage'
import { MappingsPage } from '../pages/MappingsPage'
import { InstancesPage } from '../pages/InstancesPage'
import { GraphExplorerPage } from '../pages/GraphExplorerPage'
import { QueryBuilderPage } from '../pages/QueryBuilderPage'
import { AuditPage } from '../pages/AuditPage'
import { LineagePage } from '../pages/LineagePage'
import { TasksPage } from '../pages/TasksPage'
import { AdminPage } from '../pages/AdminPage'

export const AppRouter = () => {
  const pathname = usePathname()
  const segments = useMemo(() => pathname.split('/').filter(Boolean), [pathname])

  const context = useAppStore((state) => state.context)
  const setProject = useAppStore((state) => state.setProject)

  useEffect(() => {
    if (segments[0] !== 'db' || !segments[1]) {
      return
    }
    const dbName = decodeURIComponent(segments[1])
    if (context.project !== dbName) {
      setProject(dbName)
    }
  }, [context.project, segments, setProject])

  if (segments.length === 0) {
    return <DatabasesPage />
  }

  if (segments[0] === 'db' && segments[1]) {
    const dbName = decodeURIComponent(segments[1])
    const section = segments[2] ?? 'overview'
    if (section === 'overview') {
      return <OverviewPage dbName={dbName} />
    }
    if (section === 'ontology') {
      return <OntologyPage dbName={dbName} />
    }
    if (section === 'mappings') {
      return <MappingsPage dbName={dbName} />
    }
    if (section === 'instances') {
      return <InstancesPage dbName={dbName} />
    }
    if (section === 'explore') {
      const sub = segments[3] ?? 'graph'
      if (sub === 'graph') {
        return <GraphExplorerPage dbName={dbName} />
      }
      if (sub === 'query') {
        return <QueryBuilderPage dbName={dbName} />
      }
    }
    if (section === 'audit') {
      return <AuditPage dbName={dbName} />
    }
    if (section === 'lineage') {
      return <LineagePage dbName={dbName} />
    }
  }

  if (segments[0] === 'operations') {
    if (segments[1] === 'tasks') {
      return <TasksPage />
    }
    if (segments[1] === 'admin') {
      return <AdminPage />
    }
  }

  return <NotFoundPage />
}
