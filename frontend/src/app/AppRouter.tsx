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
import { DatasetsPage } from '../pages/DatasetsPage'
import { ConnectionsPage } from '../pages/ConnectionsPage'
import { PipelineBuilderPage } from '../pages/PipelineBuilderPage'
import { ObjectifyPage } from '../pages/ObjectifyPage'
import { ActionsPage } from '../pages/ActionsPage'
import { ObjectExplorerPage } from '../pages/ObjectExplorerPage'
import { DatasetAnalysisPage } from '../pages/DatasetAnalysisPage'
import { GovernancePage } from '../pages/GovernancePage'
import { SchedulerPage } from '../pages/SchedulerPage'
import { AIAssistantPage } from '../pages/AIAssistantPage'

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

  /* Global routes (no project context) */
  if (segments[0] === 'connections') {
    return <ConnectionsPage />
  }

  if (segments[0] === 'ai') {
    return <AIAssistantPage />
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
    if (section === 'datasets') {
      /* /db/:name/datasets/:datasetId/analyze */
      if (segments[4] === 'analyze') {
        return <DatasetAnalysisPage dbName={dbName} />
      }
      return <DatasetsPage dbName={dbName} />
    }
    if (section === 'pipelines') {
      const pipelineId = segments[3] ?? null
      return <PipelineBuilderPage dbName={dbName} pipelineId={pipelineId} />
    }
    if (section === 'objectify') {
      return <ObjectifyPage dbName={dbName} />
    }
    if (section === 'actions') {
      return <ActionsPage dbName={dbName} />
    }
    if (section === 'governance') {
      return <GovernancePage dbName={dbName} />
    }
    if (section === 'explore') {
      const sub = segments[3] ?? 'graph'
      if (sub === 'graph') {
        return <GraphExplorerPage dbName={dbName} />
      }
      if (sub === 'query') {
        return <QueryBuilderPage dbName={dbName} />
      }
      if (sub === 'objects') {
        return <ObjectExplorerPage dbName={dbName} />
      }
    }
    if (section === 'analyze') {
      return <DatasetAnalysisPage dbName={dbName} />
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
    if (segments[1] === 'scheduler') {
      return <SchedulerPage />
    }
    if (segments[1] === 'admin') {
      return <AdminPage />
    }
  }

  return <NotFoundPage />
}
