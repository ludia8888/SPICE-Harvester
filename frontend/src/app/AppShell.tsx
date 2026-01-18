import { useMemo, useState } from 'react'
import { SidebarRail } from '../components/SidebarRail'
import { useAppStore, type NavKey } from '../state/store'
import { HomePage } from '../pages/HomePage'
import { DatasetsPage } from '../pages/DatasetsPage'
import { ConnectorsPage } from '../pages/ConnectorsPage'
import { GraphPage } from '../pages/GraphPage'
import { OntologyPage } from '../pages/OntologyPage'
import { AIAgentPage } from '../pages/AIAgentPage'
import { WorkshopPage } from '../pages/WorkshopPage'
import { PlaceholderPage } from '../pages/PlaceholderPage'

type NavItem = {
  icon: string
  label: string
  key: NavKey
}

const navItems: NavItem[] = [
  { icon: 'home', label: 'Home', key: 'home' },
  { icon: 'folder-close', label: 'Files', key: 'datasets' },
  { icon: 'database', label: 'Connectors', key: 'connectors' },
  { icon: 'flow-branch', label: 'Pipeline Builder', key: 'pipeline' },
  { icon: 'cube', label: 'Ontology Management', key: 'ontology' },
  { icon: 'predictive-analysis', label: 'AI Agent', key: 'ai-agent' },
  { icon: 'build', label: 'Workshop', key: 'workshop' },
]

export const AppShell = () => {
  const activeNav = useAppStore((state) => state.activeNav)
  const setActiveNav = useAppStore((state) => state.setActiveNav)
  const isAiAgentOpen = useAppStore((state) => state.isAiAgentOpen)
  const setAiAgentOpen = useAppStore((state) => state.setAiAgentOpen)
  const [isRailExpanded, setRailExpanded] = useState(false)

  const railItems = useMemo(
    () =>
      navItems.map((item) => {
        if (item.key === 'ai-agent') {
          return {
            id: item.key,
            icon: item.icon,
            label: item.label,
            active: isAiAgentOpen,
            onClick: () => setAiAgentOpen(!isAiAgentOpen),
          }
        }
        return {
          id: item.key,
          icon: item.icon,
          label: item.label,
          active: activeNav === item.key,
          onClick: () => {
            setActiveNav(item.key)
            setAiAgentOpen(false)
          },
        }
      }),
    [activeNav, isAiAgentOpen, setActiveNav, setAiAgentOpen],
  )

  const content = (() => {
    switch (activeNav) {
      case 'datasets':
        return <DatasetsPage />
      case 'pipeline':
        return <GraphPage />
      case 'ontology':
        return <OntologyPage />
      case 'connectors':
        return <ConnectorsPage />
      case 'ai-agent':
        return <AIAgentPage />
      case 'workshop':
        return <WorkshopPage />
      case 'home':
        return <HomePage />
      default:
        return <PlaceholderPage title={navItems.find((item) => item.key === activeNav)?.label ?? 'View'} />
    }
  })()

  return (
    <div className="app-shell">
      <div className={`app-body ${isRailExpanded ? 'is-expanded' : ''} ${isAiAgentOpen ? 'is-ai-open' : ''}`}>
        <SidebarRail items={railItems} onHoverChange={setRailExpanded} />
        <div className={`ai-agent-panel ${isAiAgentOpen ? 'is-open' : ''}`}>
          <div className="ai-agent-panel-inner">
            <AIAgentPage />
          </div>
        </div>
        <main
          className={`main ${activeNav === 'pipeline' ? 'is-pipeline' : ''} ${activeNav === 'ontology' ? 'is-ontology' : ''} ${activeNav === 'datasets' ? 'is-files' : ''}`}
        >
          {content}
        </main>
      </div>
    </div>
  )
}
