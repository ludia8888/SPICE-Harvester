import { useMemo, useState } from 'react'
import { SidebarRail } from '../components/SidebarRail'
import { useAppStore } from '../state/store'
import { HomePage } from '../pages/HomePage'
import { DatasetsPage } from '../pages/DatasetsPage'
import { GraphPage } from '../pages/GraphPage'
import { OntologyPage } from '../pages/OntologyPage'
import { PlaceholderPage } from '../pages/PlaceholderPage'

type NavItem = {
  icon: string
  label: string
  key: string
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
  const [isRailExpanded, setRailExpanded] = useState(false)

  const railItems = useMemo(
    () =>
      navItems.map((item) => ({
        id: item.key,
        icon: item.icon,
        label: item.label,
        active: activeNav === item.key,
        onClick: () => setActiveNav(item.key),
      })),
    [activeNav, setActiveNav],
  )

  const content = (() => {
    switch (activeNav) {
      case 'datasets':
        return <DatasetsPage />
      case 'pipeline':
        return <GraphPage />
      case 'ontology':
        return <OntologyPage />
      case 'home':
        return <HomePage />
      default:
        return <PlaceholderPage title={navItems.find((item) => item.key === activeNav)?.label ?? 'View'} />
    }
  })()

  return (
    <div className="app-shell">
      <div className={`app-body ${isRailExpanded ? 'is-expanded' : ''}`}>
        <SidebarRail items={railItems} onHoverChange={setRailExpanded} />
        <main
          className={`main ${activeNav === 'pipeline' ? 'is-pipeline' : ''} ${activeNav === 'ontology' ? 'is-ontology' : ''} ${activeNav === 'datasets' ? 'is-files' : ''}`}
        >
          {content}
        </main>
      </div>
    </div>
  )
}
