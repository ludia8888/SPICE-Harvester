import { useMemo, useState } from 'react'
import {
  Alignment,
  Button,
  Navbar,
  NavbarGroup,
  Tag,
} from '@blueprintjs/core'
import { LeftNavBar } from './components/layout/LeftNavBar'
import { SettingsDialog } from './components/SettingsDialog'
import { CommandTrackerDrawer } from './commands/CommandTrackerDrawer'
import { useCommandTracker } from './commands/useCommandTracker'
import { InspectorDrawer } from './components/layout/InspectorDrawer'
import { AppRouter } from './app/AppRouter'
import { useAppStore } from './store/useAppStore'
import { usePathname } from './state/usePathname'
import { navigate } from './state/pathname'
import './App.css'

import type { IconName } from '@blueprintjs/icons'

/* ── LNB types ── */

export type LnbChildItem = {
  id: string
  label: string
  path: string
  match?: string
  requiresProject?: boolean
}

export type LnbItem = {
  id: string
  icon: IconName
  label: string
  path: string
  match?: string
  requiresProject?: boolean
  children?: LnbChildItem[]
}

export type LnbGroup = {
  id: 'workspace' | 'builder' | 'admin'
  title: string
  items: LnbItem[]
  position: 'top' | 'bottom'
}

type Copy = (typeof copyByLang)[keyof typeof copyByLang]

const copyByLang = {
  en: {
    appTitle: 'Spice OS',
    groups: {
      workspace: 'Workspace',
      builder: 'Builder',
      admin: 'Admin',
    },
    nav: {
      databases: 'Projects',
      connections: 'Data Sources',
      overview: 'Overview',
      datasets: 'Datasets',
      pipelines: 'Pipeline Builder',
      objectify: 'Objectify',
      ontology: 'Ontology',
      actions: 'Actions',
      governance: 'Governance',
      mappings: 'Mappings',
      instances: 'Instances',
      objectExplorer: 'Object Explorer',
      graph: 'Graph Explorer',
      query: 'Query Builder',
      dataAnalysis: 'Data Analysis',
      audit: 'Audit',
      lineage: 'Lineage',
      tasks: 'Tasks',
      scheduler: 'Scheduler',
      admin: 'Admin',
      ai: 'AI Assistant',
      commands: 'Commands',
      settings: 'Settings',
      adminMode: 'Admin mode',
      noProject: 'No project selected',
      jobMonitor: 'Job Monitor',
      profileAuth: 'Profile & Auth',
      selectProject: 'Select a project first',
    },
    commandDrawer: {
      title: 'Command Tracker',
      tabs: {
        active: 'Active',
        completed: 'Completed',
        failed: 'Failed',
        expired: 'Expired',
      },
      addLabel: 'Track command',
      addPlaceholder: 'Paste command_id',
      addButton: 'Add',
      clearExpired: 'Clear expired',
      removeLabel: 'Remove',
      noGlobalList: 'There is no global command list. Track command IDs you know here.',
      emptyState: 'No tracked commands yet.',
      columns: {
        id: 'Command ID',
        status: 'Status',
        context: 'Context',
        submitted: 'Submitted',
        actions: 'Actions',
      },
      detailsTitle: 'Details',
      detailsHint: 'Select a command to load details.',
      detailsTokenHint: 'Admin token required to load status.',
      detailsStatusLabel: 'Status',
      detailsErrorLabel: 'Error',
      detailsResultLabel: 'Result',
      detailsContextLabel: 'Context',
      detailsSubmittedLabel: 'Submitted',
      detailsUnknown: 'Unknown',
    },
    settings: {
      settingsTitle: 'Settings',
      languageLabel: 'Language',
      languageHelper: 'UI uses ?lang=ko|en.',
      languageOptions: [
        { label: 'English', value: 'en' },
        { label: 'Korean', value: 'ko' },
      ],
      branchLabel: 'Branch',
      branchHelper: 'Branch is part of the URL context (SSoT).',
      branchPlaceholder: 'e.g. main',
      tokenLabel: 'Admin token',
      tokenHelper: 'Uses X-Admin-Token header for BFF.',
      tokenPlaceholder: 'e.g. change_me',
      rememberTokenLabel: 'Remember token on this device',
      darkModeLabel: 'Dark mode',
      themeHelper: 'Saved on this device.',
      adminModeLabel: 'Admin mode (dangerous actions)',
      adminModeWarning: 'Admin mode enables irreversible operations. Proceed carefully.',
      auditLinkLabel: 'Open recent audit logs',
      devModeLabel: 'Developer mode',
      devModeHelper: 'Show token override, environment info, and debug options.',
      devEnvTitle: 'Environment',
    },
  },
  ko: {
    appTitle: 'Spice OS',
    groups: {
      workspace: '워크스페이스',
      builder: '빌더',
      admin: '관리',
    },
    nav: {
      databases: '프로젝트',
      connections: '데이터 소스',
      overview: '요약',
      datasets: '데이터셋',
      pipelines: '파이프라인 빌더',
      objectify: '오브젝트화',
      ontology: '온톨로지',
      actions: '액션',
      governance: '거버넌스',
      mappings: '매핑',
      instances: '인스턴스',
      objectExplorer: '오브젝트 탐색기',
      graph: '그래프 탐색',
      query: '쿼리 빌더',
      dataAnalysis: '데이터 분석',
      audit: '감사 로그',
      lineage: '리니지',
      tasks: '작업',
      scheduler: '스케줄러',
      admin: '관리자',
      ai: 'AI 어시스턴트',
      commands: '커맨드',
      settings: '설정',
      adminMode: '관리자 모드',
      noProject: '선택된 프로젝트 없음',
      jobMonitor: '작업 모니터',
      profileAuth: '프로필 및 인증',
      selectProject: '프로젝트를 먼저 선택하세요',
    },
    commandDrawer: {
      title: '커맨드 트래커',
      tabs: {
        active: '진행중',
        completed: '완료',
        failed: '실패',
        expired: '만료',
      },
      addLabel: '커맨드 추적',
      addPlaceholder: 'command_id 붙여넣기',
      addButton: '추가',
      clearExpired: '만료 정리',
      removeLabel: '삭제',
      noGlobalList: '서버 전역 목록이 없습니다. 알고 있는 command_id만 추적하세요.',
      emptyState: '추적 중인 커맨드가 없습니다.',
      columns: {
        id: '커맨드 ID',
        status: '상태',
        context: '컨텍스트',
        submitted: '제출 시간',
        actions: '작업',
      },
      detailsTitle: '상세',
      detailsHint: '커맨드를 선택하세요.',
      detailsTokenHint: '상태 조회에는 관리자 토큰이 필요합니다.',
      detailsStatusLabel: '상태',
      detailsErrorLabel: '에러',
      detailsResultLabel: '결과',
      detailsContextLabel: '컨텍스트',
      detailsSubmittedLabel: '제출 시간',
      detailsUnknown: '알 수 없음',
    },
    settings: {
      settingsTitle: '설정',
      languageLabel: '언어',
      languageHelper: 'UI는 ?lang=ko|en 사용',
      languageOptions: [
        { label: '한국어', value: 'ko' },
        { label: 'English', value: 'en' },
      ],
      branchLabel: '브랜치',
      branchHelper: '브랜치는 URL 컨텍스트(SSoT)에 포함됩니다.',
      branchPlaceholder: '예: main',
      tokenLabel: '관리자 토큰',
      tokenHelper: 'BFF 요청에 X-Admin-Token 헤더로 전달됩니다.',
      tokenPlaceholder: '예: change_me',
      rememberTokenLabel: '이 기기에 토큰 저장',
      darkModeLabel: '다크 모드',
      themeHelper: '이 기기에 저장됩니다.',
      adminModeLabel: '관리자 모드(위험 작업)',
      adminModeWarning: '관리자 모드는 되돌릴 수 없는 작업을 허용합니다.',
      auditLinkLabel: '최근 감사 로그 열기',
      devModeLabel: '개발자 모드',
      devModeHelper: '토큰 재설정, 환경 정보, 디버그 옵션을 표시합니다.',
      devEnvTitle: '환경 정보',
    },
  },
} as const

const getLnbGroups = (project: string | null, copy: Copy): LnbGroup[] => {
  const base = project ? `/db/${encodeURIComponent(project)}` : null
  const p = (suffix: string) => (base ? `${base}${suffix}` : '')

  return [
    {
      id: 'workspace',
      title: copy.groups.workspace,
      position: 'top',
      items: [
        {
          id: 'databases',
          icon: 'folder-close',
          label: copy.nav.databases,
          path: '/',
          requiresProject: false,
        },
        {
          id: 'overview',
          icon: 'home',
          label: copy.nav.overview,
          path: p('/overview'),
          match: base ? `${base}/overview` : undefined,
          requiresProject: true,
        },
        {
          id: 'object-explorer',
          icon: 'search',
          label: copy.nav.objectExplorer,
          path: p('/explore/objects'),
          match: base ? `${base}/explore/objects` : undefined,
          requiresProject: true,
        },
        {
          id: 'graph-network',
          icon: 'graph',
          label: copy.nav.graph,
          path: p('/explore/graph'),
          match: base ? `${base}/explore/graph` : undefined,
          requiresProject: true,
        },
        {
          id: 'action-center',
          icon: 'take-action',
          label: copy.nav.actions,
          path: p('/actions'),
          match: base ? `${base}/actions` : undefined,
          requiresProject: true,
        },
        {
          id: 'query-builder',
          icon: 'console',
          label: copy.nav.query,
          path: p('/explore/query'),
          match: base ? `${base}/explore/query` : undefined,
          requiresProject: true,
        },
        {
          id: 'data-analysis',
          icon: 'chart',
          label: copy.nav.dataAnalysis,
          path: p('/analyze'),
          match: base ? `${base}/analyze` : undefined,
          requiresProject: true,
        },
        {
          id: 'audit',
          icon: 'history',
          label: copy.nav.audit,
          path: p('/audit'),
          match: base ? `${base}/audit` : undefined,
          requiresProject: true,
        },
        {
          id: 'lineage',
          icon: 'data-lineage',
          label: copy.nav.lineage,
          path: p('/lineage'),
          match: base ? `${base}/lineage` : undefined,
          requiresProject: true,
        },
      ],
    },
    {
      id: 'builder',
      title: copy.groups.builder,
      position: 'top',
      items: [
        {
          id: 'connections',
          icon: 'data-connection',
          label: copy.nav.connections,
          path: '/connections',
          match: '/connections',
          requiresProject: false,
        },
        {
          id: 'pipelines',
          icon: 'flow-linear',
          label: copy.nav.pipelines,
          path: p('/pipelines'),
          match: base ? `${base}/pipelines` : undefined,
          requiresProject: true,
        },
        {
          id: 'objectify',
          icon: 'cube',
          label: copy.nav.objectify,
          path: p('/objectify'),
          match: base ? `${base}/objectify` : undefined,
          requiresProject: true,
        },
        {
          id: 'ontology-manager',
          icon: 'diagram-tree',
          label: copy.nav.ontology,
          path: p('/ontology'),
          match: base ? `${base}/ontology` : undefined,
          requiresProject: true,
          children: [
            { id: 'ontology', label: copy.nav.ontology, path: p('/ontology'), match: base ? `${base}/ontology` : undefined, requiresProject: true },
            { id: 'mappings', label: copy.nav.mappings, path: p('/mappings'), match: base ? `${base}/mappings` : undefined, requiresProject: true },
            { id: 'instances', label: copy.nav.instances, path: p('/instances'), match: base ? `${base}/instances` : undefined, requiresProject: true },
            { id: 'governance', label: copy.nav.governance, path: p('/governance'), match: base ? `${base}/governance` : undefined, requiresProject: true },
          ],
        },
        {
          id: 'ai',
          icon: 'lightbulb',
          label: copy.nav.ai,
          path: '/ai',
          match: '/ai',
          requiresProject: false,
        },
      ],
    },
    {
      id: 'admin',
      title: copy.groups.admin,
      position: 'bottom',
      items: [
        {
          id: 'job-monitor',
          icon: 'timeline-events',
          label: copy.nav.jobMonitor,
          path: '/operations/tasks',
          match: '/operations',
          requiresProject: false,
          children: [
            { id: 'tasks', label: copy.nav.tasks, path: '/operations/tasks', match: '/operations/tasks' },
            { id: 'scheduler', label: copy.nav.scheduler, path: '/operations/scheduler', match: '/operations/scheduler' },
          ],
        },
        {
          id: 'settings',
          icon: 'cog',
          label: copy.nav.settings,
          path: '__settings__',
          requiresProject: false,
        },
        {
          id: 'profile',
          icon: 'user',
          label: copy.nav.profileAuth,
          path: '/operations/admin',
          match: '/operations/admin',
          requiresProject: false,
        },
      ],
    },
  ]
}

const findActiveLabel = (groups: LnbGroup[], pathname: string): string | null => {
  for (const group of groups) {
    for (const item of group.items) {
      if (item.children) {
        for (const child of item.children) {
          const active = child.match ? pathname.startsWith(child.match) : pathname === child.path
          if (active) return child.label
        }
      }
      const active = item.match ? pathname.startsWith(item.match) : pathname === item.path
      if (active) return item.label
    }
  }
  return null
}

const countActiveCommands = (commands: Record<string, { writePhase: string; indexPhase: string; expired?: boolean }>) =>
  Object.values(commands).filter((cmd) => {
    if (cmd.expired) return false
    if (cmd.writePhase === 'FAILED' || cmd.writePhase === 'CANCELLED') return false
    if (cmd.writePhase === 'WRITE_DONE' && cmd.indexPhase === 'VISIBLE_IN_SEARCH') return false
    return true
  }).length

const AppShell = () => {
  const pathname = usePathname()
  const context = useAppStore((state) => state.context)
  const adminMode = useAppStore((state) => state.adminMode)
  const commands = useAppStore((state) => state.commands)
  const setSettingsOpen = useAppStore((state) => state.setSettingsOpen)
  const sidebarExpanded = useAppStore((state) => state.sidebarExpanded)
  const toggleSidebar = useAppStore((state) => state.toggleSidebar)

  const [commandOpen, setCommandOpen] = useState(false)

  useCommandTracker()

  const language = context.language
  const copy = copyByLang[language]
  const lnbGroups = useMemo(() => getLnbGroups(context.project, copy), [context.project, copy])
  const activeCommandCount = useMemo(() => countActiveCommands(commands), [commands])
  const activePageLabel = useMemo(() => findActiveLabel(lnbGroups, pathname), [lnbGroups, pathname])

  // Pipeline editor needs full-height layout (no padding/scroll on <main>)
  const isPipelineEditor = !!(context.project && /\/pipelines\/[^/]+/.test(pathname))

  // Login page renders without shell chrome
  if (pathname === '/login') {
    return <AppRouter />
  }

  return (
    <div className="app-shell">
      <Navbar className="top-nav">
        <NavbarGroup align={Alignment.LEFT} className="top-nav-breadcrumbs">
          <Button className="breadcrumb-btn" minimal icon="application" onClick={() => navigate('/')} text={copy.appTitle} />
          {context.project ? (
            <>
              <span className="bp6-icon bp6-icon-chevron-right breadcrumb-separator" />
              <Button className="breadcrumb-btn" minimal icon="database" onClick={() => navigate(`/db/${encodeURIComponent(context.project!)}/overview`)} text={context.project} />
            </>
          ) : (
            <>
              <span className="bp6-icon bp6-icon-chevron-right breadcrumb-separator" />
              <Tag minimal icon="database" className="breadcrumb-tag">{copy.nav.noProject}</Tag>
            </>
          )}
          {activePageLabel ? (
            <>
              <span className="bp6-icon bp6-icon-chevron-right breadcrumb-separator" />
              <Tag minimal className="breadcrumb-tag">{activePageLabel}</Tag>
            </>
          ) : null}
        </NavbarGroup>
        <NavbarGroup align={Alignment.RIGHT}>
          <Tag minimal icon="git-branch" className="breadcrumb-tag" style={{ marginRight: 8 }}>{context.branch}</Tag>
          <Button minimal icon="history" onClick={() => setCommandOpen(true)} aria-label={copy.nav.commands}>
            {copy.nav.commands}
            {activeCommandCount > 0 ? (
              <Tag minimal round style={{ marginLeft: 6 }}>
                {activeCommandCount}
              </Tag>
            ) : null}
          </Button>
        </NavbarGroup>
      </Navbar>

      <div className={`app-body ${sidebarExpanded ? 'is-expanded' : ''}`}>
        <LeftNavBar
          groups={lnbGroups}
          expanded={sidebarExpanded}
          onToggleExpanded={toggleSidebar}
          pathname={pathname}
          project={context.project}
          onNavigate={navigate}
          onSettingsClick={() => setSettingsOpen(true)}
          selectProjectLabel={copy.nav.selectProject}
        />
        <main className={`main${isPipelineEditor ? ' is-pipeline' : ''}`}>
          <AppRouter />
        </main>
      </div>

      <CommandTrackerDrawer
        isOpen={commandOpen}
        onClose={() => setCommandOpen(false)}
        copy={copy.commandDrawer}
      />
      <SettingsDialog copy={copy.settings} />
      <InspectorDrawer />
    </div>
  )
}

export default AppShell
