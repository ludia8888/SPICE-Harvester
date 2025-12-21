import { useMemo, useState } from 'react'
import {
  Alignment,
  Button,
  Navbar,
  NavbarGroup,
  NavbarHeading,
  Tag,
} from '@blueprintjs/core'
import { SidebarRail } from './components/layout/SidebarRail'
import { SettingsPopoverContent } from './components/layout/SettingsPopoverContent'
import { SettingsDialog } from './components/SettingsDialog'
import { CommandTrackerDrawer } from './commands/CommandTrackerDrawer'
import { InspectorDrawer } from './components/layout/InspectorDrawer'
import { AppRouter } from './app/AppRouter'
import { useAppStore } from './store/useAppStore'
import { usePathname } from './state/usePathname'
import { navigate } from './state/pathname'
import './App.css'





type Copy = (typeof copyByLang)[keyof typeof copyByLang]

const copyByLang = {
  en: {
    appTitle: 'SPICE Harvester',
    nav: {
      databases: 'Databases',
      overview: 'Home',
      branches: 'Branches',
      ontology: 'Ontology',
      mappings: 'Mappings',
      sheets: 'Connectors',
      importSheets: 'Import Sheets',
      importExcel: 'Import Excel',
      schemaSuggestion: 'Schema Suggestion',
      instances: 'Instances',
      graph: 'Graph Explorer',
      query: 'Query Builder',
      merge: 'Merge',
      audit: 'Audit',
      lineage: 'Lineage',
      tasks: 'Tasks',
      admin: 'Admin',
      commands: 'Commands',
      settings: 'Settings',
      project: 'Project',
      branch: 'Branch',
      adminMode: 'Admin mode',
      noProject: 'No project selected',
    },
    sections: {
      context: 'Context',
      flow: 'Flow',
      navigation: 'Navigation',
      data: 'Data',
      explore: 'Explore',
      ops: 'Operations',
    },
    steps: [
      { id: 1, title: 'Project setup', description: 'Create or select a project' },
      { id: 2, title: 'Branch setup', description: 'Create or select a branch' },
      { id: 3, title: 'Ontology', description: 'Define classes and relationships' },
      { id: 4, title: 'Mappings', description: 'Connect source fields' },
    ],
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
    },
  },
  ko: {
    appTitle: 'SPICE Harvester',
    nav: {
      databases: '프로젝트',
      overview: '홈',
      branches: '브랜치',
      ontology: '온톨로지',
      mappings: '매핑',
      sheets: '커넥터',
      importSheets: '시트 임포트',
      importExcel: '엑셀 임포트',
      schemaSuggestion: '스키마 제안',
      instances: '인스턴스',
      graph: '그래프 탐색',
      query: '쿼리 빌더',
      merge: '머지',
      audit: '감사 로그',
      lineage: '리니지',
      tasks: '작업',
      admin: '관리자',
      commands: '커맨드',
      settings: '설정',
      project: '프로젝트',
      branch: '브랜치',
      adminMode: '관리자 모드',
      noProject: '선택된 프로젝트 없음',
    },
    sections: {
      context: '컨텍스트',
      flow: '진행 흐름',
      navigation: '내비게이션',
      data: '데이터',
      explore: '탐색',
      ops: '운영',
    },
    steps: [
      { id: 1, title: '프로젝트 설정', description: '프로젝트 생성 또는 선택' },
      { id: 2, title: '브랜치 설정', description: '브랜치 생성 또는 선택' },
      { id: 3, title: '온톨로지', description: '클래스와 관계 정의' },
      { id: 4, title: '매핑', description: '소스 필드 연결' },
    ],
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
    },
  },
} as const



const getRailItems = (
  project: string | null,
  pathname: string,
  copy: Copy,
) => {
  const items = [] as Array<{ icon: string; label: string; path: string; match?: string }>

  if (!project) {
    items.push({ icon: 'database', label: copy.nav.databases, path: '/' })
    items.push({ icon: 'timeline-events', label: copy.nav.tasks, path: '/operations/tasks', match: '/operations/tasks' })
    items.push({ icon: 'shield', label: copy.nav.admin, path: '/operations/admin', match: '/operations/admin' })
  } else {
    const base = `/db/${encodeURIComponent(project)}`
    items.push({ icon: 'home', label: copy.nav.overview, path: `${base}/overview`, match: `${base}/overview` })
    items.push({ icon: 'git-branch', label: copy.nav.branches, path: `${base}/branches`, match: `${base}/branches` })
    items.push({ icon: 'diagram-tree', label: copy.nav.ontology, path: `${base}/ontology`, match: `${base}/ontology` })
    items.push({ icon: 'flow-branch', label: copy.nav.mappings, path: `${base}/mappings`, match: `${base}/mappings` })
    items.push({ icon: 'offline', label: copy.nav.sheets, path: `${base}/data/sheets`, match: `${base}/data/sheets` })
    items.push({ icon: 'database', label: copy.nav.instances, path: `${base}/instances`, match: `${base}/instances` })
    items.push({ icon: 'graph', label: copy.nav.graph, path: `${base}/explore/graph`, match: `${base}/explore/graph` })
    items.push({ icon: 'search', label: copy.nav.query, path: `${base}/explore/query`, match: `${base}/explore/query` })
  }

  return items.map((item) => ({
    icon: item.icon,
    label: item.label,
    active: item.match ? pathname.startsWith(item.match) : pathname === item.path,
    onClick: () => navigate(item.path),
  }))
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

  const [commandOpen, setCommandOpen] = useState(false)

  const language = context.language
  const copy = copyByLang[language]
  // const navSections = useMemo(() => getNavSections(context.project, copy), [context.project, copy])
  const railItems = useMemo(
    () => getRailItems(context.project, pathname, copy),
    [context.project, pathname, copy],
  )
  const activeCommandCount = useMemo(() => countActiveCommands(commands), [commands])

  return (
    <div className="app-shell">
      <Navbar className="top-nav">
        <NavbarGroup align={Alignment.LEFT}>
          <NavbarHeading>{copy.appTitle}</NavbarHeading>
          <Tag minimal icon="database">
            {context.project ?? copy.nav.noProject}
          </Tag>
          <Tag minimal icon="git-branch">{context.branch}</Tag>
          {adminMode ? <Tag intent="warning">{copy.nav.adminMode}</Tag> : null}
        </NavbarGroup>
        <NavbarGroup align={Alignment.RIGHT}>
          <Button minimal icon="database" onClick={() => navigate('/')} aria-label={copy.nav.databases}>
            {copy.nav.databases}
          </Button>
          <Button minimal icon="history" onClick={() => setCommandOpen(true)} aria-label={copy.nav.commands}>
            {copy.nav.commands}
            {activeCommandCount > 0 ? (
              <Tag minimal round style={{ marginLeft: 6 }}>
                {activeCommandCount}
              </Tag>
            ) : null}
          </Button>
          <Button minimal icon="cog" onClick={() => setSettingsOpen(true)} aria-label={copy.nav.settings}>
            {copy.nav.settings}
          </Button>
        </NavbarGroup>
      </Navbar>

      <div className="app-body">
        <SidebarRail
          items={railItems}
          settingsLabel={copy.nav.settings}
          userLabel="User"
          settingsContent={<SettingsPopoverContent copy={copy.settings} />}
        />

        <main className="main">
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
