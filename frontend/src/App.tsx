import { useMemo } from 'react'
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import { CommandTrackerDrawer } from './commands/CommandTrackerDrawer'
import { ContextNavbar } from './components/layout/ContextNavbar'
import { InspectorPanel } from './components/layout/InspectorPanel'
import { SettingsDialog } from './components/layout/SettingsDialog'
import { SidebarPanel } from './components/layout/SidebarPanel'
import { SidebarRail, type RailItem } from './components/layout/SidebarRail'
import { retryPendingAuthRequest } from './api/bff'
import { useAppStore } from './store/useAppStore'
import { ProjectsPage } from './pages/ProjectsPage'
import { OverviewPage } from './pages/OverviewPage'
import { BranchesPage } from './pages/BranchesPage'
import { OntologyPage } from './pages/OntologyPage'
import { MappingsPage } from './pages/MappingsPage'
import { SheetsHubPage } from './pages/SheetsHubPage'
import { ImportSheetsPage } from './pages/ImportSheetsPage'
import { ImportExcelPage } from './pages/ImportExcelPage'
import { SchemaSuggestionPage } from './pages/SchemaSuggestionPage'
import { InstancesPage } from './pages/InstancesPage'
import { GraphExplorerPage } from './pages/GraphExplorerPage'
import { QueryBuilderPage } from './pages/QueryBuilderPage'
import { MergePage } from './pages/MergePage'
import { AuditPage } from './pages/AuditPage'
import { LineagePage } from './pages/LineagePage'
import { TasksPage } from './pages/TasksPage'
import { AdminPage } from './pages/AdminPage'
import './App.css'

const AppLayout = () => {
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)
  const commandDrawerOpen = useAppStore((state) => state.commandDrawerOpen)
  const openCommandDrawer = useAppStore((state) => state.openCommandDrawer)
  const closeCommandDrawer = useAppStore((state) => state.closeCommandDrawer)
  const settingsDialogOpen = useAppStore((state) => state.settingsDialogOpen)
  const settingsDialogReason = useAppStore((state) => state.settingsDialogReason)
  const openSettingsDialog = useAppStore((state) => state.openSettingsDialog)
  const closeSettingsDialog = useAppStore((state) => state.closeSettingsDialog)

  const dbPrefix = context.project ? `/db/${encodeURIComponent(context.project)}` : ''

  const railItems = useMemo<RailItem[]>(
    () => [
      { icon: 'home', label: 'Home', to: '/' },
      { icon: 'cube', label: 'Overview', to: dbPrefix ? `${dbPrefix}/overview` : '/' },
      { icon: 'git-branch', label: 'Branches', to: dbPrefix ? `${dbPrefix}/branches` : '/' },
      { icon: 'diagram-tree', label: 'Ontology', to: dbPrefix ? `${dbPrefix}/ontology` : '/' },
      { icon: 'merge-columns', label: 'Mappings', to: dbPrefix ? `${dbPrefix}/mappings` : '/' },
      { icon: 'data-connection', label: 'Import', to: dbPrefix ? `${dbPrefix}/data/import/sheets` : '/' },
      { icon: 'graph', label: 'Graph', to: dbPrefix ? `${dbPrefix}/explore/graph` : '/' },
      { icon: 'search', label: 'Query', to: dbPrefix ? `${dbPrefix}/explore/query` : '/' },
      { icon: 'shield', label: 'Admin', to: '/operations/admin' },
    ],
    [dbPrefix],
  )

  const settingsCopy =
    context.language === 'ko'
      ? {
          settingsTitle: '설정',
          languageLabel: '언어',
          languageHelper: 'UI 언어를 선택합니다.',
          languageOptions: [
            { label: '한국어', value: 'ko' },
            { label: 'English', value: 'en' },
          ] as const,
          branchLabel: '브랜치',
          branchHelper: '기본 브랜치 컨텍스트입니다.',
          branchPlaceholder: 'e.g. main',
          authTokenLabel: '액세스 토큰',
          authTokenHelper: 'Authorization: Bearer 토큰',
          authTokenPlaceholder: 'e.g. change_me',
          tokenLabel: 'Admin token',
          tokenHelper: 'X-Admin-Token 헤더로 전송됩니다.',
          tokenPlaceholder: 'e.g. change_me',
          rememberTokenLabel: '이 기기에서 토큰 기억',
          darkModeLabel: '다크 모드',
          themeHelper: '이 브라우저에 저장됩니다.',
          adminModeLabel: 'Admin mode (위험 작업)',
          adminModeWarning: 'Admin mode는 되돌릴 수 없는 작업을 허용합니다.',
          auditLinkLabel: '감사 로그 열기',
        }
      : {
          settingsTitle: 'Settings',
          languageLabel: 'Language',
          languageHelper: 'Choose the UI language.',
          languageOptions: [
            { label: 'English', value: 'en' },
            { label: '한국어', value: 'ko' },
          ] as const,
          branchLabel: 'Branch',
          branchHelper: 'Default branch context.',
          branchPlaceholder: 'e.g. main',
          authTokenLabel: 'Access token',
          authTokenHelper: 'Sent via Authorization: Bearer header.',
          authTokenPlaceholder: 'e.g. change_me',
          tokenLabel: 'Admin token',
          tokenHelper: 'Sent via X-Admin-Token header.',
          tokenPlaceholder: 'e.g. change_me',
          rememberTokenLabel: 'Remember tokens on this device',
          darkModeLabel: 'Dark mode',
          themeHelper: 'Saved locally in this browser.',
          adminModeLabel: 'Admin mode (dangerous actions)',
          adminModeWarning: 'Admin mode enables irreversible actions.',
          auditLinkLabel: 'Open audit logs',
        }

  const commandCopy =
    context.language === 'ko'
      ? {
          title: '커맨드 트래커',
          tabs: { active: '진행중', completed: '완료', failed: '실패', expired: '만료' },
          addLabel: '커맨드 ID',
          addPlaceholder: 'command_id 입력',
          addButton: '추가',
          clearExpired: '만료 제거',
          removeLabel: '삭제',
          noGlobalList: '서버 전역 리스트는 없습니다. 이 브라우저에서 추적한 ID만 표시됩니다.',
          emptyState: '표시할 커맨드가 없습니다.',
          columns: {
            id: 'Command ID',
            status: 'Status',
            context: 'Context',
            submitted: 'Submitted',
            actions: 'Actions',
          },
          detailsTitle: '상세',
          detailsHint: '커맨드를 선택하세요.',
          detailsTokenHint: '토큰을 설정해야 상태를 확인할 수 있습니다.',
          detailsStatusLabel: 'Status',
          detailsErrorLabel: 'Error',
          detailsResultLabel: 'Result',
          detailsContextLabel: 'Context',
          detailsSubmittedLabel: 'Submitted',
          detailsUnknown: 'Unknown',
        }
      : {
          title: 'Command tracker',
          tabs: { active: 'Active', completed: 'Completed', failed: 'Failed', expired: 'Expired' },
          addLabel: 'Command ID',
          addPlaceholder: 'Paste command id',
          addButton: 'Add',
          clearExpired: 'Clear expired',
          removeLabel: 'Remove',
          noGlobalList: 'No server-wide command list. Only IDs tracked in this browser are shown.',
          emptyState: 'No commands in this view.',
          columns: {
            id: 'Command ID',
            status: 'Status',
            context: 'Context',
            submitted: 'Submitted',
            actions: 'Actions',
          },
          detailsTitle: 'Details',
          detailsHint: 'Select a command to view the latest status.',
          detailsTokenHint: 'Set a token to fetch command status.',
          detailsStatusLabel: 'Status',
          detailsErrorLabel: 'Error',
          detailsResultLabel: 'Result',
          detailsContextLabel: 'Context',
          detailsSubmittedLabel: 'Submitted',
          detailsUnknown: 'Unknown',
        }

  return (
    <div className="app-shell">
      <ContextNavbar onOpenCommands={() => openCommandDrawer()} />
      <div className="app-body">
        <SidebarRail
          items={railItems}
          onOpenSettings={() => openSettingsDialog()}
          settingsLabel={context.language === 'ko' ? '설정' : 'Settings'}
          userLabel={context.language === 'ko' ? '사용자' : 'User'}
        />
        <SidebarPanel />
        <main className="main">
          <Routes>
            <Route index element={<ProjectsPage />} />
            <Route path="db/:db" element={<OverviewPage />} />
            <Route path="db/:db/overview" element={<OverviewPage />} />
            <Route path="db/:db/branches" element={<BranchesPage />} />
            <Route path="db/:db/ontology" element={<OntologyPage />} />
            <Route path="db/:db/mappings" element={<MappingsPage />} />
            <Route path="db/:db/data/sheets" element={<SheetsHubPage />} />
            <Route path="db/:db/data/import/sheets" element={<ImportSheetsPage />} />
            <Route path="db/:db/data/import/excel" element={<ImportExcelPage />} />
            <Route path="db/:db/data/schema-suggestion" element={<SchemaSuggestionPage />} />
            <Route path="db/:db/instances" element={<InstancesPage />} />
            <Route path="db/:db/explore/graph" element={<GraphExplorerPage />} />
            <Route path="db/:db/explore/query" element={<QueryBuilderPage />} />
            <Route path="db/:db/merge" element={<MergePage />} />
            <Route path="db/:db/audit" element={<AuditPage />} />
            <Route path="db/:db/lineage" element={<LineagePage />} />
            <Route path="operations/tasks" element={<TasksPage />} />
            <Route path="operations/admin" element={<AdminPage />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </main>
        <InspectorPanel />
      </div>
      <CommandTrackerDrawer
        isOpen={commandDrawerOpen}
        onClose={() => closeCommandDrawer()}
        copy={commandCopy}
      />
      <SettingsDialog
        isOpen={settingsDialogOpen}
        reason={settingsDialogReason}
        onClose={closeSettingsDialog}
        onSave={() => {
          void retryPendingAuthRequest({ language: context.language, authToken, adminToken }).catch(
            () => undefined,
          )
          closeSettingsDialog()
        }}
        copy={settingsCopy}
      />
    </div>
  )
}

const App = () => (
  <BrowserRouter>
    <AppLayout />
  </BrowserRouter>
)

export default App
