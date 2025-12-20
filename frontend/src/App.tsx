import { useCallback, useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Alignment,
  Button,
  Card,
  Divider,
  FormGroup,
  H1,
  H5,
  HTMLSelect,
  Icon,
  InputGroup,
  Navbar,
  NavbarGroup,
  NavbarHeading,
  Text,
  TextArea,
  Intent,
} from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/icons'
import {
  createDatabase,
  deleteDatabase,
  getDatabaseExpectedSeq,
  listDatabases,
  openDatabase,
} from './api/bff'
import { DangerConfirmDialog } from './components/DangerConfirmDialog'
import { SettingsPopoverContent } from './components/layout/SettingsPopoverContent'
import { SidebarRail } from './components/layout/SidebarRail'
import { classifyError } from './errors/classifyError'
import { qk } from './query/queryKeys'
import { useAppStore } from './store/useAppStore'
import './App.css'

type StatusMessage = { type: 'success' | 'error' | 'info'; text: string }

const copyByLang = {
  en: {
    navTitle: 'SPICE Harvester',
    dataSourceTitle: 'Data Source',
    projectTitle: 'Project',
    flowTitle: 'Flow',
    sourceName: 'Local stack',
    sourceMeta: 'BFF + OMS + Funnel',
    sourcePath: '/api/v1',
    steps: [
      { id: 1, title: 'Project setup', description: 'Create or select a project' },
      { id: 2, title: 'Branch setup', description: 'Create or select a branch' },
      { id: 3, title: 'Ontology', description: 'Define classes and relationships' },
      { id: 4, title: 'Mappings', description: 'Connect source fields' },
    ],
    rail: {
      home: 'Home',
      projects: 'Projects',
      db: 'DB',
      ontology: 'Ontology',
      instances: 'Instances',
      queries: 'Queries',
      mappings: 'Mappings',
      commands: 'Commands',
      monitoring: 'Monitoring',
      settings: 'Settings',
      user: 'User',
    },
    pageTitle: 'Project',
    pageSubtitle: 'Select an existing project or create a new one via the BFF.',
    selectTitle: 'Select project',
    selectLabel: 'Project',
    selectHelper: 'GET /api/v1/databases',
    openLabel: 'Open',
    refreshLabel: 'Refresh list',
    selectNote: 'Projects map to `/api/v1/databases/...`.',
    createTitle: 'Create project',
    nameLabel: 'Project name',
    nameHelper: "lowercase + numbers + '_' or '-' (db_name)",
    namePlaceholder: 'e.g. demo_project',
    descriptionLabel: 'Description (optional)',
    descriptionPlaceholder: 'Short summary for teammates',
    createLabel: 'Create project',
    openapiLabel: 'View OpenAPI',
    createNote: 'POST `/api/v1/databases` returns 202 + `command_id`.',
    settingsTitle: 'Settings',
    languageLabel: 'Language',
    languageHelper: 'UI uses `?lang=ko|en`.',
    darkModeLabel: 'Dark mode',
    themeHelper: 'Saved on this device.',
    languageOptions: [
      { label: 'English', value: 'en' },
      { label: '한국어', value: 'ko' },
    ],
    branchLabel: 'Branch',
    branchHelper: 'Branch is part of the URL context (SSoT).',
    branchPlaceholder: 'e.g. main',
    loadError: 'Failed to load projects. Check the BFF server.',
    projectHelper: 'Selected project (db_name)',
    projectEmpty: 'No project selected',
    openMissing: 'Select a project first.',
    openSuccess: 'Project opened:',
    openError: 'Failed to open the project.',
    authRequired: 'Authentication required. Set an admin token.',
    deleteLabel: 'Delete project',
    deleteConfirmTitle: 'Delete project',
    deleteConfirmBody: 'This action permanently deletes the project and its data.',
    deleteConfirmAction: 'Delete',
    deleteCancelAction: 'Cancel',
    deleteMissing: 'Select a project first.',
    deleteInvalidSeq: 'Expected sequence must be 0 or higher.',
    deleteAccepted: 'Delete request accepted.',
    deleteSuccess: 'Project deleted:',
    deleteError: 'Failed to delete the project.',
    deleteConflict: 'OCC conflict. Update expected sequence and retry.',
    expectedSeqLabel: 'Expected sequence',
    expectedSeqHelper: 'Required for DELETE (OCC). Usually 1 for new projects.',
    expectedSeqPlaceholder: 'e.g. 1',
    createMissingName: 'Project name is required.',
    createAccepted: 'Project creation accepted.',
    createSuccess: 'Project created:',
    createError: 'Failed to create the project.',
    commandLabel: 'command id:',
    tokenLabel: 'Admin token',
    tokenHelper: 'Uses X-Admin-Token header for BFF.',
    tokenPlaceholder: 'e.g. change_me',
    rememberTokenLabel: 'Remember token on this device',
    adminModeLabel: 'Admin mode (dangerous actions)',
    adminModeWarning: 'Admin mode enables irreversible operations. Proceed carefully.',
    auditLinkLabel: 'Open recent audit logs',
    adminModeRequired: 'Enable Admin mode to perform this action.',
    changeReasonLabel: 'Change reason',
    changeReasonPlaceholder: 'Why are you doing this?',
    typedConfirmLabel: 'Type the project name to confirm',
    dangerConfirmHint: 'Required for auditability and safer operations.',
    commandsTitle: 'Commands',
    commandsCurrent: 'This context',
    commandsOther: 'Other contexts',
  },
  ko: {
    navTitle: 'SPICE Harvester',
    dataSourceTitle: '데이터 소스',
    projectTitle: '프로젝트',
    flowTitle: '진행 순서',
    sourceName: '로컬 스택',
    sourceMeta: 'BFF + OMS + Funnel',
    sourcePath: '/api/v1',
    steps: [
      { id: 1, title: '프로젝트 설정', description: '프로젝트를 생성하거나 선택' },
      { id: 2, title: '브랜치 설정', description: '브랜치 생성 또는 선택' },
      { id: 3, title: '온톨로지', description: '클래스와 관계 정의' },
      { id: 4, title: '매핑', description: '소스 필드 연결' },
    ],
    rail: {
      home: '홈',
      projects: '프로젝트',
      db: 'DB',
      ontology: '온톨로지',
      instances: '인스턴스',
      queries: '쿼리',
      mappings: '매핑',
      commands: '커맨드',
      monitoring: '모니터링',
      settings: '설정',
      user: '사용자',
    },
    pageTitle: '프로젝트',
    pageSubtitle: '기존 프로젝트를 선택하거나 BFF를 통해 새로 생성합니다.',
    selectTitle: '프로젝트 선택',
    selectLabel: '프로젝트',
    selectHelper: 'GET /api/v1/databases',
    openLabel: '열기',
    refreshLabel: '목록 새로고침',
    selectNote: '프로젝트는 `/api/v1/databases/...`와 연결됩니다.',
    createTitle: '프로젝트 생성',
    nameLabel: '프로젝트 이름',
    nameHelper: "영문 소문자 + 숫자 + '_' 또는 '-' (db_name)",
    namePlaceholder: '예: demo_project',
    descriptionLabel: '설명 (선택)',
    descriptionPlaceholder: '팀을 위한 간단한 설명',
    createLabel: '프로젝트 생성',
    openapiLabel: 'OpenAPI 보기',
    createNote: 'POST `/api/v1/databases`는 202 + `command_id`를 반환합니다.',
    settingsTitle: '설정',
    languageLabel: '언어',
    languageHelper: 'UI는 `?lang=ko|en`을 사용합니다.',
    darkModeLabel: '다크 모드',
    themeHelper: '이 기기에 저장됩니다.',
    languageOptions: [
      { label: '한국어', value: 'ko' },
      { label: 'English', value: 'en' },
    ],
    branchLabel: '브랜치',
    branchHelper: '브랜치는 URL 컨텍스트(SSoT)에 포함됩니다.',
    branchPlaceholder: '예: main',
    loadError: '프로젝트 목록을 불러오지 못했습니다. BFF 서버를 확인하세요.',
    projectHelper: '선택된 프로젝트 (db_name)',
    projectEmpty: '선택된 프로젝트 없음',
    openMissing: '프로젝트를 먼저 선택하세요.',
    openSuccess: '프로젝트가 열렸습니다:',
    openError: '프로젝트 열기에 실패했습니다.',
    authRequired: '인증이 필요합니다. 관리자 토큰을 입력하세요.',
    deleteLabel: '프로젝트 삭제',
    deleteConfirmTitle: '프로젝트 삭제',
    deleteConfirmBody: '이 작업은 프로젝트와 데이터를 영구적으로 삭제합니다.',
    deleteConfirmAction: '삭제',
    deleteCancelAction: '취소',
    deleteMissing: '프로젝트를 먼저 선택하세요.',
    deleteInvalidSeq: 'expected_seq는 0 이상이어야 합니다.',
    deleteAccepted: '삭제 요청이 접수되었습니다.',
    deleteSuccess: '프로젝트가 삭제되었습니다:',
    deleteError: '프로젝트 삭제에 실패했습니다.',
    deleteConflict: 'OCC 충돌입니다. expected_seq를 갱신하세요.',
    expectedSeqLabel: 'Expected sequence',
    expectedSeqHelper: '삭제에 필요합니다(OCC). 새 프로젝트는 보통 1입니다.',
    expectedSeqPlaceholder: '예: 1',
    createMissingName: '프로젝트 이름을 입력하세요.',
    createAccepted: '프로젝트 생성이 접수되었습니다.',
    createSuccess: '프로젝트가 생성되었습니다:',
    createError: '프로젝트 생성에 실패했습니다.',
    commandLabel: '커맨드 id:',
    tokenLabel: '관리자 토큰',
    tokenHelper: 'BFF 호출 시 X-Admin-Token 헤더를 사용합니다.',
    tokenPlaceholder: '예: change_me',
    rememberTokenLabel: '이 기기에서 토큰 기억하기',
    adminModeLabel: '관리자 모드(위험 작업)',
    adminModeWarning: '관리자 모드는 되돌릴 수 없는 작업을 활성화합니다. 주의하세요.',
    auditLinkLabel: '최근 감사 로그 보기',
    adminModeRequired: '이 작업을 하려면 관리자 모드를 켜야 합니다.',
    changeReasonLabel: '변경 사유',
    changeReasonPlaceholder: '왜 이 작업을 하나요?',
    typedConfirmLabel: '확인을 위해 프로젝트 이름을 입력하세요',
    dangerConfirmHint: '감사/추적과 안전한 운영을 위해 필요합니다.',
    commandsTitle: '커맨드',
    commandsCurrent: '현재 컨텍스트',
    commandsOther: '다른 컨텍스트',
  },
} as const

function App() {
  const queryClient = useQueryClient()
  const context = useAppStore((state) => state.context)
  const adminToken = useAppStore((state) => state.adminToken)
  const adminMode = useAppStore((state) => state.adminMode)
  const commands = useAppStore((state) => state.commands)

  const setProject = useAppStore((state) => state.setProject)
  const trackCommand = useAppStore((state) => state.trackCommand)

  const selectedDb = context.project ?? ''
  const canRead = Boolean(adminToken)
  const canWrite = Boolean(adminToken) && adminMode

  const [newDbName, setNewDbName] = useState('')
  const [newDbDescription, setNewDbDescription] = useState('')
  const [deleteOpen, setDeleteOpen] = useState(false)
  const [deleteStatus, setDeleteStatus] = useState<StatusMessage | null>(null)
  const [selectStatus, setSelectStatus] = useState<StatusMessage | null>(null)
  const [createStatus, setCreateStatus] = useState<StatusMessage | null>(null)

  const copy = copyByLang[context.language]
  const railItems: Array<{ icon: IconName; label: string; active?: boolean }> = [
    { icon: 'home', label: copy.rail.home },
    { icon: 'folder-open', label: copy.rail.projects, active: true },
    { icon: 'database', label: copy.rail.db },
    { icon: 'diagram-tree', label: copy.rail.ontology },
    { icon: 'cube', label: copy.rail.instances },
    { icon: 'search', label: copy.rail.queries },
    { icon: 'exchange', label: copy.rail.mappings },
    { icon: 'console', label: copy.rail.commands },
    { icon: 'dashboard', label: copy.rail.monitoring },
  ]

  const requestContext = useMemo(
    () => ({ language: context.language, adminToken }),
    [adminToken, context.language],
  )

  const databasesQuery = useQuery({
    queryKey: qk.databases(context.language),
    queryFn: () => listDatabases(requestContext),
    enabled: canRead,
  })

  const databases = useMemo(() => (canRead ? databasesQuery.data ?? [] : []), [canRead, databasesQuery.data])
  const projectOptions = useMemo(() => {
    const options = databases.map((name) => ({ label: name, value: name }))
    if (selectedDb && !databases.includes(selectedDb)) {
      options.unshift({ label: selectedDb, value: selectedDb })
    }
    if (options.length === 0) {
      options.push({ label: copy.projectEmpty, value: '' })
    }
    return options
  }, [copy.projectEmpty, databases, selectedDb])

  const dbError = useMemo(() => {
    if (!adminToken) {
      return copy.authRequired
    }
    const error = databasesQuery.error
    if (!error) {
      return null
    }
    if (classifyError(error).kind === 'AUTH') {
      return copy.authRequired
    }
    return copy.loadError
  }, [adminToken, copy.authRequired, copy.loadError, databasesQuery.error])

  useEffect(() => {
    if (context.project) {
      return
    }
    const first = databases[0]
    if (first) {
      setProject(first)
    }
  }, [context.project, databases, setProject])

  const openMutation = useMutation({
    mutationFn: (dbName: string) => openDatabase(requestContext, dbName),
    onSuccess: (_data, dbName) => {
      setSelectStatus({ type: 'success', text: `${copy.openSuccess} ${dbName}` })
    },
    onError: (error: unknown) => {
      if (classifyError(error).kind === 'AUTH') {
        setSelectStatus({ type: 'error', text: copy.authRequired })
        return
      }
      console.error('Failed to open project', error)
      setSelectStatus({ type: 'error', text: copy.openError })
    },
  })

  const handleOpen = useCallback(() => {
    if (!selectedDb) {
      setSelectStatus({ type: 'error', text: copy.openMissing })
      return
    }
    if (!adminToken) {
      setSelectStatus({ type: 'error', text: copy.authRequired })
      return
    }
    setSelectStatus(null)
    openMutation.mutate(selectedDb)
  }, [adminToken, copy.authRequired, copy.openMissing, openMutation, selectedDb])

  const createMutation = useMutation({
    mutationFn: (input: { name: string; description?: string }) => createDatabase(requestContext, input),
    onSuccess: (result, variables) => {
      const suffix = result.commandId ? ` ${copy.commandLabel} ${result.commandId}` : ''
      if (result.status === 202) {
        setCreateStatus({ type: 'info', text: `${copy.createAccepted}${suffix}` })
      } else {
        setCreateStatus({ type: 'success', text: `${copy.createSuccess} ${variables.name}` })
      }

      if (result.commandId) {
        trackCommand({
          id: result.commandId,
          kind: 'CREATE_DATABASE',
          target: { dbName: variables.name },
          context: { project: variables.name, branch: context.branch },
          submittedAt: new Date().toISOString(),
          writePhase: 'SUBMITTED',
          indexPhase: 'UNKNOWN',
          title: `${copy.createTitle}: ${variables.name}`,
        })
      } else {
        void queryClient.invalidateQueries({ queryKey: qk.databases(context.language) })
      }

      setProject(variables.name)
      setNewDbName('')
      setNewDbDescription('')
    },
    onError: (error: unknown) => {
      if (classifyError(error).kind === 'AUTH') {
        setCreateStatus({ type: 'error', text: copy.authRequired })
        return
      }
      console.error('Failed to create project', error)
      setCreateStatus({ type: 'error', text: copy.createError })
    },
  })

  const handleCreate = useCallback(() => {
    const trimmedName = newDbName.trim()
    if (!trimmedName) {
      setCreateStatus({ type: 'error', text: copy.createMissingName })
      return
    }
    if (!adminToken) {
      setCreateStatus({ type: 'error', text: copy.authRequired })
      return
    }
    if (!adminMode) {
      setCreateStatus({ type: 'error', text: copy.adminModeRequired })
      return
    }

    setCreateStatus(null)
    createMutation.mutate({
      name: trimmedName,
      description: newDbDescription.trim() || undefined,
    })
  }, [adminMode, adminToken, copy.adminModeRequired, copy.authRequired, copy.createMissingName, createMutation, newDbDescription, newDbName])

  const deleteMutation = useMutation({
    mutationFn: async (vars: { dbName: string; reason: string }) => {
      const expectedSeq = await getDatabaseExpectedSeq(requestContext, vars.dbName)
      try {
        return await deleteDatabase(requestContext, vars.dbName, expectedSeq, {
          'X-Change-Reason': vars.reason,
        })
      } catch (error) {
        if (classifyError(error).kind === 'OCC_CONFLICT') {
          const retrySeq = await getDatabaseExpectedSeq(requestContext, vars.dbName)
          return await deleteDatabase(requestContext, vars.dbName, retrySeq, {
            'X-Change-Reason': vars.reason,
          })
        }
        throw error
      }
    },
    onSuccess: (result, variables) => {
      if (result.status === 202) {
        setDeleteStatus({ type: 'info', text: copy.deleteAccepted })
      } else {
        setDeleteStatus({ type: 'success', text: `${copy.deleteSuccess} ${variables.dbName}` })
      }

      if (result.commandId) {
        trackCommand({
          id: result.commandId,
          kind: 'DELETE_DATABASE',
          target: { dbName: variables.dbName },
          context: { project: variables.dbName, branch: context.branch },
          submittedAt: new Date().toISOString(),
          writePhase: 'SUBMITTED',
          indexPhase: 'UNKNOWN',
          title: `${copy.deleteLabel}: ${variables.dbName}`,
        })
      } else {
        void queryClient.invalidateQueries({ queryKey: qk.databases(context.language) })
      }

      if (variables.dbName === context.project) {
        setProject(null)
      }

      setDeleteOpen(false)
    },
    onError: (error: unknown) => {
      const classified = classifyError(error)
      if (classified.kind === 'AUTH') {
        setDeleteStatus({ type: 'error', text: copy.authRequired })
        return
      }
      if (classified.kind === 'OCC_CONFLICT') {
        const detail = (classified.detail ?? {}) as {
          detail?: { expected_seq?: number; actual_seq?: number }
          expected_seq?: number
          actual_seq?: number
        }
        const expected = detail?.detail?.expected_seq ?? detail?.expected_seq
        const actual = detail?.detail?.actual_seq ?? detail?.actual_seq
        const suffix =
          expected !== undefined && actual !== undefined ? ` (expected ${expected}, actual ${actual})` : ''
        setDeleteStatus({ type: 'error', text: `${copy.deleteConflict}${suffix}` })
        return
      }

      console.error('Failed to delete project', error)
      setDeleteStatus({ type: 'error', text: copy.deleteError })
    },
  })

  const handleDelete = useCallback(
    ({ reason }: { reason: string }) => {
      if (!selectedDb) {
        setDeleteStatus({ type: 'error', text: copy.deleteMissing })
        return
      }
      if (!adminToken) {
        setDeleteStatus({ type: 'error', text: copy.authRequired })
        return
      }
      if (!adminMode) {
        setDeleteStatus({ type: 'error', text: copy.adminModeRequired })
        return
      }

      setDeleteStatus(null)
      deleteMutation.mutate({ dbName: selectedDb, reason })
    },
    [adminMode, adminToken, copy.adminModeRequired, copy.authRequired, copy.deleteMissing, deleteMutation, selectedDb],
  )

  const commandGroups = useMemo(() => {
    const list = Object.values(commands)
    const currentKey = `${context.project ?? ''}::${context.branch}`
    const current = list.filter((cmd) => `${cmd.context.project ?? ''}::${cmd.context.branch}` === currentKey)
    const other = list.filter((cmd) => `${cmd.context.project ?? ''}::${cmd.context.branch}` !== currentKey)
    const activeCount = (items: typeof list) =>
      items.filter((cmd) => cmd.writePhase === 'SUBMITTED' || cmd.indexPhase !== 'VISIBLE_IN_SEARCH').length
    return { currentActive: activeCount(current), otherActive: activeCount(other) }
  }, [commands, context.branch, context.project])

  return (
    <div className="app-shell">
      <Navbar className="top-nav">
        <NavbarGroup align={Alignment.LEFT}>
          <NavbarHeading>{copy.navTitle}</NavbarHeading>
        </NavbarGroup>
      </Navbar>

      <div className="app-body">
        <SidebarRail
          items={railItems}
          settingsContent={<SettingsPopoverContent copy={copy} />}
          settingsLabel={copy.rail.settings}
          userLabel={copy.rail.user}
        />

        <aside className="sidebar-panel">
          <div className="sidebar-title">{copy.dataSourceTitle}</div>
          <div className="source-card">
            <div className="source-name">{copy.sourceName}</div>
            <div className="source-meta">{copy.sourceMeta}</div>
            <div className="source-path">{copy.sourcePath}</div>
          </div>

          <div className="sidebar-title">{copy.projectTitle}</div>
          <div className="project-card">
            <Icon icon="folder-open" />
            <div>
              <div className="project-name">{selectedDb || copy.projectEmpty}</div>
              <div className="project-meta">{copy.projectHelper}</div>
              <div className="project-meta">
                {copy.branchLabel}: {context.branch}
              </div>
            </div>
          </div>

          <div className="sidebar-title">{copy.flowTitle}</div>
          <ol className="step-list">
            {copy.steps.map((step) => (
              <li
                key={step.id}
                className={`step-item ${step.id === 1 ? 'is-active' : ''}`}
              >
                <div className="step-index">{step.id}</div>
                <div>
                  <div className="step-title">{step.title}</div>
                  <div className="step-desc">{step.description}</div>
                </div>
              </li>
            ))}
          </ol>

          <div className="sidebar-title">{copy.commandsTitle}</div>
          <div className="source-card">
            <div className="source-meta">
              {copy.commandsCurrent}: {commandGroups.currentActive}
            </div>
            <div className="source-meta">
              {copy.commandsOther}: {commandGroups.otherActive}
            </div>
          </div>
        </aside>

        <main className="main">
          <section className="page-header">
            <div>
              <H1>{copy.pageTitle}</H1>
              <Text className="page-subtitle">{copy.pageSubtitle}</Text>
            </div>
          </section>

          <div className="db-grid">
            <Card className="db-card" elevation={1}>
              <div className="card-title">
                <H5>{copy.selectTitle}</H5>
              </div>
              <FormGroup label={copy.selectLabel} helperText={copy.selectHelper}>
                <HTMLSelect
                  options={projectOptions}
                  value={selectedDb}
                  onChange={(event) => setProject(event.currentTarget.value || null)}
                />
              </FormGroup>
              <Divider />
              <div className="db-actions">
                <Button
                  icon="folder-open"
                  intent="primary"
                  loading={openMutation.isPending}
                  onClick={handleOpen}
                  disabled={!selectedDb || !canRead}
                >
                  {copy.openLabel}
                </Button>
                <Button
                  minimal
                  icon="refresh"
                  loading={databasesQuery.isFetching}
                  onClick={() => void databasesQuery.refetch()}
                  disabled={!canRead}
                >
                  {copy.refreshLabel}
                </Button>
                <Button
                  intent={Intent.DANGER}
                  icon="trash"
                  onClick={() => {
                    setDeleteStatus(null)
                    setDeleteOpen(true)
                  }}
                  disabled={!selectedDb || !canWrite}
                >
                  {copy.deleteLabel}
                </Button>
              </div>
              <Text className="muted small">{copy.selectNote}</Text>
              {dbError ? <Text className="small error">{dbError}</Text> : null}
              {selectStatus ? (
                <Text className={`small status-message ${selectStatus.type}`}>
                  {selectStatus.text}
                </Text>
              ) : null}
              {deleteStatus ? (
                <Text className={`small status-message ${deleteStatus.type}`}>
                  {deleteStatus.text}
                </Text>
              ) : null}
            </Card>

            <Card className="db-card" elevation={1}>
              <div className="card-title">
                <H5>{copy.createTitle}</H5>
              </div>
              <FormGroup label={copy.nameLabel} helperText={copy.nameHelper}>
                <InputGroup
                  placeholder={copy.namePlaceholder}
                  value={newDbName}
                  onChange={(event) => setNewDbName(event.currentTarget.value)}
                />
              </FormGroup>
              <FormGroup label={copy.descriptionLabel}>
                <TextArea
                  fill
                  placeholder={copy.descriptionPlaceholder}
                  value={newDbDescription}
                  onChange={(event) => setNewDbDescription(event.currentTarget.value)}
                />
              </FormGroup>
              <div className="db-actions">
                <Button
                  icon="folder-new"
                  intent="primary"
                  loading={createMutation.isPending}
                  onClick={handleCreate}
                  disabled={!canWrite}
                >
                  {copy.createLabel}
                </Button>
                <Button minimal icon="code-block">
                  {copy.openapiLabel}
                </Button>
              </div>
              <Text className="muted small">{copy.createNote}</Text>
              {createStatus ? (
                <Text className={`small status-message ${createStatus.type}`}>
                  {createStatus.text}
                </Text>
              ) : null}
            </Card>
          </div>
        </main>
      </div>

      <DangerConfirmDialog
        isOpen={deleteOpen}
        title={copy.deleteConfirmTitle}
        description={copy.deleteConfirmBody}
        confirmLabel={copy.deleteConfirmAction}
        cancelLabel={copy.deleteCancelAction}
        confirmTextToType={selectedDb}
        reasonLabel={copy.changeReasonLabel}
        reasonPlaceholder={copy.changeReasonPlaceholder}
        typedLabel={copy.typedConfirmLabel}
        typedPlaceholder={selectedDb || copy.projectEmpty}
        footerHint={copy.dangerConfirmHint}
        onCancel={() => setDeleteOpen(false)}
        onConfirm={handleDelete}
        loading={deleteMutation.isPending}
      />
    </div>
  )
}

export default App
