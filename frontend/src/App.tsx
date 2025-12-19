import { useCallback, useEffect, useState } from 'react'
import {
  Alignment,
  Alert,
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
  Popover,
  Position,
  Text,
  TextArea,
  Intent,
} from '@blueprintjs/core'
import './App.css'

const fallbackDatabases = ['demo_db', 'supply_chain', 'energy_ops']
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '/api/v1'
type Language = 'en' | 'ko'
type StatusMessage = { type: 'success' | 'error' | 'info'; text: string }

const PROJECT_QUERY_KEY = 'project'
const STORAGE_KEYS = {
  project: 'spice.project',
  adminToken: 'spice.adminToken',
}

const buildApiUrl = (path: string, language: Language) => {
  const normalizedPath = path.replace(/^\/+/, '')
  const base = API_BASE_URL.replace(/\/+$/, '')
  const url = base.startsWith('http')
    ? new URL(`${base}/${normalizedPath}`)
    : new URL(`${base}/${normalizedPath}`, window.location.origin)
  url.searchParams.set('lang', language)
  return url.toString()
}

const buildHeaders = (language: Language, adminToken: string, json = false) => {
  const headers = new Headers({ 'Accept-Language': language })
  if (adminToken) {
    headers.set('X-Admin-Token', adminToken)
  }
  if (json) {
    headers.set('Content-Type', 'application/json')
  }
  return headers
}

const readPersistedProject = () => {
  if (typeof window === 'undefined') {
    return null
  }

  const params = new URLSearchParams(window.location.search)
  const fromUrl = params.get(PROJECT_QUERY_KEY)
  if (fromUrl) {
    return fromUrl
  }

  try {
    return localStorage.getItem(STORAGE_KEYS.project)
  } catch {
    return null
  }
}

const readPersistedAdminToken = () => {
  const fromEnv = import.meta.env.VITE_ADMIN_TOKEN
  if (fromEnv) {
    return fromEnv
  }
  if (typeof window === 'undefined') {
    return null
  }
  try {
    return localStorage.getItem(STORAGE_KEYS.adminToken)
  } catch {
    return null
  }
}

const syncProjectSelection = (project: string | null) => {
  if (typeof window === 'undefined') {
    return
  }

  const url = new URL(window.location.href)
  if (project) {
    url.searchParams.set(PROJECT_QUERY_KEY, project)
    try {
      localStorage.setItem(STORAGE_KEYS.project, project)
    } catch (error) {
      void error
    }
  } else {
    url.searchParams.delete(PROJECT_QUERY_KEY)
    try {
      localStorage.removeItem(STORAGE_KEYS.project)
    } catch (error) {
      void error
    }
  }
  window.history.replaceState({}, '', url)
}
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
    languageOptions: [
      { label: 'English', value: 'en' },
      { label: '한국어', value: 'ko' },
    ],
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
    languageOptions: [
      { label: '한국어', value: 'ko' },
      { label: 'English', value: 'en' },
    ],
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
  },
} as const

function App() {
  const [selectedDb, setSelectedDb] = useState(
    () => readPersistedProject() ?? fallbackDatabases[0],
  )
  const [newDbName, setNewDbName] = useState('')
  const [newDbDescription, setNewDbDescription] = useState('')
  const [language, setLanguage] = useState<Language>('ko')
  const [adminToken, setAdminToken] = useState(() => readPersistedAdminToken() ?? '')
  const [databases, setDatabases] = useState<string[]>(fallbackDatabases)
  const [dbLoading, setDbLoading] = useState(false)
  const [dbError, setDbError] = useState<string | null>(null)
  const [openLoading, setOpenLoading] = useState(false)
  const [deleteOpen, setDeleteOpen] = useState(false)
  const [deleteLoading, setDeleteLoading] = useState(false)
  const [deleteExpectedSeq, setDeleteExpectedSeq] = useState('1')
  const [deleteStatus, setDeleteStatus] = useState<StatusMessage | null>(null)
  const [createLoading, setCreateLoading] = useState(false)
  const [selectStatus, setSelectStatus] = useState<StatusMessage | null>(null)
  const [createStatus, setCreateStatus] = useState<StatusMessage | null>(null)
  const copy = copyByLang[language]
  const railItems = [
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
  const loadDatabases = useCallback(async () => {
    setDbLoading(true)
    setDbError(null)

    try {
      const response = await fetch(buildApiUrl('databases', language), {
        headers: buildHeaders(language, adminToken),
      })

      if (response.status === 401) {
        setDbError(copy.authRequired)
        return
      }

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`)
      }

      const payload = (await response.json()) as {
        data?: { databases?: Array<{ name?: string }> }
      }

      const names =
        payload?.data?.databases
          ?.map((db) => db?.name)
          .filter((name): name is string => Boolean(name)) ?? []

      setDatabases(names)
      setSelectedDb((current) => (names.includes(current) ? current : names[0] ?? ''))
    } catch (error) {
      console.error('Failed to load projects', error)
      setDbError(copy.loadError)
    } finally {
      setDbLoading(false)
    }
  }, [adminToken, copy.authRequired, copy.loadError, language])

  useEffect(() => {
    void loadDatabases()
  }, [loadDatabases])

  useEffect(() => {
    syncProjectSelection(selectedDb || null)
  }, [selectedDb])

  useEffect(() => {
    if (typeof window === 'undefined') {
      return
    }
    try {
      if (adminToken) {
        localStorage.setItem(STORAGE_KEYS.adminToken, adminToken)
      } else {
        localStorage.removeItem(STORAGE_KEYS.adminToken)
      }
    } catch (error) {
      void error
    }
  }, [adminToken])

  const handleOpen = useCallback(async () => {
    if (!selectedDb) {
      setSelectStatus({ type: 'error', text: copy.openMissing })
      return
    }

    setOpenLoading(true)
    setSelectStatus(null)

    try {
      const response = await fetch(
        buildApiUrl(`databases/${encodeURIComponent(selectedDb)}`, language),
        {
          headers: buildHeaders(language, adminToken),
        },
      )

      if (response.status === 401) {
        setSelectStatus({ type: 'error', text: copy.authRequired })
        return
      }

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`)
      }

      setSelectStatus({ type: 'success', text: `${copy.openSuccess} ${selectedDb}` })
    } catch (error) {
      console.error('Failed to open project', error)
      setSelectStatus({ type: 'error', text: copy.openError })
    } finally {
      setOpenLoading(false)
    }
  }, [
    adminToken,
    copy.authRequired,
    copy.openError,
    copy.openMissing,
    copy.openSuccess,
    language,
    selectedDb,
  ])

  const handleCreate = useCallback(async () => {
    const trimmedName = newDbName.trim()
    if (!trimmedName) {
      setCreateStatus({ type: 'error', text: copy.createMissingName })
      return
    }

    setCreateLoading(true)
    setCreateStatus(null)

    const payload = {
      name: trimmedName,
      description: newDbDescription.trim() || undefined,
    }

    try {
      const response = await fetch(buildApiUrl('databases', language), {
        method: 'POST',
        headers: buildHeaders(language, adminToken, true),
        body: JSON.stringify(payload),
      })

      if (response.status === 401) {
        setCreateStatus({ type: 'error', text: copy.authRequired })
        return
      }

      let data: { data?: { command_id?: string }; command_id?: string } | null = null
      try {
        data = (await response.json()) as {
          data?: { command_id?: string }
          command_id?: string
        }
      } catch (error) {
        void error
      }

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`)
      }

      const commandId = data?.data?.command_id ?? data?.command_id
      if (response.status === 202) {
        const suffix = commandId ? ` ${copy.commandLabel} ${commandId}` : ''
        setCreateStatus({ type: 'info', text: `${copy.createAccepted}${suffix}` })
      } else {
        setCreateStatus({ type: 'success', text: `${copy.createSuccess} ${trimmedName}` })
      }

      setNewDbName('')
      setNewDbDescription('')
      void loadDatabases()
    } catch (error) {
      console.error('Failed to create project', error)
      setCreateStatus({ type: 'error', text: copy.createError })
    } finally {
      setCreateLoading(false)
    }
  }, [
    adminToken,
    copy.authRequired,
    copy.commandLabel,
    copy.createAccepted,
    copy.createError,
    copy.createMissingName,
    copy.createSuccess,
    language,
    loadDatabases,
    newDbDescription,
    newDbName,
  ])

  const handleDelete = useCallback(async () => {
    if (!selectedDb) {
      setDeleteStatus({ type: 'error', text: copy.deleteMissing })
      return
    }

    const expectedSeq = Number(deleteExpectedSeq)
    if (!Number.isFinite(expectedSeq) || expectedSeq < 0) {
      setDeleteStatus({ type: 'error', text: copy.deleteInvalidSeq })
      return
    }

    setDeleteLoading(true)
    setDeleteStatus(null)

    try {
      const url = new URL(
        buildApiUrl(`databases/${encodeURIComponent(selectedDb)}`, language),
      )
      url.searchParams.set('expected_seq', String(expectedSeq))

      const response = await fetch(url.toString(), {
        method: 'DELETE',
        headers: buildHeaders(language, adminToken),
      })

      if (response.status === 401) {
        setDeleteStatus({ type: 'error', text: copy.authRequired })
        return
      }

      let detail: unknown = null
      try {
        detail = await response.json()
      } catch (error) {
        void error
      }

      if (response.status === 409) {
        const conflict = detail as { detail?: { expected_seq?: number; actual_seq?: number } }
        const expected = conflict?.detail?.expected_seq
        const actual = conflict?.detail?.actual_seq
        const suffix =
          expected !== undefined && actual !== undefined
            ? ` (expected ${expected}, actual ${actual})`
            : ''
        setDeleteStatus({ type: 'error', text: `${copy.deleteConflict}${suffix}` })
        return
      }

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`)
      }

      if (response.status === 202) {
        setDeleteStatus({ type: 'info', text: copy.deleteAccepted })
      } else {
        setDeleteStatus({ type: 'success', text: `${copy.deleteSuccess} ${selectedDb}` })
      }

      void loadDatabases()
      setDeleteOpen(false)
    } catch (error) {
      console.error('Failed to delete project', error)
      setDeleteStatus({ type: 'error', text: copy.deleteError })
    } finally {
      setDeleteLoading(false)
    }
  }, [
    adminToken,
    copy.authRequired,
    copy.deleteAccepted,
    copy.deleteConflict,
    copy.deleteError,
    copy.deleteInvalidSeq,
    copy.deleteMissing,
    copy.deleteSuccess,
    deleteExpectedSeq,
    language,
    loadDatabases,
    selectedDb,
  ])

  return (
    <div className="app-shell">
      <Navbar className="top-nav">
        <NavbarGroup align={Alignment.LEFT}>
          <NavbarHeading>{copy.navTitle}</NavbarHeading>
        </NavbarGroup>
      </Navbar>

      <div className="app-body">
        <aside className="sidebar-rail">
          {railItems.map((item) => (
            <Button
              key={item.label}
              minimal
              icon={item.icon}
              className={`rail-button ${item.active ? 'is-active' : ''}`}
              aria-label={item.label}
              title={item.label}
            />
          ))}
          <div className="rail-spacer" />
          <Popover
            content={
              <Card className="settings-popover" elevation={2}>
                <div className="settings-title">{copy.settingsTitle}</div>
                <FormGroup label={copy.languageLabel} helperText={copy.languageHelper}>
                  <HTMLSelect
                    options={copy.languageOptions}
                    value={language}
                    onChange={(event) => setLanguage(event.currentTarget.value as 'en' | 'ko')}
                  />
                </FormGroup>
                <FormGroup label={copy.tokenLabel} helperText={copy.tokenHelper}>
                  <InputGroup
                    type="password"
                    placeholder={copy.tokenPlaceholder}
                    value={adminToken}
                    onChange={(event) => setAdminToken(event.currentTarget.value)}
                  />
                </FormGroup>
              </Card>
            }
            position={Position.RIGHT}
          >
            <Button
              minimal
              icon="cog"
              className="rail-button"
              aria-label={copy.rail.settings}
              title={copy.rail.settings}
            />
          </Popover>
          <Button
            minimal
            icon="user"
            className="rail-button"
            aria-label={copy.rail.user}
            title={copy.rail.user}
          />
        </aside>

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
                  options={databases}
                  value={selectedDb}
                  onChange={(event) => setSelectedDb(event.currentTarget.value)}
                />
              </FormGroup>
              <Divider />
              <div className="db-actions">
                <Button icon="folder-open" intent="primary" loading={openLoading} onClick={handleOpen}>
                  {copy.openLabel}
                </Button>
                <Button minimal icon="refresh" loading={dbLoading} onClick={loadDatabases}>
                  {copy.refreshLabel}
                </Button>
                <Button
                  intent={Intent.DANGER}
                  icon="trash"
                  onClick={() => {
                    setDeleteStatus(null)
                    setDeleteOpen(true)
                  }}
                  disabled={!selectedDb}
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
                  loading={createLoading}
                  onClick={handleCreate}
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

      <Alert
        isOpen={deleteOpen}
        intent={Intent.DANGER}
        icon="trash"
        confirmButtonText={copy.deleteConfirmAction}
        cancelButtonText={copy.deleteCancelAction}
        onConfirm={handleDelete}
        onCancel={() => setDeleteOpen(false)}
        canEscapeKeyCancel
        canOutsideClickCancel
        loading={deleteLoading}
      >
        <Text className="alert-title">{copy.deleteConfirmTitle}</Text>
        <Text className="muted">{copy.deleteConfirmBody}</Text>
        <FormGroup label={copy.expectedSeqLabel} helperText={copy.expectedSeqHelper}>
          <InputGroup
            placeholder={copy.expectedSeqPlaceholder}
            value={deleteExpectedSeq}
            onChange={(event) => setDeleteExpectedSeq(event.currentTarget.value)}
          />
        </FormGroup>
        {deleteStatus ? (
          <Text className={`small status-message ${deleteStatus.type}`}>
            {deleteStatus.text}
          </Text>
        ) : null}
      </Alert>
    </div>
  )
}

export default App
