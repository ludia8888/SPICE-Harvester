import { create } from 'zustand'
import { readUrlContext, subscribeUrlContext, writeUrlContext } from '../state/urlContext'
import {
  DEFAULT_CONTEXT,
  DEFAULT_RECENT_CONTEXT,
  DEFAULT_THEME,
  type AppContext,
  type Language,
  type RecentContext,
  type Theme,
} from '../types/app'

const STORAGE_KEYS = {
  project: 'spice.project',
  branch: 'spice.branch',
  language: 'spice.language',
  theme: 'spice.theme',
  rememberToken: 'spice.rememberToken',
  authToken: 'spice.authToken',
  adminToken: 'spice.adminToken',
  recentContext: 'spice.recentContext',
  commands: 'commandTracker.items',
} as const

export type CommandKind =
  | 'CREATE_DATABASE'
  | 'DELETE_DATABASE'
  | 'CREATE_BRANCH'
  | 'DELETE_BRANCH'
  | 'ONTOLOGY_APPLY'
  | 'ONTOLOGY_DELETE'
  | 'INSTANCE_WRITE'
  | 'IMPORT'
  | 'MAPPINGS_IMPORT'
  | 'MERGE_RESOLVE'
  | 'ADMIN_TASK'
  | 'UNKNOWN'

export type WritePhase = 'SUBMITTED' | 'WRITE_DONE' | 'FAILED' | 'CANCELLED'
export type IndexPhase = 'UNKNOWN' | 'INDEXING_PENDING' | 'VISIBLE_IN_SEARCH'

export type TrackedCommand = {
  id: string
  kind: CommandKind
  title?: string
  source?: string
  target: {
    dbName: string
  }
  context: Pick<AppContext, 'project' | 'branch'>
  submittedAt: string
  writePhase: WritePhase
  indexPhase: IndexPhase
  status?: string
  error?: string | null
  expired?: boolean
}

export type InspectorState = {
  title: string
  subtitle?: string
  data?: unknown
  kind?: 'Class' | 'Instance' | 'GraphNode' | 'GraphEdge' | 'AuditItem' | 'Command'
  auditCommandId?: string
  lineageRootId?: string
} | null

export type SettingsDialogReason =
  | 'TOKEN_MISSING'
  | 'UNAUTHORIZED'
  | 'FORBIDDEN'
  | 'SERVICE_UNAVAILABLE'

type TrackedCommandInput = {
  id: string
  kind?: CommandKind
  title?: string
  source?: string
  targetDbName?: string
  context?: Partial<Pick<AppContext, 'project' | 'branch'>>
}

type AppState = {
  context: AppContext
  theme: Theme
  authToken: string
  adminToken: string
  rememberToken: boolean
  adminMode: boolean
  commands: Record<string, TrackedCommand>
  inspector: InspectorState
  settingsDialogOpen: boolean
  settingsDialogReason: SettingsDialogReason | null
  commandDrawerOpen: boolean
  commandDrawerTargetId: string | null
  syncContextFromUrl: () => void
  setTheme: (theme: Theme) => void
  setProject: (project: string | null) => void
  setBranch: (branch: string) => void
  setLanguage: (language: Language) => void
  setAuthToken: (token: string) => void
  setAdminToken: (token: string) => void
  setRememberToken: (remember: boolean) => void
  setAdminMode: (enabled: boolean) => void
  trackCommand: (command: TrackedCommand) => void
  trackCommands: (commands: TrackedCommandInput[]) => void
  patchCommand: (commandId: string, patch: Partial<TrackedCommand>) => void
  removeCommand: (commandId: string) => void
  setInspector: (inspector: InspectorState) => void
  clearInspector: () => void
  openSettingsDialog: (reason?: SettingsDialogReason) => void
  closeSettingsDialog: () => void
  openCommandDrawer: (commandId?: string) => void
  closeCommandDrawer: () => void
  clearCommandDrawerTarget: () => void
}

const safeLocalStorageGet = (key: string) => {
  if (typeof window === 'undefined') {
    return null
  }
  try {
    return localStorage.getItem(key)
  } catch {
    return null
  }
}

const safeLocalStorageSet = (key: string, value: string) => {
  if (typeof window === 'undefined') {
    return
  }
  try {
    localStorage.setItem(key, value)
  } catch (error) {
    void error
  }
}

const safeLocalStorageRemove = (key: string) => {
  if (typeof window === 'undefined') {
    return
  }
  try {
    localStorage.removeItem(key)
  } catch (error) {
    void error
  }
}

const normalizeCommand = (value: unknown): TrackedCommand | null => {
  if (!value || typeof value !== 'object') {
    return null
  }
  const item = value as Partial<TrackedCommand>
  if (typeof item.id !== 'string' || !item.id.trim()) {
    return null
  }
  const contextProject =
    typeof item.context?.project === 'string' ? item.context.project : null
  const contextBranch =
    typeof item.context?.branch === 'string' && item.context.branch.trim()
      ? item.context.branch
      : DEFAULT_CONTEXT.branch
  const target =
    item.target && typeof item.target.dbName === 'string'
      ? item.target
      : { dbName: contextProject ?? '' }

  return {
    id: item.id,
    kind: item.kind ?? 'UNKNOWN',
    title: item.title,
    source: item.source,
    target,
    context: { project: contextProject, branch: contextBranch },
    submittedAt:
      typeof item.submittedAt === 'string' ? item.submittedAt : new Date().toISOString(),
    writePhase: item.writePhase ?? 'SUBMITTED',
    indexPhase: item.indexPhase ?? 'UNKNOWN',
    status: item.status,
    error: item.error ?? null,
    expired: item.expired ?? false,
  }
}

const readCachedCommands = (): Record<string, TrackedCommand> => {
  const raw = safeLocalStorageGet(STORAGE_KEYS.commands)
  if (!raw) {
    return {}
  }
  try {
    const parsed = JSON.parse(raw)
    if (!Array.isArray(parsed)) {
      return {}
    }
    const entries = parsed
      .map((item) => normalizeCommand(item))
      .filter((item): item is TrackedCommand => Boolean(item))
    return Object.fromEntries(entries.map((item) => [item.id, item]))
  } catch {
    return {}
  }
}

const persistCommands = (commands: Record<string, TrackedCommand>) => {
  const items = Object.values(commands)
  if (items.length === 0) {
    safeLocalStorageRemove(STORAGE_KEYS.commands)
    return
  }
  safeLocalStorageSet(STORAGE_KEYS.commands, JSON.stringify(items))
}

const readCachedLanguage = (): Language | null => {
  const value = safeLocalStorageGet(STORAGE_KEYS.language)
  if (value === 'en' || value === 'ko') {
    return value
  }
  return null
}

const readCachedTheme = (): Theme | null => {
  const value = safeLocalStorageGet(STORAGE_KEYS.theme)
  if (value === 'light' || value === 'dark') {
    return value
  }
  return null
}

const readCachedBranch = () => {
  const value = safeLocalStorageGet(STORAGE_KEYS.branch)
  return value?.trim() ? value.trim() : null
}

const readCachedProject = () => {
  const value = safeLocalStorageGet(STORAGE_KEYS.project)
  return value?.trim() ? value.trim() : null
}

const readRememberToken = () => {
  const raw = safeLocalStorageGet(STORAGE_KEYS.rememberToken)
  if (raw === null) {
    return Boolean(
      safeLocalStorageGet(STORAGE_KEYS.adminToken) || safeLocalStorageGet(STORAGE_KEYS.authToken),
    )
  }
  return raw === 'true'
}

const readCachedAuthToken = () => safeLocalStorageGet(STORAGE_KEYS.authToken) ?? ''

const readCachedAdminToken = () => safeLocalStorageGet(STORAGE_KEYS.adminToken) ?? ''

const readCachedRecentContext = (): RecentContext => {
  const raw = safeLocalStorageGet(STORAGE_KEYS.recentContext)
  if (!raw) {
    const project = readCachedProject()
    const branch = readCachedBranch()
    return {
      lastDb: project,
      lastBranchByDb: project && branch ? { [project]: branch } : {},
    }
  }
  try {
    const parsed = JSON.parse(raw) as Partial<RecentContext>
    const lastDb = typeof parsed.lastDb === 'string' ? parsed.lastDb : null
    const lastBranchByDb =
      parsed.lastBranchByDb && typeof parsed.lastBranchByDb === 'object'
        ? (parsed.lastBranchByDb as Record<string, string>)
        : {}
    return {
      lastDb,
      lastBranchByDb,
    }
  } catch {
    return { ...DEFAULT_RECENT_CONTEXT }
  }
}

const persistRecentContext = (recent: RecentContext) => {
  safeLocalStorageSet(STORAGE_KEYS.recentContext, JSON.stringify(recent))
}

const hasExplicitBranchParam = () => {
  if (typeof window === 'undefined') {
    return false
  }
  try {
    const url = new URL(window.location.href)
    return url.searchParams.has('branch')
  } catch {
    return false
  }
}

const getInitialContext = (): AppContext => {
  const url = readUrlContext()
  const recent = readCachedRecentContext()
  const project = url.project ?? recent.lastDb ?? readCachedProject()
  const recentBranch = project ? recent.lastBranchByDb[project] : null
  const branch = hasExplicitBranchParam()
    ? url.branch
    : recentBranch || readCachedBranch() || url.branch || DEFAULT_CONTEXT.branch

  return {
    project,
    branch: branch || DEFAULT_CONTEXT.branch,
    language: url.language || readCachedLanguage() || DEFAULT_CONTEXT.language,
  }
}

const ensureUrlContext = (context: AppContext) => {
  const current = readUrlContext()
  if (
    current.project === context.project &&
    current.branch === context.branch &&
    current.language === context.language
  ) {
    return
  }
  writeUrlContext(context, 'replace')
}

const buildTrackedCommand = (input: TrackedCommandInput, context: AppContext): TrackedCommand => {
  const project = input.context?.project ?? context.project
  const branch = input.context?.branch ?? context.branch
  return {
    id: input.id,
    kind: input.kind ?? 'UNKNOWN',
    title: input.title,
    source: input.source,
    target: { dbName: input.targetDbName ?? project ?? '' },
    context: { project, branch },
    submittedAt: new Date().toISOString(),
    writePhase: 'SUBMITTED',
    indexPhase: 'UNKNOWN',
  }
}

export const useAppStore = create<AppState>((set, get) => {
  const rememberToken = readRememberToken()
  const tokenFromEnv = import.meta.env.VITE_ADMIN_TOKEN ?? ''
  const authTokenFromEnv = import.meta.env.VITE_AUTH_TOKEN ?? ''
  const authToken = authTokenFromEnv || (rememberToken ? readCachedAuthToken() : '')
  const adminToken = tokenFromEnv || (rememberToken ? readCachedAdminToken() : '')
  const context = getInitialContext()
  const theme = readCachedTheme() ?? DEFAULT_THEME

  return {
    context,
    theme,
    authToken,
    adminToken,
    rememberToken,
    adminMode: false,
    commands: readCachedCommands(),
    inspector: null,
    settingsDialogOpen: false,
    settingsDialogReason: null,
    commandDrawerOpen: false,
    commandDrawerTargetId: null,
    syncContextFromUrl: () => {
      const next = getInitialContext()
      set({ context: next })
      safeLocalStorageSet(STORAGE_KEYS.branch, next.branch)
      safeLocalStorageSet(STORAGE_KEYS.language, next.language)
      if (next.project) {
        safeLocalStorageSet(STORAGE_KEYS.project, next.project)
      } else {
        safeLocalStorageRemove(STORAGE_KEYS.project)
      }

      const recent = readCachedRecentContext()
      const updatedRecent: RecentContext = {
        lastDb: next.project ?? recent.lastDb,
        lastBranchByDb: { ...recent.lastBranchByDb },
      }
      if (next.project) {
        updatedRecent.lastDb = next.project
        updatedRecent.lastBranchByDb[next.project] = next.branch
      }
      persistRecentContext(updatedRecent)
      ensureUrlContext(next)
    },
    setTheme: (next) => {
      set({ theme: next })
      safeLocalStorageSet(STORAGE_KEYS.theme, next)
    },
    setProject: (project) => {
      const recent = readCachedRecentContext()
      const normalized = project?.trim() ? project.trim() : null
      const nextBranch = normalized
        ? recent.lastBranchByDb[normalized] ?? DEFAULT_CONTEXT.branch
        : DEFAULT_CONTEXT.branch
      writeUrlContext({ project: normalized, branch: nextBranch }, 'replace')
      get().syncContextFromUrl()
    },
    setBranch: (branch) => {
      writeUrlContext({ branch }, 'replace')
      get().syncContextFromUrl()
    },
    setLanguage: (language) => {
      writeUrlContext({ language }, 'replace')
      get().syncContextFromUrl()
    },
    setAuthToken: (token) => {
      const normalized = token.trim()
      set({ authToken: normalized })
      if (get().rememberToken) {
        safeLocalStorageSet(STORAGE_KEYS.authToken, normalized)
      }
    },
    setAdminToken: (token) => {
      const normalized = token.trim()
      set({ adminToken: normalized })
      if (get().rememberToken) {
        safeLocalStorageSet(STORAGE_KEYS.adminToken, normalized)
      }
    },
    setRememberToken: (remember) => {
      set({ rememberToken: remember })
      safeLocalStorageSet(STORAGE_KEYS.rememberToken, remember ? 'true' : 'false')
      if (remember) {
        safeLocalStorageSet(STORAGE_KEYS.adminToken, get().adminToken)
        safeLocalStorageSet(STORAGE_KEYS.authToken, get().authToken)
      } else {
        safeLocalStorageRemove(STORAGE_KEYS.adminToken)
        safeLocalStorageRemove(STORAGE_KEYS.authToken)
      }
    },
    setAdminMode: (enabled) => set({ adminMode: enabled }),
    trackCommand: (command) =>
      set((state) => {
        const next = { ...state.commands, [command.id]: command }
        persistCommands(next)
        return { commands: next }
      }),
    trackCommands: (commands) =>
      set((state) => {
        if (!commands.length) {
          return state
        }
        const next = { ...state.commands }
        commands.forEach((command) => {
          const built = buildTrackedCommand(command, state.context)
          next[built.id] = built
        })
        persistCommands(next)
        return { commands: next }
      }),
    patchCommand: (commandId, patch) =>
      set((state) => {
        if (!state.commands[commandId]) {
          return state
        }
        const next = {
          ...state.commands,
          [commandId]: { ...state.commands[commandId], ...patch },
        }
        persistCommands(next)
        return { commands: next }
      }),
    removeCommand: (commandId) =>
      set((state) => {
        if (!state.commands[commandId]) {
          return state
        }
        const next = { ...state.commands }
        delete next[commandId]
        persistCommands(next)
        return { commands: next }
      }),
    setInspector: (inspector) => set({ inspector }),
    clearInspector: () => set({ inspector: null }),
    openSettingsDialog: (reason) =>
      set({ settingsDialogOpen: true, settingsDialogReason: reason ?? null }),
    closeSettingsDialog: () => set({ settingsDialogOpen: false, settingsDialogReason: null }),
    openCommandDrawer: (commandId) =>
      set({ commandDrawerOpen: true, commandDrawerTargetId: commandId ?? null }),
    closeCommandDrawer: () => set({ commandDrawerOpen: false }),
    clearCommandDrawerTarget: () => set({ commandDrawerTargetId: null }),
  }
})

let urlSyncUnsubscribe: (() => void) | null = null

export const startUrlSync = () => {
  if (urlSyncUnsubscribe) {
    return urlSyncUnsubscribe
  }

  useAppStore.getState().syncContextFromUrl()
  urlSyncUnsubscribe = subscribeUrlContext(() => useAppStore.getState().syncContextFromUrl())
  return () => {
    urlSyncUnsubscribe?.()
    urlSyncUnsubscribe = null
  }
}
