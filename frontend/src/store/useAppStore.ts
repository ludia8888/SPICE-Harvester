import { create } from 'zustand'
import { readUrlContext, subscribeUrlContext, writeUrlContext } from '../state/urlContext'
import { DEFAULT_CONTEXT, type AppContext, type Language } from '../types/app'

const STORAGE_KEYS = {
  project: 'spice.project',
  branch: 'spice.branch',
  language: 'spice.language',
  rememberToken: 'spice.rememberToken',
  adminToken: 'spice.adminToken',
} as const

export type CommandKind = 'CREATE_DATABASE' | 'DELETE_DATABASE'
export type WritePhase = 'SUBMITTED' | 'WRITE_DONE' | 'FAILED' | 'CANCELLED'
export type IndexPhase = 'UNKNOWN' | 'INDEXING_PENDING' | 'VISIBLE_IN_SEARCH'

export type TrackedCommand = {
  id: string
  kind: CommandKind
  target: {
    dbName: string
  }
  context: Pick<AppContext, 'project' | 'branch'>
  submittedAt: string
  writePhase: WritePhase
  indexPhase: IndexPhase
  status?: string
  error?: string | null
  title?: string
}

type AppState = {
  context: AppContext
  adminToken: string
  rememberToken: boolean
  adminMode: boolean
  commands: Record<string, TrackedCommand>
  syncContextFromUrl: () => void
  setProject: (project: string | null) => void
  setBranch: (branch: string) => void
  setLanguage: (language: Language) => void
  setAdminToken: (token: string) => void
  setRememberToken: (remember: boolean) => void
  setAdminMode: (enabled: boolean) => void
  trackCommand: (command: TrackedCommand) => void
  patchCommand: (commandId: string, patch: Partial<TrackedCommand>) => void
  removeCommand: (commandId: string) => void
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

const readCachedLanguage = (): Language | null => {
  const value = safeLocalStorageGet(STORAGE_KEYS.language)
  if (value === 'en' || value === 'ko') {
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
    return Boolean(safeLocalStorageGet(STORAGE_KEYS.adminToken))
  }
  return raw === 'true'
}

const readCachedAdminToken = () => safeLocalStorageGet(STORAGE_KEYS.adminToken) ?? ''

const getInitialContext = (): AppContext => {
  const url = readUrlContext()
  return {
    project: url.project ?? readCachedProject(),
    branch: url.branch || readCachedBranch() || DEFAULT_CONTEXT.branch,
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

export const useAppStore = create<AppState>((set, get) => {
  const rememberToken = readRememberToken()
  const tokenFromEnv = import.meta.env.VITE_ADMIN_TOKEN ?? ''
  const adminToken = tokenFromEnv || (rememberToken ? readCachedAdminToken() : '')
  const context = getInitialContext()

  return {
    context,
    adminToken,
    rememberToken,
    adminMode: false,
    commands: {},
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
      ensureUrlContext(next)
    },
    setProject: (project) => {
      writeUrlContext({ project }, 'replace')
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
      } else {
        safeLocalStorageRemove(STORAGE_KEYS.adminToken)
      }
    },
    setAdminMode: (enabled) => set({ adminMode: enabled }),
    trackCommand: (command) =>
      set((state) => ({
        commands: { ...state.commands, [command.id]: command },
      })),
    patchCommand: (commandId, patch) =>
      set((state) => ({
        commands: state.commands[commandId]
          ? { ...state.commands, [commandId]: { ...state.commands[commandId], ...patch } }
          : state.commands,
      })),
    removeCommand: (commandId) =>
      set((state) => {
        if (!state.commands[commandId]) {
          return state
        }
        const next = { ...state.commands }
        delete next[commandId]
        return { commands: next }
      }),
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
