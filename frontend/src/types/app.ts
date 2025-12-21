export type Language = 'en' | 'ko'
export type Theme = 'light' | 'dark'

export type AppContext = {
  project: string | null
  branch: string
  language: Language
}

export type RecentContext = {
  lastDb: string | null
  lastBranchByDb: Record<string, string>
}

export const DEFAULT_CONTEXT: AppContext = {
  project: null,
  branch: 'main',
  language: 'ko',
}

export const DEFAULT_RECENT_CONTEXT: RecentContext = {
  lastDb: null,
  lastBranchByDb: {},
}

export const DEFAULT_THEME: Theme = 'light'

export const URL_CONTEXT_KEYS = {
  project: 'project',
  branch: 'branch',
  language: 'lang',
} as const
