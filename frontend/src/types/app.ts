export type Language = 'en' | 'ko'
export type Theme = 'light' | 'dark'

export type AppContext = {
  project: string | null
  branch: string
  language: Language
}

export const DEFAULT_CONTEXT: AppContext = {
  project: null,
  branch: 'main',
  language: 'en',
}

export const DEFAULT_THEME: Theme = 'light'
