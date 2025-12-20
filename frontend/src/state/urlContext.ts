import { DEFAULT_CONTEXT, type AppContext, type Language, URL_CONTEXT_KEYS } from '../types/app'

export type UrlUpdateMode = 'replace' | 'push'

const LANG_VALUES: Language[] = ['en', 'ko']

const normalizeLanguage = (value: string | null): Language | null => {
  if (!value) {
    return null
  }
  return (LANG_VALUES as string[]).includes(value) ? (value as Language) : null
}

const normalizeOptionalString = (value: string | null): string | null => {
  if (!value) {
    return null
  }
  const trimmed = value.trim()
  return trimmed ? trimmed : null
}

export const readUrlContext = (): AppContext => {
  if (typeof window === 'undefined') {
    return DEFAULT_CONTEXT
  }

  const url = new URL(window.location.href)
  const language = normalizeLanguage(url.searchParams.get(URL_CONTEXT_KEYS.language))
  const project = normalizeOptionalString(url.searchParams.get(URL_CONTEXT_KEYS.project))
  const branch = normalizeOptionalString(url.searchParams.get(URL_CONTEXT_KEYS.branch))

  return {
    project,
    branch: branch ?? DEFAULT_CONTEXT.branch,
    language: language ?? DEFAULT_CONTEXT.language,
  }
}

export const writeUrlContext = (
  partial: Partial<AppContext>,
  mode: UrlUpdateMode = 'replace',
) => {
  if (typeof window === 'undefined') {
    return
  }

  const current = readUrlContext()
  const next: AppContext = {
    project:
      partial.project === undefined
        ? current.project
        : normalizeOptionalString(partial.project),
    branch:
      partial.branch === undefined
        ? current.branch
        : normalizeOptionalString(partial.branch) ?? DEFAULT_CONTEXT.branch,
    language:
      partial.language === undefined
        ? current.language
        : partial.language ?? DEFAULT_CONTEXT.language,
  }

  const url = new URL(window.location.href)
  if (next.project) {
    url.searchParams.set(URL_CONTEXT_KEYS.project, next.project)
  } else {
    url.searchParams.delete(URL_CONTEXT_KEYS.project)
  }
  if (next.branch) {
    url.searchParams.set(URL_CONTEXT_KEYS.branch, next.branch)
  } else {
    url.searchParams.delete(URL_CONTEXT_KEYS.branch)
  }
  if (next.language) {
    url.searchParams.set(URL_CONTEXT_KEYS.language, next.language)
  } else {
    url.searchParams.delete(URL_CONTEXT_KEYS.language)
  }

  const action = mode === 'push' ? window.history.pushState : window.history.replaceState
  action.call(window.history, {}, '', url)
}

const URL_CHANGE_EVENT = 'spice:urlchange'
const patchHistory = (() => {
  let patched = false
  return () => {
    if (patched || typeof window === 'undefined') {
      return
    }

    patched = true

    const dispatch = () => {
      window.dispatchEvent(new Event(URL_CHANGE_EVENT))
    }

    const originalPush = window.history.pushState.bind(window.history)
    window.history.pushState = (...args) => {
      originalPush(...args)
      dispatch()
    }

    const originalReplace = window.history.replaceState.bind(window.history)
    window.history.replaceState = (...args) => {
      originalReplace(...args)
      dispatch()
    }

    window.addEventListener('popstate', dispatch)
  }
})()

export const subscribeUrlContext = (listener: () => void) => {
  if (typeof window === 'undefined') {
    return () => {}
  }

  patchHistory()

  const wrapped = () => listener()
  window.addEventListener(URL_CHANGE_EVENT, wrapped)
  return () => window.removeEventListener(URL_CHANGE_EVENT, wrapped)
}

