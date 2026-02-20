import type { Language } from '../types/app'

export type UrlContext = {
  project: string | null
  branch: string | null
  language: Language | null
}

type WriteMode = 'replace' | 'push'

const URL_CHANGE_EVENT = 'spice:urlchange'

const coerceLanguage = (value: string | null): Language | null => {
  if (value === 'en' || value === 'ko') {
    return value
  }
  return null
}

export const readUrlContext = (): UrlContext => {
  if (typeof window === 'undefined') {
    return { project: null, branch: null, language: null }
  }
  const url = new URL(window.location.href)
  const segments = url.pathname.split('/').filter(Boolean)
  const project =
    segments[0] === 'db' && segments[1] ? decodeURIComponent(segments[1]) : null
  const branch = url.searchParams.get('branch')
  const language = coerceLanguage(url.searchParams.get('lang'))
  return { project, branch, language }
}

const buildProjectPath = (currentPath: string, project: string) => {
  const segments = currentPath.split('/').filter(Boolean)
  if (segments[0] === 'db' && segments[1]) {
    const rest = segments.slice(2).join('/')
    return `/db/${encodeURIComponent(project)}${rest ? `/${rest}` : '/overview'}`
  }
  return `/db/${encodeURIComponent(project)}/overview`
}

export const writeUrlContext = (
  patch: Partial<UrlContext>,
  mode: WriteMode = 'replace',
) => {
  if (typeof window === 'undefined') {
    return
  }
  const current = readUrlContext()
  const next: UrlContext = {
    project: patch.project !== undefined ? patch.project : current.project,
    branch: patch.branch !== undefined ? patch.branch : current.branch,
    language: patch.language !== undefined ? patch.language : current.language,
  }

  const url = new URL(window.location.href)
  if (next.project) {
    url.pathname = buildProjectPath(url.pathname, next.project)
  } else if (patch.project === null) {
    url.pathname = '/'
  }

  if (next.branch) {
    url.searchParams.set('branch', next.branch)
  } else {
    url.searchParams.delete('branch')
  }
  if (next.language) {
    url.searchParams.set('lang', next.language)
  } else {
    url.searchParams.delete('lang')
  }

  const action = mode === 'push' ? window.history.pushState : window.history.replaceState
  action.call(window.history, {}, '', url)
  window.dispatchEvent(new Event(URL_CHANGE_EVENT))
}

export const subscribeUrlContext = (listener: () => void) => {
  if (typeof window === 'undefined') {
    return () => {}
  }
  const wrapped = () => listener()
  window.addEventListener(URL_CHANGE_EVENT, wrapped)
  window.addEventListener('popstate', wrapped)
  return () => {
    window.removeEventListener(URL_CHANGE_EVENT, wrapped)
    window.removeEventListener('popstate', wrapped)
  }
}
