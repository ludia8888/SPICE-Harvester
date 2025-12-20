export type PathUpdateMode = 'replace' | 'push'

export const readPathname = () => {
  if (typeof window === 'undefined') {
    return '/'
  }
  return window.location.pathname || '/'
}

export const navigate = (pathname: string, mode: PathUpdateMode = 'push') => {
  if (typeof window === 'undefined') {
    return
  }
  const url = new URL(window.location.href)
  url.pathname = pathname
  const action = mode === 'push' ? window.history.pushState : window.history.replaceState
  action.call(window.history, {}, '', url)
}

export const navigateWithSearch = (
  pathname: string,
  params: Record<string, string | number | boolean | null | undefined>,
  mode: PathUpdateMode = 'push',
) => {
  if (typeof window === 'undefined') {
    return
  }
  const url = new URL(window.location.href)
  url.pathname = pathname
  Object.entries(params).forEach(([key, value]) => {
    if (value === null || value === undefined || value === '') {
      return
    }
    url.searchParams.set(key, String(value))
  })
  const action = mode === 'push' ? window.history.pushState : window.history.replaceState
  action.call(window.history, {}, '', url)
}

export const subscribePathname = (listener: () => void) => {
  if (typeof window === 'undefined') {
    return () => {}
  }
  const wrapped = () => listener()
  window.addEventListener('spice:urlchange', wrapped)
  return () => window.removeEventListener('spice:urlchange', wrapped)
}
