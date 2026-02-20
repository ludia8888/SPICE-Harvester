import { useEffect } from 'react'
import { useAppStore } from '../store/useAppStore'

export const AppBootstrap = () => {
  const syncContextFromUrl = useAppStore((state) => state.syncContextFromUrl)
  const theme = useAppStore((state) => state.theme)

  useEffect(() => {
    syncContextFromUrl()
    const handler = () => syncContextFromUrl()
    window.addEventListener('spice:urlchange', handler)
    window.addEventListener('popstate', handler)
    return () => {
      window.removeEventListener('spice:urlchange', handler)
      window.removeEventListener('popstate', handler)
    }
  }, [syncContextFromUrl])

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme)
    document.documentElement.classList.toggle('bp6-dark', theme === 'dark')
  }, [theme])

  return null
}
