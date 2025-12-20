import { useSyncExternalStore } from 'react'
import { readPathname, subscribePathname } from './pathname'

export const usePathname = () =>
  useSyncExternalStore(subscribePathname, readPathname, () => '/')
