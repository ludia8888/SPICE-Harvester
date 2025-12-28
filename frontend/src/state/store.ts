import { create } from 'zustand'

export type NavKey = 'home' | 'datasets' | 'connectors' | 'pipeline' | 'ontology' | 'workshop'

type AppState = {
  activeNav: NavKey
  setActiveNav: (nav: NavKey) => void
}

export const useAppStore = create<AppState>((set) => ({
  activeNav: 'home',
  setActiveNav: (nav) => set({ activeNav: nav }),
}))
