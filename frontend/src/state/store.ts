import { create } from 'zustand'

export type NavKey = 'home' | 'datasets' | 'connectors' | 'pipeline' | 'ontology' | 'ai-agent' | 'workshop'

export type PipelineContext = {
  folderId: string
  folderName: string
}

type AppState = {
  activeNav: NavKey
  setActiveNav: (nav: NavKey) => void
  pipelineContext: PipelineContext | null
  setPipelineContext: (context: PipelineContext | null) => void
}

export const useAppStore = create<AppState>((set) => ({
  activeNav: 'home',
  setActiveNav: (nav) => set({ activeNav: nav }),
  pipelineContext: null,
  setPipelineContext: (pipelineContext) => set({ pipelineContext }),
}))
