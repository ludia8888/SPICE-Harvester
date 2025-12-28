import { create } from 'zustand'

export type NavKey = 'home' | 'datasets' | 'connectors' | 'pipeline' | 'ontology' | 'workshop'

export type PipelineType = 'batch' | 'streaming'

export type PipelineDataset = {
  id: string
  name: string
  datasetName: string
}

export type PipelineContext = {
  folderId: string
  folderName: string
  datasets: PipelineDataset[]
}

export type PipelineInfo = {
  id: string
  folderId: string
  name: string
  type: PipelineType
}

type AppState = {
  activeNav: NavKey
  setActiveNav: (nav: NavKey) => void
  pipelineContext: PipelineContext | null
  setPipelineContext: (context: PipelineContext | null) => void
  pipelinesByFolder: Record<string, PipelineInfo>
  createPipeline: (pipeline: PipelineInfo) => void
}

export const useAppStore = create<AppState>((set) => ({
  activeNav: 'home',
  setActiveNav: (nav) => set({ activeNav: nav }),
  pipelineContext: null,
  setPipelineContext: (pipelineContext) => set({ pipelineContext }),
  pipelinesByFolder: {},
  createPipeline: (pipeline) =>
    set((state) => ({
      pipelinesByFolder: {
        ...state.pipelinesByFolder,
        [pipeline.folderId]: pipeline,
      },
    })),
}))
