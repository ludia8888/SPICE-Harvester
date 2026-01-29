import { create } from 'zustand'

export type NavKey = 'home' | 'datasets' | 'connectors' | 'pipeline' | 'ontology' | 'explorer' | 'lineage' | 'workshop'

export type PipelineContext = {
  folderId: string
  folderName: string
}

export type PipelineAgentRun = Record<string, unknown>

export type PipelineAgentRequest = {
  goal: string
  data_scope: Record<string, unknown>
  planner_hints?: Record<string, unknown>
}

export type ExplorerResult = {
  id: string
  classId: string
  className: string
  label: string
  properties: Record<string, unknown>
}

export type SimulationResultState = {
  success: boolean
  changes: Array<{
    entityId: string
    entityLabel: string
    field: string
    before: unknown
    after: unknown
  }>
  warnings?: string[]
  errors?: string[]
}

type AppState = {
  activeNav: NavKey
  setActiveNav: (nav: NavKey) => void
  pipelineContext: PipelineContext | null
  setPipelineContext: (context: PipelineContext | null) => void
  pipelineAgentRun: PipelineAgentRun | null
  setPipelineAgentRun: (run: PipelineAgentRun | null) => void
  pipelineAgentRequest: PipelineAgentRequest | null
  setPipelineAgentRequest: (request: PipelineAgentRequest | null) => void
  pipelineAgentQuestions: Array<Record<string, unknown>>
  setPipelineAgentQuestions: (questions: Array<Record<string, unknown>>) => void

  // Explorer
  explorerSearchQuery: string
  setExplorerSearchQuery: (q: string) => void
  explorerResults: ExplorerResult[]
  setExplorerResults: (r: ExplorerResult[]) => void
  explorerSelectedClasses: string[]
  setExplorerSelectedClasses: (classes: string[]) => void
  selectedInstanceId: string | null
  setSelectedInstanceId: (id: string | null) => void

  // Lineage
  lineageTargetId: string | null
  setLineageTargetId: (id: string | null) => void
  lineageDirection: 'upstream' | 'downstream' | 'both'
  setLineageDirection: (d: 'upstream' | 'downstream' | 'both') => void

  // Ontology - Action
  selectedActionTypeId: string | null
  setSelectedActionTypeId: (id: string | null) => void
  actionSimulationResult: SimulationResultState | null
  setActionSimulationResult: (r: SimulationResultState | null) => void
}

export const useAppStore = create<AppState>((set) => ({
  activeNav: 'home',
  setActiveNav: (nav) => set({ activeNav: nav }),
  pipelineContext: null,
  setPipelineContext: (pipelineContext) => set({ pipelineContext }),
  pipelineAgentRun: null,
  setPipelineAgentRun: (pipelineAgentRun) => set({ pipelineAgentRun }),
  pipelineAgentRequest: null,
  setPipelineAgentRequest: (pipelineAgentRequest) => set({ pipelineAgentRequest }),
  pipelineAgentQuestions: [],
  setPipelineAgentQuestions: (pipelineAgentQuestions) => set({ pipelineAgentQuestions }),

  // Explorer
  explorerSearchQuery: '',
  setExplorerSearchQuery: (explorerSearchQuery) => set({ explorerSearchQuery }),
  explorerResults: [],
  setExplorerResults: (explorerResults) => set({ explorerResults }),
  explorerSelectedClasses: [],
  setExplorerSelectedClasses: (explorerSelectedClasses) => set({ explorerSelectedClasses }),
  selectedInstanceId: null,
  setSelectedInstanceId: (selectedInstanceId) => set({ selectedInstanceId }),

  // Lineage
  lineageTargetId: null,
  setLineageTargetId: (lineageTargetId) => set({ lineageTargetId }),
  lineageDirection: 'both',
  setLineageDirection: (lineageDirection) => set({ lineageDirection }),

  // Ontology - Action
  selectedActionTypeId: null,
  setSelectedActionTypeId: (selectedActionTypeId) => set({ selectedActionTypeId }),
  actionSimulationResult: null,
  setActionSimulationResult: (actionSimulationResult) => set({ actionSimulationResult }),
}))
