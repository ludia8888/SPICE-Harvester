import { create } from 'zustand'

export type NavKey = 'home' | 'datasets' | 'connectors' | 'pipeline' | 'ontology' | 'ai-agent' | 'workshop'

export type PipelineContext = {
  folderId: string
  folderName: string
}

export type AiAgentContextNode = {
  id: string
  label: string
  type?: string
}

export type AiAgentContext = {
  projectName: string
  pipelineName: string
  nodes: AiAgentContextNode[]
}

export type PipelineAgentRun = Record<string, unknown>

export type PipelineAgentRequest = {
  goal: string
  data_scope: Record<string, unknown>
  planner_hints?: Record<string, unknown>
}

type AppState = {
  activeNav: NavKey
  setActiveNav: (nav: NavKey) => void
  pipelineContext: PipelineContext | null
  setPipelineContext: (context: PipelineContext | null) => void
  isAiAgentOpen: boolean
  setAiAgentOpen: (isOpen: boolean) => void
  aiAgentContext: AiAgentContext
  setAiAgentContext: (context: AiAgentContext) => void
  pipelineAgentRun: PipelineAgentRun | null
  setPipelineAgentRun: (run: PipelineAgentRun | null) => void
  pipelineAgentRequest: PipelineAgentRequest | null
  setPipelineAgentRequest: (request: PipelineAgentRequest | null) => void
  pipelineAgentQuestions: Array<Record<string, unknown>>
  setPipelineAgentQuestions: (questions: Array<Record<string, unknown>>) => void
}

export const useAppStore = create<AppState>((set) => ({
  activeNav: 'home',
  setActiveNav: (nav) => set({ activeNav: nav }),
  pipelineContext: null,
  setPipelineContext: (pipelineContext) => set({ pipelineContext }),
  isAiAgentOpen: false,
  setAiAgentOpen: (isAiAgentOpen) => set({ isAiAgentOpen }),
  aiAgentContext: { projectName: '', pipelineName: '', nodes: [] },
  setAiAgentContext: (aiAgentContext) => set({ aiAgentContext }),
  pipelineAgentRun: null,
  setPipelineAgentRun: (pipelineAgentRun) => set({ pipelineAgentRun }),
  pipelineAgentRequest: null,
  setPipelineAgentRequest: (pipelineAgentRequest) => set({ pipelineAgentRequest }),
  pipelineAgentQuestions: [],
  setPipelineAgentQuestions: (pipelineAgentQuestions) => set({ pipelineAgentQuestions }),
}))
