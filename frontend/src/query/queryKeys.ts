import type { Language } from '../types/app'

export const qk = {
  databases: (language: Language) => ['bff', 'databases', { lang: language }] as const,
  databaseExpectedSeq: (dbName: string) => ['bff', 'databases', dbName, 'expected-seq'] as const,
  branches: (dbName: string, language: Language) =>
    ['bff', 'databases', dbName, 'branches', { lang: language }] as const,
  classes: (dbName: string, language: Language) =>
    ['bff', 'databases', dbName, 'classes', { lang: language }] as const,
  ontologyList: (dbName: string, branch: string, language: Language) =>
    ['bff', 'ontology', dbName, 'list', { branch, lang: language }] as const,
  ontology: (dbName: string, classLabel: string, branch: string, language: Language) =>
    ['bff', 'ontology', dbName, classLabel, { branch, lang: language }] as const,
  ontologySchema: (dbName: string, classId: string, branch: string, format: string, language: Language) =>
    ['bff', 'ontology', dbName, classId, 'schema', { branch, format, lang: language }] as const,
  mappingsSummary: (dbName: string, language: Language) =>
    ['bff', 'mappings', dbName, 'summary', { lang: language }] as const,
  registeredSheets: (dbName: string | null, language: Language) =>
    ['bff', 'sheets', 'registered', { db: dbName, lang: language }] as const,
  instances: (dbName: string, classId: string, language: Language, params: Record<string, unknown>) =>
    ['bff', 'instances', dbName, classId, { ...params, lang: language }] as const,
  instance: (dbName: string, classId: string, instanceId: string, language: Language) =>
    ['bff', 'instances', dbName, classId, instanceId, { lang: language }] as const,
  sampleValues: (dbName: string, classId: string, language: Language) =>
    ['bff', 'instances', dbName, classId, 'sample-values', { lang: language }] as const,
  queryBuilder: (dbName: string, language: Language) =>
    ['bff', 'query', dbName, 'builder', { lang: language }] as const,
  auditLogs: (dbName: string, language: Language, params: Record<string, unknown>) =>
    ['bff', 'audit', dbName, { ...params, lang: language }] as const,
  lineageGraph: (dbName: string, root: string, language: Language) =>
    ['bff', 'lineage', dbName, 'graph', root, { lang: language }] as const,
  lineageImpact: (dbName: string, root: string, language: Language) =>
    ['bff', 'lineage', dbName, 'impact', root, { lang: language }] as const,
  lineageMetrics: (dbName: string, language: Language, windowMinutes?: number) =>
    ['bff', 'lineage', dbName, 'metrics', { windowMinutes, lang: language }] as const,
  tasks: (language: Language, params?: Record<string, unknown>) =>
    ['bff', 'tasks', { ...(params ?? {}), lang: language }] as const,
  summary: (params: { dbName?: string | null; branch?: string | null; language: Language }) =>
    [
      'bff',
      'summary',
      {
        db: params.dbName ?? null,
        branch: params.branch ?? null,
        lang: params.language,
      },
    ] as const,
  commandStatus: (commandId: string, language: Language) =>
    ['bff', 'commands', commandId, 'status', { lang: language }] as const,
} as const
