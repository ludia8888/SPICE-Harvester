import type { Language } from '../types/app'

export const qk = {
  databases: (language: Language) => ['bff', 'databases', { lang: language }] as const,
  databaseExpectedSeq: (dbName: string) => ['bff', 'databases', dbName, 'expected-seq'] as const,
  commandStatus: (commandId: string, language: Language) =>
    ['bff', 'commands', commandId, 'status', { lang: language }] as const,
  branches: (dbName: string, language: Language) =>
    ['bff', 'branches', dbName, { lang: language }] as const,
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
  ontologies: (params: { dbName: string; branch: string; language: Language }) =>
    ['bff', 'ontology', params.dbName, params.branch, { lang: params.language }] as const,
  ontologyDetail: (params: { dbName: string; classLabel: string; branch: string; language: Language }) =>
    [
      'bff',
      'ontology',
      params.dbName,
      params.classLabel,
      params.branch,
      { lang: params.language },
    ] as const,
  mappingsSummary: (params: { dbName: string; language: Language }) =>
    ['bff', 'mappings', params.dbName, 'summary', { lang: params.language }] as const,
  mappingsExport: (params: { dbName: string; language: Language }) =>
    ['bff', 'mappings', params.dbName, 'export', { lang: params.language }] as const,
  sheetsRegistered: (params: { dbName?: string; language: Language }) =>
    ['bff', 'sheets', 'registered', params.dbName ?? null, { lang: params.language }] as const,
  instances: (params: {
    dbName: string
    classId: string
    limit?: number
    offset?: number
    search?: string
    language: Language
  }) =>
    [
      'bff',
      'instances',
      params.dbName,
      params.classId,
      {
        limit: params.limit ?? null,
        offset: params.offset ?? null,
        search: params.search ?? null,
        lang: params.language,
      },
    ] as const,
  instanceDetail: (params: { dbName: string; classId: string; instanceId: string; language: Language }) =>
    [
      'bff',
      'instance',
      params.dbName,
      params.classId,
      params.instanceId,
      { lang: params.language },
    ] as const,
  queryBuilder: (params: { dbName: string; language: Language }) =>
    ['bff', 'query-builder', params.dbName, { lang: params.language }] as const,
  auditLogs: (params: { partitionKey: string; language: Language }) =>
    ['bff', 'audit', params.partitionKey, { lang: params.language }] as const,
  lineage: (params: { dbName: string; root: string; language: Language }) =>
    ['bff', 'lineage', params.dbName, params.root, { lang: params.language }] as const,
  tasks: (params: { status?: string; taskType?: string; language: Language }) =>
    ['bff', 'tasks', params.status ?? null, params.taskType ?? null, { lang: params.language }] as const,
} as const
