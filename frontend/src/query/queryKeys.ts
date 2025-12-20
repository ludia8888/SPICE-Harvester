import type { Language } from '../types/app'

export const qk = {
  databases: (language: Language) => ['bff', 'databases', { lang: language }] as const,
  databaseExpectedSeq: (dbName: string) => ['bff', 'databases', dbName, 'expected-seq'] as const,
  commandStatus: (commandId: string, language: Language) =>
    ['bff', 'commands', commandId, 'status', { lang: language }] as const,
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
} as const

