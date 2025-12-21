import type { QueryKey } from '@tanstack/react-query'
import { qk } from '../query/queryKeys'
import type { Language } from '../types/app'
import type { TrackedCommand } from '../store/useAppStore'

export const getInvalidationKeys = (
  command: TrackedCommand,
  language: Language,
): QueryKey[] => {
  switch (command.kind) {
    case 'CREATE_DATABASE':
    case 'DELETE_DATABASE':
      return [
        qk.databases(language),
        qk.summary({ dbName: command.target.dbName, branch: command.context.branch, language }),
      ]
    case 'CREATE_BRANCH':
    case 'DELETE_BRANCH':
      return [
        qk.branches(command.target.dbName, language),
        qk.summary({ dbName: command.target.dbName, branch: command.context.branch, language }),
      ]
    case 'ONTOLOGY_APPLY':
    case 'ONTOLOGY_DELETE':
      return [
        qk.ontologies({ dbName: command.target.dbName, branch: command.context.branch, language }),
        qk.summary({ dbName: command.target.dbName, branch: command.context.branch, language }),
      ]
    case 'MAPPINGS_IMPORT':
      return [qk.mappingsSummary({ dbName: command.target.dbName, language })]
    case 'ADMIN_TASK':
      return [qk.tasks({ language })]
    default:
      return []
  }
}
