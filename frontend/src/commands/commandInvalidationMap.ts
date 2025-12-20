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
    case 'CREATE_ONTOLOGY':
    case 'UPDATE_ONTOLOGY':
    case 'DELETE_ONTOLOGY':
      return [
        qk.ontologyList(command.target.dbName, command.context.branch, language),
        ...(command.target.classId
          ? [qk.ontology(command.target.dbName, command.target.classId, command.context.branch, language)]
          : []),
        qk.summary({ dbName: command.target.dbName, branch: command.context.branch, language }),
      ]
    case 'CREATE_INSTANCE':
    case 'UPDATE_INSTANCE':
    case 'DELETE_INSTANCE':
    case 'BULK_CREATE_INSTANCE':
    case 'IMPORT_SHEETS':
    case 'IMPORT_EXCEL': {
      const classId = command.target.classId
      const instanceId = command.target.instanceId
      return [
        ...(classId ? [['bff', 'instances', command.target.dbName, classId]] : []),
        ...(classId ? [qk.sampleValues(command.target.dbName, classId, language)] : []),
        ...(classId && instanceId
          ? [qk.instance(command.target.dbName, classId, instanceId, language)]
          : []),
        qk.summary({ dbName: command.target.dbName, branch: command.context.branch, language }),
      ]
    }
    case 'MERGE_RESOLVE':
      return [
        qk.summary({ dbName: command.target.dbName, branch: command.context.branch, language }),
        qk.ontologyList(command.target.dbName, command.context.branch, language),
      ]
    default:
      return []
  }
}
