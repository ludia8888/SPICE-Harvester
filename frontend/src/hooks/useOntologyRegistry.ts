import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { listOntologies } from '../api/bff'
import { qk } from '../query/queryKeys'
import { useAppStore } from '../store/useAppStore'
import { formatLabel, type LocalizedText } from '../utils/labels'

type OntologyProperty = {
  name?: string
  type?: string
  label?: LocalizedText
  required?: boolean
  primary_key?: boolean
}

type OntologyRelationship = {
  predicate?: string
  target?: string
  label?: LocalizedText
  cardinality?: string
}

type OntologyClass = {
  id?: string
  label?: LocalizedText
  description?: LocalizedText
  version?: number
  properties?: OntologyProperty[]
  relationships?: OntologyRelationship[]
  [key: string]: unknown
}

const extractOntologies = (payload: any): OntologyClass[] => {
  if (!payload || typeof payload !== 'object') {
    return []
  }
  if (Array.isArray(payload.ontologies)) {
    return payload.ontologies
  }
  if (payload.data && Array.isArray(payload.data.ontologies)) {
    return payload.data.ontologies
  }
  if (payload.data && Array.isArray(payload.data.classes)) {
    return payload.data.classes
  }
  return []
}

export const useOntologyRegistry = (dbName?: string | null, branch?: string) => {
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)

  const query = useQuery({
    queryKey: dbName && branch ? qk.ontologies({ dbName, branch, language: context.language }) : ['bff', 'ontology', 'empty'],
    queryFn: () =>
      listOntologies(
        { language: context.language, authToken, adminToken },
        dbName ?? '',
        branch ?? context.branch,
      ),
    enabled: Boolean(dbName && branch),
  })

  const classes = useMemo(() => extractOntologies(query.data), [query.data])

  const classOptions = useMemo(
    () =>
      classes
        .filter((item) => item.id)
        .map((item) => ({
          value: item.id as string,
          label: formatLabel(item.label, context.language, item.id as string),
        })),
    [classes, context.language],
  )

  const classMap = useMemo(() => {
    const map = new Map<string, OntologyClass>()
    classes.forEach((item) => {
      if (item.id) {
        map.set(item.id, item)
      }
    })
    return map
  }, [classes])

  const predicateMap = useMemo(() => {
    const map = new Map<string, string>()
    classes.forEach((item) => {
      item.relationships?.forEach((rel) => {
        if (rel.predicate) {
          map.set(rel.predicate, formatLabel(rel.label, context.language, rel.predicate))
        }
      })
    })
    return map
  }, [classes, context.language])

  const propertyLabelsByClass = useMemo(() => {
    const map = new Map<string, { id: string; label: string; type?: string }[]>()
    classes.forEach((item) => {
      if (!item.id) {
        return
      }
      const props =
        item.properties?.map((prop) => ({
          id: prop.name ?? '',
          label: formatLabel(prop.label, context.language, prop.name ?? ''),
          type: prop.type,
        })) ?? []
      map.set(item.id, props)
    })
    return map
  }, [classes, context.language])

  return {
    ...query,
    classes,
    classOptions,
    classMap,
    predicateMap,
    propertyLabelsByClass,
  }
}

export type { OntologyClass, OntologyProperty, OntologyRelationship }
