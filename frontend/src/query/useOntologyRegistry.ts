import { useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { listOntology } from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { qk } from './queryKeys'
import { useAppStore } from '../store/useAppStore'

export type OntologyProperty = {
  id: string
  label: string
  raw: Record<string, unknown>
}

export type OntologyRelationship = {
  predicate: string
  label: string
  target: string
  raw: Record<string, unknown>
}

export type OntologyClass = {
  id: string
  label: string
  raw: Record<string, unknown>
  properties: OntologyProperty[]
  relationships: OntologyRelationship[]
}

const coerceLabel = (value: unknown, fallback: string) => {
  if (typeof value === 'string' && value.trim()) {
    return value.trim()
  }
  if (value && typeof value === 'object') {
    const localized = value as Record<string, unknown>
    const ko = typeof localized.ko === 'string' ? localized.ko.trim() : ''
    const en = typeof localized.en === 'string' ? localized.en.trim() : ''
    if (ko) return ko
    if (en) return en
    const first = Object.values(localized).find((item) => typeof item === 'string' && item.trim())
    if (typeof first === 'string') {
      return first.trim()
    }
  }
  return fallback
}

const normalizeProperties = (raw: unknown): Record<string, unknown>[] => {
  if (Array.isArray(raw)) {
    return raw.filter((item): item is Record<string, unknown> => Boolean(item && typeof item === 'object'))
  }
  if (raw && typeof raw === 'object') {
    return Object.entries(raw as Record<string, unknown>)
      .filter(([name]) => name && !name.startsWith('rdfs:') && name !== '@type' && name !== '@class')
      .map(([name, value]) => ({
        name,
        type: typeof value === 'string' ? value : (value as { '@class'?: string })['@class'],
      }) as Record<string, unknown>)
  }
  return []
}

const normalizeRelationships = (raw: unknown) => {
  if (!Array.isArray(raw)) {
    return []
  }
  return raw.filter((item): item is Record<string, unknown> => Boolean(item && typeof item === 'object'))
}

export const useOntologyRegistry = (dbName: string, branch: string) => {
  const requestContext = useRequestContext()
  const language = useAppStore((state) => state.context.language)

  const query = useQuery({
    queryKey: qk.ontologyList(dbName, branch, requestContext.language),
    queryFn: () => listOntology(requestContext, dbName, branch),
  })

  const classes = useMemo(() => {
    const payload = query.data as { ontologies?: Array<Record<string, unknown>>; data?: { ontologies?: Array<Record<string, unknown>> } } | undefined
    const list = payload?.ontologies ?? payload?.data?.ontologies ?? []
    return list
      .map((item) => {
        const id = String(item.id ?? item['@id'] ?? '').trim()
        if (!id) return null
        const label = coerceLabel(item.label ?? item['@label'] ?? item.display_label, id)
        const props = normalizeProperties(item.properties).map((prop) => {
          const propId = String(prop.name ?? prop.id ?? '').trim()
          if (!propId) {
            return null
          }
          const propLabel = coerceLabel(prop.display_label ?? prop.label, propId)
          return { id: propId, label: propLabel, raw: prop }
        }).filter((prop): prop is OntologyProperty => Boolean(prop))
        const rels = normalizeRelationships(item.relationships).map((rel) => {
          const predicate = String(rel.predicate ?? '').trim()
          if (!predicate) {
            return null
          }
          const target = String(rel.target ?? '').trim()
          const relLabel = coerceLabel(rel.display_label ?? rel.label, predicate)
          return { predicate, label: relLabel, target, raw: rel }
        }).filter((rel): rel is OntologyRelationship => Boolean(rel))
        return { id, label, raw: item, properties: props, relationships: rels }
      })
      .filter((item): item is OntologyClass => Boolean(item))
  }, [query.data])

  return {
    classes,
    isLoading: query.isLoading,
    isFetching: query.isFetching,
    error: query.error,
    language,
  }
}
