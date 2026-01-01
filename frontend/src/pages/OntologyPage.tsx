import { useMemo, useState } from 'react'
import { Button, Card, Icon, InputGroup, Popover, Text } from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/icons'

type ResourceType =
  | 'Object type'
  | 'Link type'
  | 'Property'
  | 'Shared property'
  | 'Action type'
  | 'Group'
  | 'Interface'
  | 'Value type'
  | 'Function'

type ResourceCategory =
  | 'object-types'
  | 'properties'
  | 'shared-properties'
  | 'link-types'
  | 'action-types'
  | 'groups'
  | 'interfaces'
  | 'value-types'
  | 'functions'

type OntologyResource = {
  id: string
  name: string
  type: ResourceType
}

type CreateItem = {
  id: string
  title: string
  description: string
  icon: IconName
}

type OntologyNavItem = {
  id: string
  label: string
  icon: IconName
  count?: number
}

type OntologyContent = {
  title: string
  description: string
  details?: string[]
}

const resourceTypeLabels: Record<ResourceCategory, ResourceType> = {
  'object-types': 'Object type',
  properties: 'Property',
  'shared-properties': 'Shared property',
  'link-types': 'Link type',
  'action-types': 'Action type',
  groups: 'Group',
  interfaces: 'Interface',
  'value-types': 'Value type',
  functions: 'Function',
}

const buildResourceList = (
  category: ResourceCategory,
  count: number,
  seed: Array<{ id: string; name: string }> = [],
): OntologyResource[] => {
  const label = resourceTypeLabels[category]
  const base = seed.slice(0, count).map((item) => ({ ...item, type: label }))
  const remainder = Math.max(count - base.length, 0)
  const generated = Array.from({ length: remainder }, (_, index) => ({
    id: `${category}-${index + base.length + 1}`,
    name: `${label} ${index + base.length + 1}`,
    type: label,
  }))
  return [...base, ...generated]
}

const ontologyResourceCatalog: Record<ResourceCategory, OntologyResource[]> = {
  'object-types': buildResourceList('object-types', 17, [
    { id: 'object:facility', name: 'Facility' },
    { id: 'object:vendor', name: 'Vendor' },
  ]),
  properties: buildResourceList('properties', 1, [{ id: 'property:address', name: 'Address' }]),
  'shared-properties': buildResourceList('shared-properties', 0),
  'link-types': buildResourceList('link-types', 13, [{ id: 'link:facility_vendor', name: 'Facility ↔ Vendor' }]),
  'action-types': buildResourceList('action-types', 19),
  groups: buildResourceList('groups', 0),
  interfaces: buildResourceList('interfaces', 0),
  'value-types': buildResourceList('value-types', 0),
  functions: buildResourceList('functions', 147, [
    { id: 'function:normalize_address', name: 'Normalize Address' },
  ]),
}

const ontologyResources: OntologyResource[] = Object.values(ontologyResourceCatalog).flat()

const resourceCounts: Record<ResourceCategory, number> = {
  'object-types': ontologyResourceCatalog['object-types'].length,
  properties: ontologyResourceCatalog.properties.length,
  'shared-properties': ontologyResourceCatalog['shared-properties'].length,
  'link-types': ontologyResourceCatalog['link-types'].length,
  'action-types': ontologyResourceCatalog['action-types'].length,
  groups: ontologyResourceCatalog.groups.length,
  interfaces: ontologyResourceCatalog.interfaces.length,
  'value-types': ontologyResourceCatalog['value-types'].length,
  functions: ontologyResourceCatalog.functions.length,
}

const createMenuItems: CreateItem[] = [
  {
    id: 'object-type',
    title: 'Object type',
    description: 'Map datasets and models to object types',
    icon: 'cube',
  },
  {
    id: 'link-type',
    title: 'Link type',
    description: 'Create relationships between object types',
    icon: 'link',
  },
  {
    id: 'action-type',
    title: 'Action type',
    description: 'Allow users to writeback to their ontology',
    icon: 'flash',
  },
  {
    id: 'shared-property',
    title: 'Shared property',
    description: 'Create properties that can be shared across object types',
    icon: 'property',
  },
  {
    id: 'group',
    title: 'Group',
    description: 'Use groups to create ontology taxonomies',
    icon: 'group-objects',
  },
  {
    id: 'interface',
    title: 'Interface',
    description: 'Use interfaces to build against abstract types',
    icon: 'grid',
  },
  {
    id: 'function',
    title: 'Function',
    description: 'Define object modifications in code',
    icon: 'function',
  },
  {
    id: 'value-type',
    title: 'Value type',
    description: 'Define constraints that can be applied to property values',
    icon: 'tag',
  },
]

const primaryNavItems: OntologyNavItem[] = [
  { id: 'discover', label: 'Discover', icon: 'compass' },
  { id: 'history', label: 'History', icon: 'history' },
]

const resourceNavItems: OntologyNavItem[] = [
  { id: 'object-types', label: 'Object types', icon: 'cube', count: resourceCounts['object-types'] },
  { id: 'properties', label: 'Properties', icon: 'property', count: resourceCounts.properties },
  { id: 'shared-properties', label: 'Shared properties', icon: 'layers', count: resourceCounts['shared-properties'] },
  { id: 'link-types', label: 'Link types', icon: 'link', count: resourceCounts['link-types'] },
  { id: 'action-types', label: 'Action types', icon: 'flash', count: resourceCounts['action-types'] },
  { id: 'groups', label: 'Groups', icon: 'group-objects', count: resourceCounts.groups },
  { id: 'interfaces', label: 'Interfaces', icon: 'grid', count: resourceCounts.interfaces },
  { id: 'value-types', label: 'Value types', icon: 'tag', count: resourceCounts['value-types'] },
  { id: 'functions', label: 'Functions', icon: 'function', count: resourceCounts.functions },
]

const operationsNavItems: OntologyNavItem[] = [
  { id: 'health-issues', label: 'Health issues', icon: 'issue' },
  { id: 'cleanup', label: 'Cleanup', icon: 'clean' },
  { id: 'ontology-config', label: 'Ontology configuration', icon: 'cog' },
]

const ontologyContentMap: Record<string, OntologyContent> = {
  discover: {
    title: 'Discover',
    description: '온톨로지를 빠르게 탐색하고 이해하는 화면입니다.',
    details: ['오브젝트 타입, 연결 구조, 핵심 속성을 요약합니다.', '신규 참여자 온보딩에 활용됩니다.'],
  },
  history: {
    title: 'History',
    description: '온톨로지 변경 이력을 추적합니다.',
    details: ['누가/언제/무엇을 변경했는지 확인합니다.', '운영 사고를 방지하기 위한 감사 로그 역할을 합니다.'],
  },
  'object-types': {
    title: `Object types (${resourceCounts['object-types']})`,
    description: '도메인의 명사에 해당하는 핵심 객체 정의입니다.',
    details: ['고유 ID 규칙, 속성, 링크, 액션을 포함합니다.'],
  },
  properties: {
    title: `Properties (${resourceCounts.properties})`,
    description: '오브젝트 타입에 붙는 속성 정의입니다.',
    details: ['타입, 필수 여부, 제약 조건을 포함합니다.'],
  },
  'shared-properties': {
    title: `Shared properties (${resourceCounts['shared-properties']})`,
    description: '여러 타입에서 재사용 가능한 공통 속성 정의입니다.',
    details: ['중복 정의를 줄이고 일관성을 유지합니다.'],
  },
  'link-types': {
    title: `Link types (${resourceCounts['link-types']})`,
    description: '오브젝트 간 관계(엣지 타입)를 정의합니다.',
    details: ['방향성, 카디널리티, 링크 속성을 포함합니다.'],
  },
  'action-types': {
    title: `Action types (${resourceCounts['action-types']})`,
    description: '오브젝트/링크에 가능한 행동 정의입니다.',
    details: ['업무 트랜잭션 및 자동화 단위로 활용됩니다.'],
  },
  groups: {
    title: `Groups (${resourceCounts.groups})`,
    description: '도메인별 리소스를 묶어 관리하는 구조입니다.',
    details: ['큰 온톨로지를 모듈화하는 데 사용합니다.'],
  },
  interfaces: {
    title: `Interfaces (${resourceCounts.interfaces})`,
    description: '공통 계약을 정의해 여러 타입이 채택하게 합니다.',
    details: ['객체지향의 interface와 유사합니다.'],
  },
  'value-types': {
    title: `Value types (${resourceCounts['value-types']})`,
    description: '도메인 값 타입을 정의합니다.',
    details: ['예: Money, Address, GeoPoint 같은 값 타입을 포함합니다.'],
  },
  functions: {
    title: `Functions (${resourceCounts.functions})`,
    description: '온톨로지 위에서 쓰는 계산/파생 로직입니다.',
    details: ['UI 표시값, 규칙/알림/액션 트리거에 사용됩니다.'],
  },
  'health-issues': {
    title: 'Health issues',
    description: '온톨로지의 문제점과 경고를 진단합니다.',
    details: ['미사용 타입, 끊긴 링크, 정의 누락 등을 확인합니다.'],
  },
  cleanup: {
    title: 'Cleanup',
    description: '미사용 리소스를 정리하는 화면입니다.',
    details: ['참조되지 않는 정의를 제거해 품질을 유지합니다.'],
  },
  'ontology-config': {
    title: 'Ontology configuration',
    description: '온톨로지 전역 정책과 환경을 설정합니다.',
    details: ['네이밍 규칙, 권한, 기본값 같은 설정을 관리합니다.'],
  },
}

export const OntologyPage = () => {
  const [searchQuery, setSearchQuery] = useState('')
  const [isSearchFocused, setSearchFocused] = useState(false)
  const [activeNavId, setActiveNavId] = useState('discover')
  const activeContent = ontologyContentMap[activeNavId]

  const filteredResources = useMemo(() => {
    const query = searchQuery.trim().toLowerCase()
    if (!query) {
      return []
    }
    return ontologyResources.filter((resource) => resource.name.toLowerCase().includes(query))
  }, [searchQuery])

  const showResults = isSearchFocused && searchQuery.trim().length > 0

  const handleSelectResource = (resource: OntologyResource) => {
    setSearchQuery(resource.name)
    setSearchFocused(false)
  }

  const createMenu = (
    <div className="ontology-create-menu" role="menu">
      {createMenuItems.map((item) => (
        <button key={item.id} type="button" className="ontology-create-item" role="menuitem">
          <span className="ontology-create-item-icon">
            <Icon icon={item.icon} size={16} />
          </span>
          <span className="ontology-create-item-text">
            <span className="ontology-create-item-title">{item.title}</span>
            <span className="ontology-create-item-description">{item.description}</span>
          </span>
        </button>
      ))}
    </div>
  )

  return (
    <div className="page ontology-page">
      <div className="ontology-topbar">
        <span className="ontology-home-label">Ontology Manager</span>

        <div className="ontology-search">
          <InputGroup
            leftIcon="search"
            placeholder="Search resources..."
            value={searchQuery}
            onChange={(event) => setSearchQuery(event.target.value)}
            onFocus={() => setSearchFocused(true)}
            onBlur={() => setSearchFocused(false)}
            rightElement={<span className="ontology-search-hint">⌘K</span>}
            className="ontology-search-input"
            aria-label="Search ontology resources"
          />
          {showResults ? (
            <div
              className="ontology-search-results"
              onMouseDown={(event) => event.preventDefault()}
              role="listbox"
            >
              {filteredResources.length > 0 ? (
                filteredResources.map((resource) => (
                  <button
                    key={resource.id}
                    type="button"
                    className="ontology-search-result"
                    onClick={() => handleSelectResource(resource)}
                    role="option"
                  >
                    <span className="ontology-search-result-name">{resource.name}</span>
                    <span className="ontology-search-result-type">{resource.type}</span>
                  </button>
                ))
              ) : (
                <div className="ontology-search-empty">No results found.</div>
              )}
            </div>
          ) : null}
        </div>

        <div className="ontology-actions">
          <Popover content={createMenu} placement="bottom-end" popoverClassName="ontology-create-popover">
            <Button className="ontology-new-button" text="New" rightIcon="caret-down" />
          </Popover>
        </div>
      </div>

      <div className="ontology-layout">
        <aside className="ontology-side-panel">
          <div className="ontology-side-section">
            {primaryNavItems.map((item) => (
              <button
                key={item.id}
                type="button"
                className={`ontology-side-item ${item.id === activeNavId ? 'is-active' : ''}`}
                onClick={() => setActiveNavId(item.id)}
              >
                <Icon icon={item.icon} size={14} className="ontology-side-item-icon" />
                <span className="ontology-side-item-label">{item.label}</span>
              </button>
            ))}
          </div>
          <div className="ontology-side-section">
            <span className="ontology-side-title">Resources</span>
            {resourceNavItems.map((item) => (
              <button
                key={item.id}
                type="button"
                className={`ontology-side-item ${item.id === activeNavId ? 'is-active' : ''}`}
                onClick={() => setActiveNavId(item.id)}
              >
                <Icon icon={item.icon} size={14} className="ontology-side-item-icon" />
                <span className="ontology-side-item-label">{item.label}</span>
                {item.count !== undefined ? (
                  <span className="ontology-side-item-count">{item.count}</span>
                ) : null}
              </button>
            ))}
          </div>
          <div className="ontology-side-section">
            <span className="ontology-side-title">Operations</span>
            {operationsNavItems.map((item) => (
              <button
                key={item.id}
                type="button"
                className={`ontology-side-item ${item.id === activeNavId ? 'is-active' : ''}`}
                onClick={() => setActiveNavId(item.id)}
              >
                <Icon icon={item.icon} size={14} className="ontology-side-item-icon" />
                <span className="ontology-side-item-label">{item.label}</span>
              </button>
            ))}
          </div>
        </aside>
        <div className="ontology-main">
          <Card className="card ontology-content">
            <div className="ontology-content-header">
              <Text className="ontology-content-title">{activeContent.title}</Text>
              <Text className="ontology-content-description">{activeContent.description}</Text>
            </div>
            {activeContent.details?.length ? (
              <ul className="ontology-content-list">
                {activeContent.details.map((detail) => (
                  <li key={detail}>{detail}</li>
                ))}
              </ul>
            ) : null}
          </Card>
        </div>
      </div>
    </div>
  )
}
