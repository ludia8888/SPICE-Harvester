import { useCallback, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Button, Card, Icon, InputGroup, Popover, Spinner, Text } from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/icons'
import { useAppStore } from '../state/store'
import {
  listActionTypes,
  getActionType,
  simulateAction,
  executeAction,
  listActionLogs,
  listOntologyClasses,
  listLinkTypes,
  type ActionType,
  type ActionTypeDetail as ActionTypeDetailType,
} from '../api/bff'
import {
  ActionTypeList,
  ActionTypeDetail,
  ActionForm,
  SimulationPreview,
  ActionHistory,
  ObjectTypeList,
  LinkTypeList,
} from '../components/ontology'

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
  resourceId: string
  name: string
  type: ResourceType
  category: ResourceCategory
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
  properties: {
    title: 'Properties',
    description: '오브젝트 타입에 붙는 속성 정의입니다.',
    details: ['타입, 필수 여부, 제약 조건을 포함합니다.'],
  },
  'shared-properties': {
    title: 'Shared properties',
    description: '여러 타입에서 재사용 가능한 공통 속성 정의입니다.',
    details: ['중복 정의를 줄이고 일관성을 유지합니다.'],
  },
  groups: {
    title: 'Groups',
    description: '도메인별 리소스를 묶어 관리하는 구조입니다.',
    details: ['큰 온톨로지를 모듈화하는 데 사용합니다.'],
  },
  interfaces: {
    title: 'Interfaces',
    description: '공통 계약을 정의해 여러 타입이 채택하게 합니다.',
    details: ['객체지향의 interface와 유사합니다.'],
  },
  'value-types': {
    title: 'Value types',
    description: '도메인 값 타입을 정의합니다.',
    details: ['예: Money, Address, GeoPoint 같은 값 타입을 포함합니다.'],
  },
  functions: {
    title: 'Functions',
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
  const pipelineContext = useAppStore((state) => state.pipelineContext)
  const selectedActionTypeId = useAppStore((state) => state.selectedActionTypeId)
  const setSelectedActionTypeId = useAppStore((state) => state.setSelectedActionTypeId)
  const actionSimulationResult = useAppStore((state) => state.actionSimulationResult)
  const setActionSimulationResult = useAppStore((state) => state.setActionSimulationResult)

  const [searchQuery, setSearchQuery] = useState('')
  const [isSearchFocused, setSearchFocused] = useState(false)
  const [activeNavId, setActiveNavId] = useState('discover')
  const [selectedObjectTypeId, setSelectedObjectTypeId] = useState<string | null>(null)
  const [selectedLinkTypeId, setSelectedLinkTypeId] = useState<string | null>(null)
  const [selectedActionDetail, setSelectedActionDetail] = useState<ActionTypeDetailType | null>(null)
  const [isSimulating, setIsSimulating] = useState(false)
  const [isExecuting, setIsExecuting] = useState(false)

  const activeDbName = pipelineContext?.folderId ?? ''

  // Queries
  const { data: apiActionTypes = [], isLoading: isLoadingActionTypes } = useQuery({
    queryKey: ['action-types', activeDbName],
    queryFn: () => listActionTypes(activeDbName),
    enabled: Boolean(activeDbName),
  })

  const { data: actionLogs = [], isLoading: isLoadingActionLogs } = useQuery({
    queryKey: ['action-logs', activeDbName],
    queryFn: () => listActionLogs(activeDbName),
    enabled: Boolean(activeDbName) && activeNavId === 'action-types',
  })

  const { data: apiObjectTypes = [], isLoading: isLoadingObjectTypes } = useQuery({
    queryKey: ['ontology-classes', activeDbName],
    queryFn: () => listOntologyClasses(activeDbName),
    enabled: Boolean(activeDbName),
  })

  const { data: apiLinkTypes = [], isLoading: isLoadingLinkTypes } = useQuery({
    queryKey: ['link-types', activeDbName],
    queryFn: () => listLinkTypes(activeDbName),
    enabled: Boolean(activeDbName),
  })

  const ontologyResources = useMemo<OntologyResource[]>(() => {
    const resources: OntologyResource[] = []

    apiObjectTypes.forEach((objectType) => {
      resources.push({
        id: `object-types:${objectType.id}`,
        resourceId: objectType.id,
        name: objectType.label,
        type: resourceTypeLabels['object-types'],
        category: 'object-types',
      })
    })

    apiLinkTypes.forEach((linkType) => {
      resources.push({
        id: `link-types:${linkType.id}`,
        resourceId: linkType.id,
        name: linkType.name,
        type: resourceTypeLabels['link-types'],
        category: 'link-types',
      })
    })

    apiActionTypes.forEach((actionType) => {
      resources.push({
        id: `action-types:${actionType.id}`,
        resourceId: actionType.id,
        name: actionType.name,
        type: resourceTypeLabels['action-types'],
        category: 'action-types',
      })
    })

    return resources
  }, [apiActionTypes, apiLinkTypes, apiObjectTypes])

  const resourceNavItems: OntologyNavItem[] = useMemo(() => [
    { id: 'object-types', label: 'Object types', icon: 'cube', count: isLoadingObjectTypes ? undefined : apiObjectTypes.length },
    { id: 'link-types', label: 'Link types', icon: 'link', count: isLoadingLinkTypes ? undefined : apiLinkTypes.length },
    { id: 'action-types', label: 'Action types', icon: 'flash', count: isLoadingActionTypes ? undefined : apiActionTypes.length },
    { id: 'properties', label: 'Properties', icon: 'property' },
    { id: 'shared-properties', label: 'Shared properties', icon: 'layers' },
    { id: 'groups', label: 'Groups', icon: 'group-objects' },
    { id: 'interfaces', label: 'Interfaces', icon: 'grid' },
    { id: 'value-types', label: 'Value types', icon: 'tag' },
    { id: 'functions', label: 'Functions', icon: 'function' },
  ], [apiActionTypes.length, apiLinkTypes.length, apiObjectTypes.length, isLoadingActionTypes, isLoadingLinkTypes, isLoadingObjectTypes])

  const activeContent = ontologyContentMap[activeNavId]

  const filteredResources = useMemo(() => {
    const query = searchQuery.trim().toLowerCase()
    if (!query) {
      return []
    }
    return ontologyResources.filter((resource) => resource.name.toLowerCase().includes(query))
  }, [ontologyResources, searchQuery])

  const showResults = isSearchFocused && searchQuery.trim().length > 0
  const isSearchLoading = isLoadingActionTypes || isLoadingObjectTypes || isLoadingLinkTypes

  const handleSelectResource = (resource: OntologyResource) => {
    setSearchQuery(resource.name)
    setSearchFocused(false)

    if (resource.category === 'object-types') {
      setSelectedObjectTypeId(resource.resourceId)
      setActiveNavId('object-types')
      return
    }
    if (resource.category === 'link-types') {
      setSelectedLinkTypeId(resource.resourceId)
      setActiveNavId('link-types')
      return
    }
    if (resource.category === 'action-types') {
      const match = apiActionTypes.find((a) => a.id === resource.resourceId)
      if (match) {
        void handleSelectActionType(match)
        setActiveNavId('action-types')
      }
    }
  }

  const handleSelectActionType = useCallback(async (actionType: ActionType) => {
    setSelectedActionTypeId(actionType.id)
    setActionSimulationResult(null)

    if (activeDbName) {
      try {
        const detail = await getActionType(activeDbName, actionType.id)
        setSelectedActionDetail(detail)
      } catch {
        setSelectedActionDetail({
          ...actionType,
          examples: [],
        })
      }
    }
  }, [activeDbName, setSelectedActionTypeId, setActionSimulationResult])

  const handleSimulate = useCallback(async (params: Record<string, unknown>) => {
    if (!activeDbName || !selectedActionTypeId) return

    setIsSimulating(true)
    try {
      const result = await simulateAction(activeDbName, selectedActionTypeId, params)
      setActionSimulationResult(result)
    } catch (error) {
      setActionSimulationResult({
        success: false,
        changes: [],
        errors: [error instanceof Error ? error.message : '시뮬레이션에 실패했습니다'],
      })
    } finally {
      setIsSimulating(false)
    }
  }, [activeDbName, selectedActionTypeId, setActionSimulationResult])

  const handleExecute = useCallback(async (params: Record<string, unknown>) => {
    if (!activeDbName || !selectedActionTypeId) return

    setIsExecuting(true)
    try {
      await executeAction(activeDbName, selectedActionTypeId, params)
      setActionSimulationResult(null)
    } catch (error) {
      setActionSimulationResult({
        success: false,
        changes: [],
        errors: [error instanceof Error ? error.message : 'Action 실행에 실패했습니다'],
      })
    } finally {
      setIsExecuting(false)
    }
  }, [activeDbName, selectedActionTypeId, setActionSimulationResult])

  const handleConfirmSimulation = useCallback(() => {
    if (selectedActionDetail) {
      const defaultParams: Record<string, unknown> = {}
      selectedActionDetail.parameters.forEach((p) => {
        if (p.defaultValue !== undefined) {
          defaultParams[p.name] = p.defaultValue
        }
      })
      handleExecute(defaultParams)
    }
  }, [selectedActionDetail, handleExecute])

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

  const renderActionTypesContent = () => (
    <div className="ontology-action-types-content">
      <div className="ontology-action-types-layout">
        <div className="ontology-action-types-list">
          <ActionTypeList
            actionTypes={apiActionTypes}
            selectedId={selectedActionTypeId}
            onSelect={handleSelectActionType}
            isLoading={isLoadingActionTypes}
          />
        </div>
        <div className="ontology-action-types-detail">
          {selectedActionDetail ? (
            <>
              <ActionTypeDetail actionType={selectedActionDetail} />
              <ActionForm
                actionType={selectedActionDetail}
                onSimulate={handleSimulate}
                onExecute={handleExecute}
                isSimulating={isSimulating}
                isExecuting={isExecuting}
              />
            </>
          ) : (
            <div className="ontology-action-types-empty">
              <Icon icon="flash" size={32} />
              <span>Action을 선택하면 상세 정보와 실행 옵션이 표시됩니다</span>
            </div>
          )}
        </div>
      </div>

      {actionSimulationResult && (
        <SimulationPreview
          result={actionSimulationResult}
          onConfirm={handleConfirmSimulation}
          onCancel={() => setActionSimulationResult(null)}
          isExecuting={isExecuting}
        />
      )}

      <ActionHistory logs={actionLogs} isLoading={isLoadingActionLogs} />
    </div>
  )

  const renderObjectTypesContent = () => (
    <div className="ontology-object-types-content">
      <div className="ontology-object-types-layout">
        <div className="ontology-object-types-list">
          <ObjectTypeList
            objectTypes={apiObjectTypes}
            selectedId={selectedObjectTypeId}
            onSelect={(obj) => setSelectedObjectTypeId(obj.id)}
            isLoading={isLoadingObjectTypes}
          />
        </div>
        <div className="ontology-object-types-detail">
          {selectedObjectTypeId ? (
            <Card className="card">
              <div className="ontology-object-detail-header">
                <Icon icon="cube" size={16} />
                <span>{apiObjectTypes.find((o) => o.id === selectedObjectTypeId)?.label}</span>
              </div>
              <p className="ontology-object-detail-desc">
                {apiObjectTypes.find((o) => o.id === selectedObjectTypeId)?.description || '설명 없음'}
              </p>
            </Card>
          ) : (
            <div className="ontology-object-types-empty">
              <Icon icon="cube" size={32} />
              <span>Object type을 선택하면 상세 정보가 표시됩니다</span>
            </div>
          )}
        </div>
      </div>
    </div>
  )

  const renderLinkTypesContent = () => (
    <div className="ontology-link-types-content">
      <div className="ontology-link-types-layout">
        <div className="ontology-link-types-list">
          <LinkTypeList
            linkTypes={apiLinkTypes}
            selectedId={selectedLinkTypeId}
            onSelect={(link) => setSelectedLinkTypeId(link.id)}
            isLoading={isLoadingLinkTypes}
          />
        </div>
        <div className="ontology-link-types-detail">
          {selectedLinkTypeId ? (
            <Card className="card">
              <div className="ontology-link-detail-header">
                <Icon icon="link" size={16} />
                <span>{apiLinkTypes.find((l) => l.id === selectedLinkTypeId)?.name}</span>
              </div>
              <div className="ontology-link-detail-visual">
                <span>{apiLinkTypes.find((l) => l.id === selectedLinkTypeId)?.sourceClassName}</span>
                <Icon icon="arrow-right" size={14} />
                <span>{apiLinkTypes.find((l) => l.id === selectedLinkTypeId)?.targetClassName}</span>
              </div>
            </Card>
          ) : (
            <div className="ontology-link-types-empty">
              <Icon icon="link" size={32} />
              <span>Link type을 선택하면 관계 정보가 표시됩니다</span>
            </div>
          )}
        </div>
      </div>
    </div>
  )

  const renderContent = () => {
    if (activeNavId === 'action-types' && activeDbName) {
      return renderActionTypesContent()
    }

    if (activeNavId === 'object-types' && activeDbName) {
      return renderObjectTypesContent()
    }

    if (activeNavId === 'link-types' && activeDbName) {
      return renderLinkTypesContent()
    }

    // Default content for other tabs
    return (
      <Card className="card ontology-content">
        <div className="ontology-content-header">
          <Text className="ontology-content-title">{activeContent?.title || activeNavId}</Text>
          <Text className="ontology-content-description">{activeContent?.description || ''}</Text>
        </div>
        {activeContent?.details?.length ? (
          <ul className="ontology-content-list">
            {activeContent.details.map((detail) => (
              <li key={detail}>{detail}</li>
            ))}
          </ul>
        ) : null}
      </Card>
    )
  }

  if (!activeDbName) {
    return (
      <div className="page ontology-page">
        <div className="ontology-empty-state">
          <Icon icon="cube" size={48} />
          <h2>프로젝트를 선택해주세요</h2>
          <p>Files에서 프로젝트를 선택하면 온톨로지를 관리할 수 있습니다</p>
        </div>
      </div>
    )
  }

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
              {isSearchLoading && filteredResources.length === 0 ? (
                <div className="ontology-search-loading">
                  <Spinner size={18} />
                  <span>Loading resources...</span>
                </div>
              ) : filteredResources.length > 0 ? (
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
        <div className="ontology-main">{renderContent()}</div>
      </div>
    </div>
  )
}
