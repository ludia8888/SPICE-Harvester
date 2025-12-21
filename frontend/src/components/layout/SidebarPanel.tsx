import { Link, useLocation } from 'react-router-dom'
import { Card, Icon, Tag } from '@blueprintjs/core'
import { useAppStore } from '../../store/useAppStore'

export const SidebarPanel = () => {
  const context = useAppStore((state) => state.context)
  const location = useLocation()

  const db = context.project
  const steps = [
    { id: 1, title: '프로젝트 설정', description: '프로젝트 생성 또는 선택', to: '/' },
    { id: 2, title: '브랜치 설정', description: '브랜치 생성 또는 선택', to: db ? `/db/${encodeURIComponent(db)}/branches` : '/' },
    { id: 3, title: '온톨로지', description: '클래스/관계 정의', to: db ? `/db/${encodeURIComponent(db)}/ontology` : '/' },
    { id: 4, title: '매핑', description: '라벨 매핑 정리', to: db ? `/db/${encodeURIComponent(db)}/mappings` : '/' },
    { id: 5, title: '임포트', description: '시트/엑셀 적재', to: db ? `/db/${encodeURIComponent(db)}/data/import/sheets` : '/' },
    { id: 6, title: '그래프', description: '그래프 검증', to: db ? `/db/${encodeURIComponent(db)}/explore/graph` : '/' },
  ]

  return (
    <aside className="sidebar-panel">
      <div>
        <div className="sidebar-title">DATA SOURCE</div>
        <Card className="source-card" elevation={0}>
          <div className="source-name">Local stack</div>
          <div className="source-meta">BFF + OMS + Funnel</div>
          <div className="source-path">/api/v1</div>
        </Card>
      </div>

      <div>
        <div className="sidebar-title">PROJECT</div>
        <Card className="project-card" elevation={0}>
          <Icon icon="database" />
          <div>
            <div className="project-name">{db ?? 'No project'}</div>
            <div className="project-meta">
              {db ? (
                <>
                  <Tag minimal>{context.branch}</Tag>
                </>
              ) : (
                'Select or create a project'
              )}
            </div>
          </div>
        </Card>
      </div>

      <div>
        <div className="sidebar-title">FLOW</div>
        <ul className="step-list">
          {steps.map((step) => {
            const isActive = location.pathname === step.to
            return (
              <li key={step.id} className={`step-item ${isActive ? 'is-active' : ''}`}>
                <div className="step-index">{step.id}</div>
                <div>
                  <Link to={step.to} className="step-title">
                    {step.title}
                  </Link>
                  <div className="step-desc">{step.description}</div>
                </div>
              </li>
            )
          })}
        </ul>
      </div>
    </aside>
  )
}
