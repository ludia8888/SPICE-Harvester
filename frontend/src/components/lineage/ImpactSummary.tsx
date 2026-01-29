import { Icon, Spinner } from '@blueprintjs/core'
import type { LineageImpactResponse } from '../../api/bff'

type ImpactSummaryProps = {
  impact: LineageImpactResponse | null
  isLoading?: boolean
}

export const ImpactSummary = ({ impact, isLoading = false }: ImpactSummaryProps) => {
  if (isLoading) {
    return (
      <div className="impact-summary is-loading">
        <Spinner size={18} />
        <span>영향 분석 중...</span>
      </div>
    )
  }

  if (!impact) {
    return (
      <div className="impact-summary is-empty">
        <Icon icon="info-sign" size={14} />
        <span>항목을 선택하면 영향 분석이 표시됩니다</span>
      </div>
    )
  }

  const hasImpact = impact.affectedReports > 0 || impact.affectedPipelines > 0

  return (
    <div className="impact-summary">
      <div className="impact-summary-header">
        <Icon icon="warning-sign" size={16} />
        <span className="impact-summary-title">영향 분석</span>
      </div>

      <div className="impact-summary-body">
        <div className="impact-summary-stats">
          <div className="impact-summary-stat">
            <Icon icon="chart" size={14} />
            <span className="impact-summary-stat-value">{impact.affectedReports}</span>
            <span className="impact-summary-stat-label">개 리포트에 영향</span>
          </div>
          <div className="impact-summary-stat">
            <Icon icon="flow-branch" size={14} />
            <span className="impact-summary-stat-value">{impact.affectedPipelines}</span>
            <span className="impact-summary-stat-label">개 파이프라인에 영향</span>
          </div>
        </div>

        {impact.lastUpdated && (
          <div className="impact-summary-meta">
            <Icon icon="time" size={12} />
            <span>마지막 업데이트: {new Date(impact.lastUpdated).toLocaleString('ko-KR')}</span>
          </div>
        )}

        {hasImpact && impact.downstream && impact.downstream.length > 0 && (
          <div className="impact-summary-downstream">
            <div className="impact-summary-downstream-title">영향받는 항목</div>
            <div className="impact-summary-downstream-list">
              {impact.downstream.slice(0, 5).map((item) => (
                <div key={item.id} className="impact-summary-downstream-item">
                  <Icon
                    icon={item.type === 'report' ? 'chart' : 'flow-branch'}
                    size={12}
                  />
                  <span>{item.name}</span>
                </div>
              ))}
              {impact.downstream.length > 5 && (
                <div className="impact-summary-downstream-more">
                  +{impact.downstream.length - 5}개 더 보기
                </div>
              )}
            </div>
          </div>
        )}

        {!hasImpact && (
          <div className="impact-summary-safe">
            <Icon icon="tick-circle" size={14} />
            <span>이 데이터를 변경해도 다른 항목에 영향이 없습니다</span>
          </div>
        )}
      </div>
    </div>
  )
}
