import { useMemo } from 'react'
import { Link, useParams } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import { Card, Callout, Intent } from '@blueprintjs/core'
import { getSummary } from '../api/bff'
import { ApiErrorCallout } from '../components/ApiErrorCallout'
import { PageHeader } from '../components/PageHeader'
import { JsonView } from '../components/JsonView'
import { qk } from '../query/queryKeys'
import { useAppStore } from '../store/useAppStore'
import { asRecord, getBoolean } from '../utils/typed'

export const OverviewPage = () => {
  const { db } = useParams()
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)

  const requestContext = useMemo(
    () => ({ language: context.language, authToken, adminToken }),
    [adminToken, authToken, context.language],
  )

  const summaryQuery = useQuery({
    queryKey: qk.summary({ dbName: db ?? null, branch: context.branch, language: context.language }),
    queryFn: () => getSummary(requestContext, { dbName: db ?? null, branch: context.branch }),
    enabled: Boolean(db),
  })

  const summaryRecord = asRecord(summaryQuery.data)
  const policy = asRecord(asRecord(summaryRecord.data).policy)
  const isProtected = getBoolean(policy.is_protected_branch) ?? false
  const dbPrefix = db ? `/db/${encodeURIComponent(db)}` : ''

  return (
    <div>
      <PageHeader title="Overview" subtitle="현재 컨텍스트 요약과 정책 상태를 확인합니다." />
      {isProtected ? (
        <Callout intent={Intent.WARNING} title="Protected branch">
          이 브랜치는 보호 브랜치입니다. 중요한 변경에는 추가 인증과 사유가 필요합니다.
        </Callout>
      ) : null}

      {summaryQuery.error ? (
        <ApiErrorCallout error={summaryQuery.error} language={context.language} />
      ) : null}

      <Card elevation={1} className="section-card">
        <div className="card-title">Summary</div>
        <JsonView value={summaryQuery.data} />
      </Card>

      <Card elevation={1} className="section-card">
        <div className="card-title">Next Steps</div>
        <ol className="step-list">
          <li className="step-item">
            <div className="step-index">1</div>
            <div>
              온톨로지 정의
              {dbPrefix ? (
                <div className="muted small">
                  <Link to={`${dbPrefix}/ontology`}>Open Ontology</Link>
                </div>
              ) : null}
            </div>
          </li>
          <li className="step-item">
            <div className="step-index">2</div>
            <div>
              Sheets preview/grid 확인
              {dbPrefix ? (
                <div className="muted small">
                  <Link to={`${dbPrefix}/data/sheets`}>Open Sheets Hub</Link>
                </div>
              ) : null}
            </div>
          </li>
          <li className="step-item">
            <div className="step-index">3</div>
            <div>
              Schema suggestion & mappings
              {dbPrefix ? (
                <div className="muted small">
                  <Link to={`${dbPrefix}/data/schema-suggestion`}>Open Schema Suggestion</Link>
                </div>
              ) : null}
            </div>
          </li>
          <li className="step-item">
            <div className="step-index">4</div>
            <div>
              Dry-run → Commit
              {dbPrefix ? (
                <div className="muted small">
                  <Link to={`${dbPrefix}/data/import/sheets`}>Open Import</Link>
                </div>
              ) : null}
            </div>
          </li>
          <li className="step-item">
            <div className="step-index">5</div>
            <div>
              Graph Explorer로 검증
              {dbPrefix ? (
                <div className="muted small">
                  <Link to={`${dbPrefix}/explore/graph`}>Open Graph Explorer</Link>
                </div>
              ) : null}
            </div>
          </li>
        </ol>
      </Card>
    </div>
  )
}
