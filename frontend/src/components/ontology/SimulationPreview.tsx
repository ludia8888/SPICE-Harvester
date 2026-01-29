import { Icon, Button, Callout } from '@blueprintjs/core'
import type { SimulationResult } from '../../api/bff'

type SimulationPreviewProps = {
  result: SimulationResult | null
  onConfirm: () => void
  onCancel: () => void
  isExecuting?: boolean
}

const formatValue = (value: unknown): string => {
  if (value === null || value === undefined) {
    return '-'
  }
  if (typeof value === 'number') {
    return value.toLocaleString('ko-KR')
  }
  if (typeof value === 'boolean') {
    return value ? '예' : '아니오'
  }
  return String(value)
}

export const SimulationPreview = ({
  result,
  onConfirm,
  onCancel,
  isExecuting = false,
}: SimulationPreviewProps) => {
  if (!result) {
    return null
  }

  const hasErrors = result.errors && result.errors.length > 0
  const hasWarnings = result.warnings && result.warnings.length > 0
  const hasChanges = result.changes && result.changes.length > 0

  return (
    <div className="simulation-preview">
      <div className="simulation-preview-header">
        <Icon icon="eye-open" size={16} />
        <span className="simulation-preview-title">시뮬레이션 결과 (미리보기)</span>
      </div>

      {hasErrors && (
        <Callout intent="danger" icon="error" className="simulation-preview-callout">
          <div className="simulation-preview-errors">
            {result.errors?.map((error, index) => (
              <div key={index}>{error}</div>
            ))}
          </div>
        </Callout>
      )}

      {hasWarnings && (
        <Callout intent="warning" icon="warning-sign" className="simulation-preview-callout">
          <div className="simulation-preview-warnings">
            {result.warnings?.map((warning, index) => (
              <div key={index}>{warning}</div>
            ))}
          </div>
        </Callout>
      )}

      {hasChanges ? (
        <div className="simulation-preview-changes">
          <table className="simulation-preview-table">
            <thead>
              <tr>
                <th>항목</th>
                <th>필드</th>
                <th>변경 전</th>
                <th>변경 후</th>
                <th>차이</th>
              </tr>
            </thead>
            <tbody>
              {result.changes.map((change, index) => {
                const beforeNum = typeof change.before === 'number' ? change.before : null
                const afterNum = typeof change.after === 'number' ? change.after : null
                const diff =
                  beforeNum !== null && afterNum !== null ? afterNum - beforeNum : null

                return (
                  <tr key={index}>
                    <td className="simulation-preview-entity">{change.entityLabel}</td>
                    <td>{change.field}</td>
                    <td className="simulation-preview-before">
                      {formatValue(change.before)}
                    </td>
                    <td className="simulation-preview-after">{formatValue(change.after)}</td>
                    <td
                      className={`simulation-preview-diff ${
                        diff !== null && diff > 0
                          ? 'is-positive'
                          : diff !== null && diff < 0
                          ? 'is-negative'
                          : ''
                      }`}
                    >
                      {diff !== null
                        ? diff > 0
                          ? `+${diff.toLocaleString('ko-KR')}`
                          : diff.toLocaleString('ko-KR')
                        : '-'}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="simulation-preview-no-changes">
          <Icon icon="info-sign" size={14} />
          <span>변경 사항이 없습니다</span>
        </div>
      )}

      <div className="simulation-preview-actions">
        <Button text="취소" onClick={onCancel} disabled={isExecuting} />
        <Button
          icon="tick"
          text="확인하고 실행"
          intent="primary"
          onClick={onConfirm}
          disabled={hasErrors || isExecuting}
          loading={isExecuting}
        />
      </div>
    </div>
  )
}
