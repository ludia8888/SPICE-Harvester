import { Icon, Spinner } from '@blueprintjs/core'
import type { KeyboardEvent } from 'react'
import type { ExplorerResult } from '../../state/store'

type ResultsTableProps = {
  results: ExplorerResult[]
  columns: string[]
  selectedId: string | null
  onSelect: (result: ExplorerResult) => void
  isLoading?: boolean
  totalCount?: number
}

const formatCellValue = (value: unknown): string => {
  if (value === null || value === undefined) {
    return '-'
  }
  if (typeof value === 'string') {
    return value
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value)
  }
  if (value instanceof Date) {
    return value.toLocaleDateString('ko-KR')
  }
  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

export const ResultsTable = ({
  results,
  columns,
  selectedId,
  onSelect,
  isLoading = false,
  totalCount,
}: ResultsTableProps) => {
  if (isLoading) {
    return (
      <div className="results-table-loading">
        <Spinner size={24} />
        <span>검색 중...</span>
      </div>
    )
  }

  if (results.length === 0) {
    return (
      <div className="results-table-empty">
        <Icon icon="search" size={24} />
        <span className="results-table-empty-title">검색 결과가 없습니다</span>
        <span className="results-table-empty-subtitle">
          다른 검색어로 시도하거나 클래스 필터를 조정해보세요
        </span>
      </div>
    )
  }

  const displayColumns = columns.length > 0 ? columns : ['label']

  return (
    <div className="results-table-container">
      {totalCount !== undefined && (
        <div className="results-table-header-row">
          <div className="results-table-count">
            총 <strong>{totalCount.toLocaleString()}</strong>건
          </div>
        </div>
      )}
      <div className="results-table-wrapper">
        <table className="results-table">
          <thead>
            <tr>
              <th>클래스</th>
              {displayColumns.map((col) => (
                <th key={col}>{col}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {results.map((result) => (
              <tr
                key={result.id}
                className={selectedId === result.id ? 'is-selected' : ''}
                onClick={() => onSelect(result)}
                tabIndex={0}
                onKeyDown={(event: KeyboardEvent<HTMLTableRowElement>) => {
                  if (event.key === 'Enter' || event.key === ' ') {
                    event.preventDefault()
                    onSelect(result)
                  }
                }}
              >
                <td>
                  <Icon icon="cube" size={12} />
                  <span>{result.className}</span>
                </td>
                {displayColumns.map((col) => (
                  <td key={col}>
                    {col === 'label'
                      ? result.label
                      : formatCellValue(result.properties[col])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
