import { useState, useMemo } from 'react'
import {
  Dialog,
  Button,
  InputGroup,
  HTMLTable,
  Icon,
  Spinner,
  NonIdealState,
} from '@blueprintjs/core'
import type { DatasetRecord } from '../../api/bff'

type DatasetSelectDialogProps = {
  isOpen: boolean
  onClose: () => void
  onSelect: (dataset: DatasetRecord) => void
  datasets: DatasetRecord[]
  isLoading?: boolean
}

export const DatasetSelectDialog = ({
  isOpen,
  onClose,
  onSelect,
  datasets,
  isLoading = false,
}: DatasetSelectDialogProps) => {
  const [searchQuery, setSearchQuery] = useState('')

  const filteredDatasets = useMemo(() => {
    const query = searchQuery.trim().toLowerCase()
    if (!query) return datasets
    return datasets.filter(
      (d) =>
        d.name.toLowerCase().includes(query) ||
        (d.description || '').toLowerCase().includes(query)
    )
  }, [datasets, searchQuery])

  const handleSelect = (dataset: DatasetRecord) => {
    onSelect(dataset)
    onClose()
    setSearchQuery('')
  }

  const renderContent = () => {
    if (isLoading) {
      return (
        <div style={{ padding: 40, textAlign: 'center' }}>
          <Spinner size={40} />
          <p style={{ marginTop: 16, color: '#a7b6c2' }}>데이터셋 목록을 불러오는 중...</p>
        </div>
      )
    }

    if (datasets.length === 0) {
      return (
        <NonIdealState
          icon="database"
          title="데이터셋이 없습니다"
          description="먼저 데이터셋을 업로드하세요"
        />
      )
    }

    if (filteredDatasets.length === 0) {
      return (
        <NonIdealState
          icon="search"
          title="검색 결과 없음"
          description={`"${searchQuery}"에 해당하는 데이터셋이 없습니다`}
        />
      )
    }

    return (
      <HTMLTable striped interactive style={{ width: '100%' }}>
        <thead>
          <tr>
            <th>이름</th>
            <th>설명</th>
            <th>소스</th>
            <th style={{ width: 80 }}>선택</th>
          </tr>
        </thead>
        <tbody>
          {filteredDatasets.map((dataset) => (
            <tr key={dataset.dataset_id}>
              <td>
                <Icon icon="database" style={{ marginRight: 8, color: '#48aff0' }} />
                {dataset.name}
              </td>
              <td style={{ color: '#a7b6c2', maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis' }}>
                {dataset.description || '-'}
              </td>
              <td style={{ color: '#a7b6c2' }}>{dataset.source_type || '-'}</td>
              <td>
                <Button
                  small
                  intent="primary"
                  icon="tick"
                  onClick={() => handleSelect(dataset)}
                />
              </td>
            </tr>
          ))}
        </tbody>
      </HTMLTable>
    )
  }

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title="데이터셋 선택"
      icon="database"
      style={{ width: 700 }}
      className="bp6-dark"
    >
      <div className="bp6-dialog-body" style={{ padding: 20 }}>
        <InputGroup
          leftIcon="search"
          placeholder="데이터셋 검색..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          style={{ marginBottom: 16 }}
        />
        <div style={{ maxHeight: 400, overflowY: 'auto' }}>
          {renderContent()}
        </div>
      </div>
      <div className="bp6-dialog-footer">
        <div className="bp6-dialog-footer-actions">
          <Button text="취소" onClick={onClose} />
        </div>
      </div>
    </Dialog>
  )
}
