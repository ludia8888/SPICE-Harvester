import { useState, useEffect, useCallback } from 'react'
import {
  Dialog,
  Button,
  InputGroup,
  TextArea,
  HTMLTable,
  Icon,
  Spinner,
  NonIdealState,
  Callout,
} from '@blueprintjs/core'
import { listUdfs, createUdf, getUdf, createUdfVersion, type UdfRecord } from '../../api/bff'

type UdfManageDialogProps = {
  isOpen: boolean
  onClose: () => void
  dbName: string
  onSelectUdf?: (udf: UdfRecord) => void
}

export const UdfManageDialog = ({
  isOpen,
  onClose,
  dbName,
  onSelectUdf,
}: UdfManageDialogProps) => {
  const [udfs, setUdfs] = useState<UdfRecord[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Create form
  const [isCreating, setIsCreating] = useState(false)
  const [newName, setNewName] = useState('')
  const [newDescription, setNewDescription] = useState('')
  const [newCode, setNewCode] = useState(`def transform(row):
    # row는 딕셔너리 형태입니다
    # 새로운 컬럼을 추가하거나 기존 값을 변환할 수 있습니다
    value = int(row.get("id") or 0)
    return {**row, "new_column": value + 1}
`)
  const [isSaving, setIsSaving] = useState(false)

  // Edit mode
  const [editingUdf, setEditingUdf] = useState<UdfRecord | null>(null)
  const [editCode, setEditCode] = useState('')
  const [isUpdating, setIsUpdating] = useState(false)

  const loadUdfs = useCallback(async () => {
    if (!dbName) return
    setIsLoading(true)
    setError(null)
    try {
      const data = await listUdfs(dbName)
      setUdfs(data)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'UDF 목록을 불러오는데 실패했습니다')
    } finally {
      setIsLoading(false)
    }
  }, [dbName])

  useEffect(() => {
    if (isOpen) {
      void loadUdfs()
    }
  }, [isOpen, loadUdfs])

  const handleCreate = async () => {
    if (!newName.trim() || !newCode.trim()) return
    setIsSaving(true)
    try {
      await createUdf(dbName, {
        name: newName.trim(),
        code: newCode,
        description: newDescription.trim() || undefined,
      })
      setNewName('')
      setNewDescription('')
      setNewCode(`def transform(row):
    value = int(row.get("id") or 0)
    return {**row, "new_column": value + 1}
`)
      setIsCreating(false)
      await loadUdfs()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'UDF 생성에 실패했습니다')
    } finally {
      setIsSaving(false)
    }
  }

  const handleEdit = async (udf: UdfRecord) => {
    try {
      const detail = await getUdf(udf.udf_id)
      setEditingUdf(detail)
      setEditCode(detail.code || '')
    } catch (err) {
      setError(err instanceof Error ? err.message : 'UDF를 불러오는데 실패했습니다')
    }
  }

  const handleUpdateVersion = async () => {
    if (!editingUdf || !editCode.trim()) return
    setIsUpdating(true)
    try {
      await createUdfVersion(editingUdf.udf_id, editCode)
      setEditingUdf(null)
      setEditCode('')
      await loadUdfs()
    } catch (err) {
      setError(err instanceof Error ? err.message : 'UDF 버전 생성에 실패했습니다')
    } finally {
      setIsUpdating(false)
    }
  }

  const handleSelect = (udf: UdfRecord) => {
    onSelectUdf?.(udf)
    onClose()
  }

  const renderContent = () => {
    if (isLoading) {
      return (
        <div style={{ padding: 40, textAlign: 'center' }}>
          <Spinner size={40} />
          <p style={{ marginTop: 16, color: '#a7b6c2' }}>UDF 목록을 불러오는 중...</p>
        </div>
      )
    }

    if (editingUdf) {
      return (
        <div className="udf-edit-form">
          <h4 style={{ margin: '0 0 16px 0' }}>
            <Icon icon="code" style={{ marginRight: 8 }} />
            {editingUdf.name} (v{editingUdf.latest_version}) 편집
          </h4>
          <Callout intent="primary" icon="info-sign" style={{ marginBottom: 16 }}>
            코드를 수정하면 새로운 버전이 생성됩니다. 기존 버전은 유지됩니다.
          </Callout>
          <TextArea
            value={editCode}
            onChange={(e) => setEditCode(e.target.value)}
            fill
            rows={12}
            style={{ fontFamily: 'monospace', fontSize: 13 }}
          />
          <div style={{ marginTop: 16, display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
            <Button text="취소" onClick={() => setEditingUdf(null)} />
            <Button
              intent="primary"
              text={`v${editingUdf.latest_version + 1} 저장`}
              onClick={handleUpdateVersion}
              loading={isUpdating}
              disabled={!editCode.trim()}
            />
          </div>
        </div>
      )
    }

    if (isCreating) {
      return (
        <div className="udf-create-form">
          <h4 style={{ margin: '0 0 16px 0' }}>
            <Icon icon="add" style={{ marginRight: 8 }} />
            새 UDF 생성
          </h4>
          <div style={{ marginBottom: 12 }}>
            <label style={{ display: 'block', marginBottom: 4, fontWeight: 500 }}>이름 *</label>
            <InputGroup
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              placeholder="예: add_computed_column"
            />
          </div>
          <div style={{ marginBottom: 12 }}>
            <label style={{ display: 'block', marginBottom: 4, fontWeight: 500 }}>설명</label>
            <InputGroup
              value={newDescription}
              onChange={(e) => setNewDescription(e.target.value)}
              placeholder="이 UDF가 하는 일을 간단히 설명하세요"
            />
          </div>
          <div style={{ marginBottom: 12 }}>
            <label style={{ display: 'block', marginBottom: 4, fontWeight: 500 }}>코드 *</label>
            <Callout intent="warning" icon="warning-sign" style={{ marginBottom: 8, fontSize: 12 }}>
              보안상 import, while, for, try/except 등은 사용할 수 없습니다.
            </Callout>
            <TextArea
              value={newCode}
              onChange={(e) => setNewCode(e.target.value)}
              fill
              rows={10}
              style={{ fontFamily: 'monospace', fontSize: 13 }}
            />
          </div>
          <div style={{ marginTop: 16, display: 'flex', gap: 8, justifyContent: 'flex-end' }}>
            <Button text="취소" onClick={() => setIsCreating(false)} />
            <Button
              intent="primary"
              text="생성"
              onClick={handleCreate}
              loading={isSaving}
              disabled={!newName.trim() || !newCode.trim()}
            />
          </div>
        </div>
      )
    }

    if (udfs.length === 0) {
      return (
        <NonIdealState
          icon="code"
          title="UDF가 없습니다"
          description="사용자 정의 함수를 만들어 파이프라인에서 사용하세요"
          action={
            <Button intent="primary" icon="add" text="새 UDF 만들기" onClick={() => setIsCreating(true)} />
          }
        />
      )
    }

    return (
      <div className="udf-list">
        <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'flex-end' }}>
          <Button intent="primary" icon="add" text="새 UDF" onClick={() => setIsCreating(true)} />
        </div>
        <HTMLTable striped interactive style={{ width: '100%' }}>
          <thead>
            <tr>
              <th>이름</th>
              <th>설명</th>
              <th>버전</th>
              <th style={{ width: 120 }}>작업</th>
            </tr>
          </thead>
          <tbody>
            {udfs.map((udf) => (
              <tr key={udf.udf_id}>
                <td>
                  <Icon icon="code" style={{ marginRight: 8, color: '#48aff0' }} />
                  {udf.name}
                </td>
                <td style={{ color: '#a7b6c2' }}>{udf.description || '-'}</td>
                <td>v{udf.latest_version}</td>
                <td>
                  <Button small minimal icon="edit" onClick={() => handleEdit(udf)} title="편집" />
                  {onSelectUdf && (
                    <Button
                      small
                      minimal
                      icon="selection"
                      intent="primary"
                      onClick={() => handleSelect(udf)}
                      title="선택"
                    />
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </HTMLTable>
      </div>
    )
  }

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title="UDF (사용자 정의 함수) 관리"
      icon="code"
      style={{ width: 700 }}
      className="bp6-dark"
    >
      <div className="bp6-dialog-body" style={{ padding: 20 }}>
        {error && (
          <Callout intent="danger" icon="error" style={{ marginBottom: 16 }}>
            {error}
            <Button small minimal icon="cross" style={{ float: 'right' }} onClick={() => setError(null)} />
          </Callout>
        )}
        {renderContent()}
      </div>
    </Dialog>
  )
}
