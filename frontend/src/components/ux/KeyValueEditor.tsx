import { useState, useCallback } from 'react'
import { Button, InputGroup, Intent } from '@blueprintjs/core'

type KVRow = { key: string; value: string }

type KeyValueEditorProps = {
  value: Record<string, string>
  onChange: (value: Record<string, string>) => void
  keyPlaceholder?: string
  valuePlaceholder?: string
  addLabel?: string
}

export const KeyValueEditor = ({
  value,
  onChange,
  keyPlaceholder = 'Key',
  valuePlaceholder = 'Value',
  addLabel = 'Add row',
}: KeyValueEditorProps) => {
  const rows: KVRow[] = Object.entries(value).map(([k, v]) => ({ key: k, value: v }))

  const emit = useCallback(
    (newRows: KVRow[]) => {
      const obj: Record<string, string> = {}
      for (const r of newRows) {
        const k = r.key.trim()
        if (k) obj[k] = r.value
      }
      onChange(obj)
    },
    [onChange],
  )

  const updateRow = (idx: number, field: 'key' | 'value', val: string) => {
    const next = [...rows]
    next[idx] = { ...next[idx], [field]: val }
    emit(next)
  }

  const removeRow = (idx: number) => {
    emit(rows.filter((_, i) => i !== idx))
  }

  const addRow = () => {
    emit([...rows, { key: '', value: '' }])
  }

  return (
    <div>
      {rows.map((row, i) => (
        <div className="kv-editor-row" key={i}>
          <InputGroup
            placeholder={keyPlaceholder}
            value={row.key}
            onChange={(e) => updateRow(i, 'key', e.target.value)}
          />
          <InputGroup
            placeholder={valuePlaceholder}
            value={row.value}
            onChange={(e) => updateRow(i, 'value', e.target.value)}
          />
          <Button
            minimal
            icon="cross"
            intent={Intent.DANGER}
            onClick={() => removeRow(i)}
          />
        </div>
      ))}
      <Button minimal icon="plus" intent={Intent.PRIMARY} onClick={addRow}>
        {addLabel}
      </Button>
    </div>
  )
}
