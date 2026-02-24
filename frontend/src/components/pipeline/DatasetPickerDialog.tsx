import { useState, useMemo } from 'react'
import {
  Button,
  Checkbox,
  Dialog,
  DialogBody,
  DialogFooter,
  InputGroup,
  Intent,
  Spinner,
  Tag,
} from '@blueprintjs/core'
import { useQuery } from '@tanstack/react-query'
import type { RequestContext, DatasetRecord } from '../../api/bff'
import { listPipelineDatasets } from '../../api/bff'

type Props = {
  isOpen: boolean
  onClose: () => void
  onAdd: (datasets: DatasetRecord[]) => void
  ctx: RequestContext
  dbName: string
  branch: string
}

export const DatasetPickerDialog = ({ isOpen, onClose, onAdd, ctx, dbName, branch }: Props) => {
  const [search, setSearch] = useState('')
  const [selected, setSelected] = useState<Set<string>>(new Set())

  const listQ = useQuery({
    queryKey: ['pipeline-datasets-picker', dbName, branch],
    queryFn: () => listPipelineDatasets(ctx, { db_name: dbName, branch }),
    enabled: isOpen && !!dbName,
  })

  const datasets: DatasetRecord[] = Array.isArray(listQ.data) ? listQ.data : []

  const filtered = useMemo(() => {
    if (!search) return datasets
    const lower = search.toLowerCase()
    return datasets.filter((d) => d.name.toLowerCase().includes(lower))
  }, [datasets, search])

  const toggle = (id: string) => {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  const handleAdd = () => {
    const picked = datasets.filter((d) => selected.has(d.dataset_id))
    onAdd(picked)
    setSelected(new Set())
    setSearch('')
    onClose()
  }

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title="Add Datasets"
      icon="database"
      style={{ width: 520 }}
    >
      <DialogBody>
        <InputGroup
          leftIcon="search"
          placeholder="Search datasets..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          style={{ marginBottom: 12 }}
        />

        {listQ.isLoading && <Spinner size={20} />}
        {listQ.error && <div style={{ color: 'var(--red5)' }}>Failed to load datasets.</div>}

        <div style={{ maxHeight: 320, overflow: 'auto' }}>
          {filtered.map((ds) => (
            <div
              key={ds.dataset_id}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 8,
                padding: '6px 0',
                borderBottom: '1px solid var(--foundry-border)',
              }}
            >
              <Checkbox
                checked={selected.has(ds.dataset_id)}
                onChange={() => toggle(ds.dataset_id)}
                style={{ margin: 0 }}
              />
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontWeight: 500, fontSize: 13 }}>{ds.name}</div>
                <div style={{ fontSize: 11, color: 'var(--foundry-text-muted)' }}>
                  <Tag minimal>{ds.source_type}</Tag>
                  {ds.row_count != null && <span style={{ marginLeft: 8 }}>{ds.row_count} rows</span>}
                </div>
              </div>
            </div>
          ))}
          {filtered.length === 0 && !listQ.isLoading && (
            <div style={{ padding: 16, textAlign: 'center', color: 'var(--foundry-text-muted)' }}>
              No datasets found.
            </div>
          )}
        </div>
      </DialogBody>
      <DialogFooter
        actions={
          <>
            <Button onClick={onClose}>Cancel</Button>
            <Button
              intent={Intent.PRIMARY}
              icon="plus"
              disabled={selected.size === 0}
              onClick={handleAdd}
            >
              Add {selected.size > 0 ? `${selected.size} Dataset${selected.size > 1 ? 's' : ''}` : 'Selected'}
            </Button>
          </>
        }
      />
    </Dialog>
  )
}
