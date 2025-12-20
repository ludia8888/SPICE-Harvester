import { useEffect, useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import {
  Button,
  Callout,
  Card,
  FormGroup,
  HTMLTable,
  HTMLSelect,
  InputGroup,
  Intent,
  Text,
  TextArea,
} from '@blueprintjs/core'
import { getSummary, listBranches, resolveMerge, simulateMerge } from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { useAppStore } from '../store/useAppStore'

export const MergePage = ({ dbName }: { dbName: string }) => {
  const requestContext = useRequestContext()
  const language = useAppStore((state) => state.context.language)
  const branch = useAppStore((state) => state.context.branch)
  const adminToken = useAppStore((state) => state.adminToken)
  const adminMode = useAppStore((state) => state.adminMode)

  const [sourceBranch, setSourceBranch] = useState(branch)
  const [targetBranch, setTargetBranch] = useState('main')
  const [message, setMessage] = useState('')
  const [author, setAuthor] = useState('')
  const [resolutionSelections, setResolutionSelections] = useState<
    Record<string, { optionId: string; manualValue: string }>
  >({})
  const [manualErrors, setManualErrors] = useState<Record<string, string>>({})

  const branchesQuery = useQuery({
    queryKey: qk.branches(dbName, requestContext.language),
    queryFn: () => listBranches(requestContext, dbName),
  })

  const simulateMutation = useMutation({
    mutationFn: () => simulateMerge(requestContext, dbName, {
      source_branch: sourceBranch,
      target_branch: targetBranch,
      strategy: 'merge',
    }),
    onError: (error) => toastApiError(error, language),
  })

  const resolveMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) => resolveMerge(requestContext, dbName, payload),
    onError: (error) => toastApiError(error, language),
  })

  const branchOptions = (() => {
    const payload = branchesQuery.data as { branches?: Array<Record<string, unknown>> } | undefined
    const list = payload?.branches ?? []
    return list.map((item) => {
      const name = (item.name as string | undefined) || (item.branch_name as string | undefined) || ''
      return { label: name, value: name }
    })
  })()

  const mergePreview = (() => {
    const payload = simulateMutation.data as { data?: { merge_preview?: Record<string, unknown> }; merge_preview?: Record<string, unknown> } | undefined
    return payload?.data?.merge_preview ?? payload?.merge_preview ?? null
  })()

  const conflicts = (mergePreview?.conflicts as Array<Record<string, unknown>> | undefined) ?? []

  useEffect(() => {
    if (conflicts.length === 0) {
      return
    }
    setResolutionSelections((prev) => {
      const next = { ...prev }
      conflicts.forEach((conflict) => {
        const id = String(conflict.id ?? '')
        if (!id || next[id]) {
          return
        }
        const options = getConflictOptionList(conflict)
        const recommended = options.find((option) => option.recommended) ?? options[0]
        if (recommended?.id) {
          next[id] = { optionId: String(recommended.id), manualValue: '' }
        }
      })
      return next
    })
  }, [conflicts])

  const summaryQuery = useQuery({
    queryKey: qk.summary({ dbName, branch: targetBranch, language: requestContext.language }),
    queryFn: () => getSummary(requestContext, { dbName, branch: targetBranch }),
    enabled: Boolean(targetBranch),
  })

  const isProtected = Boolean(
    (summaryQuery.data as { data?: { policy?: { is_protected_branch?: boolean } } } | undefined)?.data
      ?.policy?.is_protected_branch,
  )

  const canResolve = !isProtected || (adminMode && adminToken)

  const getConflictPath = (conflict: Record<string, unknown>) => {
    const path = conflict.path as { raw?: string; human_readable?: string } | string | undefined
    if (typeof path === 'string') return path
    if (path && typeof path === 'object') {
      return String(path.human_readable || path.raw || '')
    }
    return ''
  }

  const getConflictRawPath = (conflict: Record<string, unknown>) => {
    const path = conflict.path as { raw?: string } | string | undefined
    if (typeof path === 'string') return path
    if (path && typeof path === 'object') {
      return String(path.raw || '')
    }
    return ''
  }

  const getConflictOptionList = (conflict: Record<string, unknown>) => {
    const resolution = conflict.resolution as { options?: Array<Record<string, unknown>> } | undefined
    const options = resolution?.options ?? []
    if (options.length > 0) {
      return options
    }
    return [
      { id: 'use_source', label: 'Use source', value: (conflict.sides as { source?: { value?: unknown } })?.source?.value },
      { id: 'use_target', label: 'Use target', value: (conflict.sides as { target?: { value?: unknown } })?.target?.value },
      { id: 'manual_merge', label: 'Manual value', value: null },
    ]
  }

  const parseManualValue = (raw: string) => {
    if (!raw.trim()) {
      return null
    }
    try {
      return JSON.parse(raw)
    } catch {
      return raw
    }
  }

  const handleResolve = () => {
    const errors: Record<string, string> = {}
    const resolutions = conflicts.map((conflict) => {
      const conflictId = String(conflict.id ?? '')
      const selection = resolutionSelections[conflictId]
      if (!selection?.optionId) {
        errors[conflictId] = 'Select a resolution option.'
        return null
      }
      const options = getConflictOptionList(conflict)
      const option = options.find((item) => item.id === selection.optionId)
      if (!option) {
        errors[conflictId] = 'Invalid resolution option.'
        return null
      }
      const resolvedValue =
        option.id === 'manual_merge'
          ? parseManualValue(selection.manualValue)
          : option.value
      if (option.id === 'manual_merge' && selection.manualValue.trim() && resolvedValue === null) {
        errors[conflictId] = 'Invalid JSON value.'
        return null
      }
      return {
        path: getConflictRawPath(conflict) || getConflictPath(conflict),
        resolution_type: 'use_value',
        resolved_value: resolvedValue,
        metadata: {
          conflict_id: conflictId,
          option_id: option.id,
        },
      }
    })

    if (Object.keys(errors).length) {
      setManualErrors(errors)
      return
    }
    setManualErrors({})
    resolveMutation.mutate({
      source_branch: sourceBranch,
      target_branch: targetBranch,
      strategy: 'merge',
      message: message || undefined,
      author: author || undefined,
      resolutions: resolutions.filter((item): item is Record<string, unknown> => Boolean(item)),
    })
  }

  return (
    <div>
      <PageHeader title="Merge" subtitle={`Branch context: ${branch}`} />

      <div className="page-grid two-col">
        <Card className="card-stack">
          <FormGroup label="Source branch">
            <HTMLSelect
              options={[{ label: 'Select source', value: '' }, ...branchOptions]}
              value={sourceBranch}
              onChange={(event) => setSourceBranch(event.currentTarget.value)}
            />
          </FormGroup>
          <FormGroup label="Target branch">
            <HTMLSelect
              options={[{ label: 'Select target', value: '' }, ...branchOptions]}
              value={targetBranch}
              onChange={(event) => setTargetBranch(event.currentTarget.value)}
            />
          </FormGroup>
          <div className="form-row">
            <Button intent={Intent.PRIMARY} onClick={() => simulateMutation.mutate()} disabled={!sourceBranch || !targetBranch} loading={simulateMutation.isPending}>
              Simulate
            </Button>
          </div>
          <JsonViewer value={simulateMutation.data} empty="Simulation results will appear here." />
        </Card>

        <Card className="card-stack">
          {isProtected && !canResolve ? (
            <Callout intent={Intent.WARNING}>
              Protected branch. Admin token + Admin mode required to resolve.
            </Callout>
          ) : null}
          <FormGroup label="Merge message (optional)">
            <InputGroup value={message} onChange={(event) => setMessage(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Author (optional)">
            <InputGroup value={author} onChange={(event) => setAuthor(event.currentTarget.value)} />
          </FormGroup>
          {conflicts.length === 0 ? (
            <Callout intent={Intent.PRIMARY}>Run simulation to load conflicts.</Callout>
          ) : (
            <HTMLTable striped interactive className="command-table">
              <thead>
                <tr>
                  <th>Path</th>
                  <th>Source</th>
                  <th>Target</th>
                  <th>Resolution</th>
                  <th>Manual value</th>
                </tr>
              </thead>
              <tbody>
                {conflicts.map((conflict, index) => {
                  const conflictId = String(conflict.id ?? `conflict-${index}`)
                  const path = getConflictPath(conflict) || getConflictRawPath(conflict) || conflictId
                  const sides = conflict.sides as { source?: { preview?: string; value?: unknown }; target?: { preview?: string; value?: unknown } } | undefined
                  const sourcePreview = sides?.source?.preview ?? JSON.stringify(sides?.source?.value ?? '')
                  const targetPreview = sides?.target?.preview ?? JSON.stringify(sides?.target?.value ?? '')
                  const options = getConflictOptionList(conflict)
                  const selection = resolutionSelections[conflictId] ?? { optionId: '', manualValue: '' }
                  const manualEnabled = selection.optionId === 'manual_merge'
                  return (
                    <tr key={conflictId}>
                      <td>{path}</td>
                      <td>{sourcePreview}</td>
                      <td>{targetPreview}</td>
                      <td>
                        <HTMLSelect
                          options={[{ label: 'Select option', value: '' }, ...options.map((option) => ({
                            label: String(option.label ?? option.id ?? 'Option'),
                            value: String(option.id ?? ''),
                          }))]}
                          value={selection.optionId}
                          onChange={(event) => {
                            const next = { ...resolutionSelections }
                            next[conflictId] = { optionId: event.currentTarget.value, manualValue: selection.manualValue }
                            setResolutionSelections(next)
                            setManualErrors((prev) => {
                              const updated = { ...prev }
                              delete updated[conflictId]
                              return updated
                            })
                          }}
                        />
                        {manualErrors[conflictId] ? (
                          <Text className="small error">{manualErrors[conflictId]}</Text>
                        ) : null}
                      </td>
                      <td>
                        <TextArea
                          value={selection.manualValue}
                          onChange={(event) => {
                            const next = { ...resolutionSelections }
                            next[conflictId] = { optionId: selection.optionId, manualValue: event.currentTarget.value }
                            setResolutionSelections(next)
                          }}
                          disabled={!manualEnabled}
                          rows={3}
                          placeholder={manualEnabled ? 'JSON value' : 'Select "Manual value" to edit'}
                        />
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </HTMLTable>
          )}
          <div className="form-row">
            <Button
              intent={Intent.PRIMARY}
              onClick={handleResolve}
              disabled={!sourceBranch || !targetBranch || conflicts.length === 0 || !canResolve}
              loading={resolveMutation.isPending}
            >
              Resolve merge
            </Button>
            <Button
              onClick={() => setResolutionSelections({})}
              disabled={conflicts.length === 0}
            >
              Reset selections
            </Button>
          </div>
          <JsonViewer value={resolveMutation.data} empty="Resolution response will appear here." />
        </Card>
      </div>
    </div>
  )
}
