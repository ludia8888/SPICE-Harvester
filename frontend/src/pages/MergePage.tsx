import { useMemo, useState } from 'react'
import { useParams } from 'react-router-dom'
import { useMutation, useQuery } from '@tanstack/react-query'
import {
  Button,
  Callout,
  Card,
  FormGroup,
  H5,
  HTMLSelect,
  HTMLTable,
  InputGroup,
  Intent,
} from '@blueprintjs/core'
import { getSummary, listBranches, resolveMerge, simulateMerge, type BranchListResponse } from '../api/bff'
import { PageHeader } from '../components/PageHeader'
import { JsonView } from '../components/JsonView'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { useAppStore } from '../store/useAppStore'
import { asArray, asRecord, getBoolean } from '../utils/typed'

const parseResolvedValue = (value: string) => {
  if (!value.trim()) {
    return null
  }
  try {
    return JSON.parse(value)
  } catch {
    return value
  }
}

export const MergePage = () => {
  const { db } = useParams()
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)

  const [sourceBranch, setSourceBranch] = useState('')
  const [targetBranch, setTargetBranch] = useState('main')
  const [resolutions, setResolutions] = useState<Array<{ path: string; resolution_type: string; resolved_value: string }>>([])
  const [simulateResult, setSimulateResult] = useState<unknown>(null)
  const [changeReason, setChangeReason] = useState('')
  const [adminActor, setAdminActor] = useState('')

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

  const branchesQuery = useQuery<BranchListResponse>({
    queryKey: db ? qk.branches(db, context.language) : ['bff', 'branches', 'empty'],
    queryFn: () => listBranches(requestContext, db ?? ''),
    enabled: Boolean(db),
  })

  const branches = useMemo(
    () => branchesQuery.data?.branches ?? [],
    [branchesQuery.data],
  )
  const branchNames = useMemo(
    () =>
      branches
        .map((branch) => (typeof branch === 'string' ? branch : branch?.name))
        .filter((name): name is string => typeof name === 'string' && name.length > 0),
    [branches],
  )

  const simulateMutation = useMutation({
    mutationFn: () =>
      simulateMerge(requestContext, db ?? '', {
        source_branch: sourceBranch,
        target_branch: targetBranch,
        strategy: 'merge',
      }),
    onSuccess: (result) => setSimulateResult(result),
    onError: (error) => toastApiError(error, context.language),
  })

  const resolveMutation = useMutation({
    mutationFn: () => {
      const actor = adminActor.trim()
      const headers = isProtected
        ? {
            'X-Change-Reason': changeReason.trim(),
            ...(actor ? { 'X-Admin-Actor': actor } : {}),
          }
        : undefined
      return resolveMerge(
        requestContext,
        db ?? '',
        {
          source_branch: sourceBranch,
          target_branch: targetBranch,
          resolutions: resolutions.map((row) => ({
            path: row.path,
            resolution_type: row.resolution_type,
            resolved_value: parseResolvedValue(row.resolved_value),
          })),
        },
        headers,
      )
    },
    onSuccess: () => {
      setResolutions([])
    },
    onError: (error) => toastApiError(error, context.language),
  })

  const conflicts = asArray<unknown>(
    asRecord(asRecord(asRecord(simulateResult).data).merge_preview).conflicts,
  )

  return (
    <div>
      <PageHeader title="Merge" subtitle="브랜치 병합 충돌 시뮬레이션 및 해결" />

      {isProtected ? (
        <Callout intent={Intent.WARNING} title="Protected branch">
          보호 브랜치에서는 Admin token과 사유가 필요합니다.
        </Callout>
      ) : null}

      <Card elevation={1} className="section-card">
        <div className="form-row">
          <FormGroup label="Source">
            <HTMLSelect
              value={sourceBranch}
              onChange={(event) => setSourceBranch(event.currentTarget.value)}
              options={[{ label: 'Select source', value: '' }, ...branchNames.map((name) => ({ label: name, value: name }))]}
            />
          </FormGroup>
          <FormGroup label="Target">
            <HTMLSelect
              value={targetBranch}
              onChange={(event) => setTargetBranch(event.currentTarget.value)}
              options={[{ label: 'Select target', value: '' }, ...branchNames.map((name) => ({ label: name, value: name }))]}
            />
          </FormGroup>
          <Button intent={Intent.PRIMARY} onClick={() => simulateMutation.mutate()} disabled={!sourceBranch || !targetBranch}>
            Simulate
          </Button>
        </div>
        <JsonView value={simulateResult} />
      </Card>

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>Conflicts</H5>
          <Button minimal icon="add" onClick={() => setResolutions((prev) => [...prev, { path: '', resolution_type: 'use_value', resolved_value: '' }])}>
            Add resolution row
          </Button>
        </div>
        <HTMLTable striped className="full-width">
          <thead>
            <tr>
              <th>Path</th>
              <th>Resolution type</th>
              <th>Resolved value</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {resolutions.map((row, index) => (
              <tr key={`${row.path}-${index}`}>
                <td>
                  <InputGroup
                    value={row.path}
                    onChange={(event) => {
                      const value = event.currentTarget.value
                      setResolutions((prev) => {
                        const next = [...prev]
                        next[index] = { ...next[index], path: value }
                        return next
                      })
                    }}
                  />
                </td>
                <td>
                  <HTMLSelect
                    value={row.resolution_type}
                    onChange={(event) => {
                      const value = event.currentTarget.value
                      setResolutions((prev) => {
                        const next = [...prev]
                        next[index] = { ...next[index], resolution_type: value }
                        return next
                      })
                    }}
                    options={['use_value', 'use_source', 'use_target', 'remove']}
                  />
                </td>
                <td>
                  <InputGroup
                    value={row.resolved_value}
                    onChange={(event) => {
                      const value = event.currentTarget.value
                      setResolutions((prev) => {
                        const next = [...prev]
                        next[index] = { ...next[index], resolved_value: value }
                        return next
                      })
                    }}
                  />
                </td>
                <td>
                  <Button
                    minimal
                    icon="trash"
                    onClick={() => setResolutions((prev) => prev.filter((_, i) => i !== index))}
                  />
                </td>
              </tr>
            ))}
          </tbody>
        </HTMLTable>
        {conflicts.length ? <JsonView value={conflicts} /> : null}
        {isProtected ? (
          <div className="form-row">
            <InputGroup
              placeholder="Change reason"
              value={changeReason}
              onChange={(event) => setChangeReason(event.currentTarget.value)}
            />
            <InputGroup
              placeholder="Admin actor (optional)"
              value={adminActor}
              onChange={(event) => setAdminActor(event.currentTarget.value)}
            />
          </div>
        ) : null}
        <Button
          intent={Intent.DANGER}
          onClick={() => resolveMutation.mutate()}
          disabled={!sourceBranch || !targetBranch || resolutions.length === 0 || (isProtected && (!changeReason.trim() || !adminToken))}
        >
          Resolve
        </Button>
      </Card>
    </div>
  )
}
