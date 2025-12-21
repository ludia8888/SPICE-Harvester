import { useMemo, useState } from 'react'
import { Button, Card, FormGroup, HTMLTable, InputGroup, Intent, Text } from '@blueprintjs/core'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { createBranch, deleteBranch, listBranches } from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { PageHeader } from '../components/layout/PageHeader'
import { showAppToast } from '../app/AppToaster'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { useAppStore } from '../store/useAppStore'

type BranchItem = Record<string, unknown>

export const BranchesPage = ({ dbName }: { dbName: string }) => {
  const queryClient = useQueryClient()
  const requestContext = useRequestContext()
  const language = useAppStore((state) => state.context.language)
  const setBranch = useAppStore((state) => state.setBranch)

  const [name, setName] = useState('')
  const [fromBranch, setFromBranch] = useState('main')

  const listQuery = useQuery({
    queryKey: qk.branches(dbName, requestContext.language),
    queryFn: () => listBranches(requestContext, dbName),
  })

  const createMutation = useMutation({
    mutationFn: () => createBranch(requestContext, dbName, { name: name.trim(), from_branch: fromBranch.trim() || 'main' }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.branches(dbName, requestContext.language) })
      void showAppToast({ intent: Intent.SUCCESS, message: `Branch created: ${name.trim()}` })
      setName('')
    },
    onError: (error) => toastApiError(error, language),
  })

  const deleteMutation = useMutation({
    mutationFn: (branchName: string) => deleteBranch(requestContext, dbName, branchName),
    onSuccess: (_payload, branchName) => {
      void queryClient.invalidateQueries({ queryKey: qk.branches(dbName, requestContext.language) })
      void showAppToast({ intent: Intent.WARNING, message: `Branch deleted: ${branchName}` })
    },
    onError: (error) => toastApiError(error, language),
  })

  const branches = useMemo(() => {
    const payload = listQuery.data ?? {}
    const list = (payload as { branches?: BranchItem[] }).branches ?? []
    return list
  }, [listQuery.data])

  const renderName = (branch: BranchItem) => {
    const nameValue =
      (branch.name as string | undefined) ||
      (branch.branch_name as string | undefined) ||
      (branch.id as string | undefined)
    return nameValue ?? 'unknown'
  }

  return (
    <div>
      <PageHeader title="Branches" subtitle="Create and manage branches for experiments." />

      <div className="card-stack">
        <Card>
          <div className="card-title">Create branch</div>
          <div className="form-row">
            <FormGroup label="Name">
              <InputGroup value={name} onChange={(event) => setName(event.currentTarget.value)} />
            </FormGroup>
            <FormGroup label="From branch">
              <InputGroup value={fromBranch} onChange={(event) => setFromBranch(event.currentTarget.value)} />
            </FormGroup>
            <Button
              intent={Intent.PRIMARY}
              onClick={() => createMutation.mutate()}
              disabled={!name.trim() || createMutation.isPending}
              loading={createMutation.isPending}
            >
              Create
            </Button>
          </div>
        </Card>

        <Card>
          <div className="card-title">
            <Text>Branches</Text>
            {listQuery.isFetching ? <Text className="muted small">Loading...</Text> : null}
          </div>
          {branches.length === 0 ? (
            <Text className="muted">No branches yet.</Text>
          ) : (
            <HTMLTable striped interactive className="command-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {branches.map((branch) => {
                  const branchName = renderName(branch)
                  return (
                    <tr key={branchName}>
                      <td>{branchName}</td>
                      <td>
                        <Button small icon="git-branch" onClick={() => setBranch(branchName)}>
                          Switch
                        </Button>
                        <Button
                          small
                          icon="trash"
                          intent={Intent.DANGER}
                          style={{ marginLeft: 8 }}
                          onClick={() => deleteMutation.mutate(branchName)}
                          disabled={deleteMutation.isPending}
                        >
                          Delete
                        </Button>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </HTMLTable>
          )}
        </Card>
      </div>
    </div>
  )
}
