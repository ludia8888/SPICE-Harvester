import { useMemo, useState } from 'react'
import { useParams } from 'react-router-dom'
import { useMutation, useQuery } from '@tanstack/react-query'
import {
  Button,
  Card,
  FormGroup,
  H5,
  HTMLSelect,
  HTMLTable,
  InputGroup,
  Intent,
} from '@blueprintjs/core'
import { createBranch, deleteBranch, listBranches, type BranchListResponse } from '../api/bff'
import { PageHeader } from '../components/PageHeader'
import { showAppToast } from '../app/AppToaster'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { useAppStore } from '../store/useAppStore'

export const BranchesPage = () => {
  const { db } = useParams()
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)
  const setBranch = useAppStore((state) => state.setBranch)

  const [branchName, setBranchName] = useState('')
  const [fromBranch, setFromBranch] = useState('main')

  const requestContext = useMemo(
    () => ({ language: context.language, authToken, adminToken }),
    [adminToken, authToken, context.language],
  )

  const branchesQuery = useQuery<BranchListResponse>({
    queryKey: db ? qk.branches(db, context.language) : ['bff', 'branches', 'empty'],
    queryFn: () => listBranches(requestContext, db ?? ''),
    enabled: Boolean(db),
  })

  const createMutation = useMutation({
    mutationFn: () =>
      createBranch(requestContext, db ?? '', { name: branchName.trim(), from_branch: fromBranch }),
    onSuccess: () => {
      void branchesQuery.refetch()
      setBranchName('')
      void showAppToast({ intent: Intent.SUCCESS, message: '브랜치 생성 완료' })
    },
    onError: (error) => toastApiError(error, context.language),
  })

  const deleteMutation = useMutation({
    mutationFn: (name: string) => deleteBranch(requestContext, db ?? '', name),
    onSuccess: () => {
      void branchesQuery.refetch()
      void showAppToast({ intent: Intent.SUCCESS, message: '브랜치 삭제 완료' })
    },
    onError: (error) => toastApiError(error, context.language),
  })

  const branchNames = useMemo(() => {
    const branches = branchesQuery.data?.branches ?? []
    return branches
      .map((branch) => (typeof branch === 'string' ? branch : branch?.name))
      .filter((name): name is string => typeof name === 'string' && name.length > 0)
  }, [branchesQuery.data])

  return (
    <div>
      <PageHeader title="Branches" subtitle="브랜치를 생성하고 전환합니다." />

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>브랜치 생성</H5>
        </div>
        <div className="form-row">
          <FormGroup label="Branch name">
            <InputGroup value={branchName} onChange={(event) => setBranchName(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="From branch">
            <HTMLSelect
              value={fromBranch}
              onChange={(event) => setFromBranch(event.currentTarget.value)}
              options={['main', ...branchNames]}
            />
          </FormGroup>
          <Button
            intent={Intent.PRIMARY}
            icon="git-branch"
            onClick={() => createMutation.mutate()}
            disabled={!branchName.trim()}
            loading={createMutation.isPending}
          >
            Create Branch
          </Button>
        </div>
      </Card>

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>브랜치 목록</H5>
          <Button minimal icon="refresh" onClick={() => branchesQuery.refetch()}>
            Refresh
          </Button>
        </div>
        <HTMLTable striped interactive className="full-width">
          <thead>
            <tr>
              <th>Branch</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {branchNames.map((name) => {
              const isActive = name === context.branch
              return (
                <tr key={name}>
                  <td>{name}</td>
                  <td>
                    <Button
                      minimal
                      icon="git-branch"
                      disabled={isActive}
                      onClick={() => {
                        setBranch(name)
                        void showAppToast({ intent: Intent.SUCCESS, message: `브랜치 전환: ${name}` })
                      }}
                    >
                      Switch to
                    </Button>
                    <Button
                      minimal
                      icon="trash"
                      intent={Intent.DANGER}
                      onClick={() => deleteMutation.mutate(name)}
                    >
                      Delete
                    </Button>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </HTMLTable>
      </Card>
    </div>
  )
}
