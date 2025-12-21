import { useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Button,
  Card,
  FormGroup,
  H5,
  HTMLSelect,
  InputGroup,
  Intent,
  TextArea,
} from '@blueprintjs/core'
import {
  createDatabase,
  deleteDatabase,
  getDatabaseExpectedSeq,
  listDatabases,
  openDatabase,
} from '../api/bff'
import { PageHeader } from '../components/PageHeader'
import { DangerConfirmDialog } from '../components/DangerConfirmDialog'
import { showAppToast } from '../app/AppToaster'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { useAppStore } from '../store/useAppStore'

export const ProjectsPage = () => {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)
  const trackCommand = useAppStore((state) => state.trackCommand)
  const setProject = useAppStore((state) => state.setProject)

  const requestContext = useMemo(
    () => ({ language: context.language, authToken, adminToken }),
    [adminToken, authToken, context.language],
  )

  const [selectedProject, setSelectedProject] = useState(context.project ?? '')
  const [createName, setCreateName] = useState('')
  const [createDescription, setCreateDescription] = useState('')
  const [deleteOpen, setDeleteOpen] = useState(false)

  const databasesQuery = useQuery({
    queryKey: qk.databases(context.language),
    queryFn: () => listDatabases(requestContext),
  })

  const expectedSeqQuery = useQuery({
    queryKey: selectedProject ? qk.databaseExpectedSeq(selectedProject) : ['bff', 'expected-seq', 'empty'],
    queryFn: () => getDatabaseExpectedSeq(requestContext, selectedProject),
    enabled: Boolean(selectedProject),
  })

  const createMutation = useMutation({
    mutationFn: () =>
      createDatabase(requestContext, {
        name: createName.trim(),
        description: createDescription.trim() || undefined,
      }),
    onSuccess: (result) => {
      if (result.commandId) {
        trackCommand({
          id: result.commandId,
          kind: 'CREATE_DATABASE',
          title: `Create ${createName.trim()}`,
          target: { dbName: createName.trim() },
          context: { project: createName.trim(), branch: context.branch },
          submittedAt: new Date().toISOString(),
          writePhase: 'SUBMITTED',
          indexPhase: 'INDEXING_PENDING',
        })
      }
      void queryClient.invalidateQueries({ queryKey: qk.databases(context.language) })
      setCreateName('')
      setCreateDescription('')
      void showAppToast({ intent: Intent.SUCCESS, message: '프로젝트 생성 요청을 전송했습니다.' })
    },
    onError: (error) => toastApiError(error, context.language),
  })

  const deleteMutation = useMutation({
    mutationFn: async (reason: string) => {
      const expectedSeq = expectedSeqQuery.data
      if (typeof expectedSeq !== 'number') {
        throw new Error('expected_seq missing')
      }
      return deleteDatabase(requestContext, selectedProject, expectedSeq, { 'X-Change-Reason': reason })
    },
    onSuccess: (result) => {
      if (result.commandId) {
        trackCommand({
          id: result.commandId,
          kind: 'DELETE_DATABASE',
          title: `Delete ${selectedProject}`,
          target: { dbName: selectedProject },
          context: { project: selectedProject, branch: context.branch },
          submittedAt: new Date().toISOString(),
          writePhase: 'SUBMITTED',
          indexPhase: 'INDEXING_PENDING',
        })
      }
      void queryClient.invalidateQueries({ queryKey: qk.databases(context.language) })
      setDeleteOpen(false)
      void showAppToast({ intent: Intent.SUCCESS, message: '삭제 요청을 전송했습니다.' })
    },
    onError: (error) => toastApiError(error, context.language),
  })

  const handleOpen = async () => {
    if (!selectedProject) {
      return
    }
    try {
      await openDatabase(requestContext, selectedProject)
      setProject(selectedProject)
      navigate(`/db/${encodeURIComponent(selectedProject)}/overview`)
    } catch (error) {
      toastApiError(error, context.language)
    }
  }

  return (
    <div>
      <PageHeader
        title="프로젝트"
        subtitle="기존 프로젝트를 선택하거나 새로운 프로젝트를 생성합니다."
      />

      <div className="db-grid">
        <Card className="db-card" elevation={1}>
          <div className="card-title">
            <H5>프로젝트 선택</H5>
            <Button minimal icon="refresh" onClick={() => databasesQuery.refetch()}>
              Refresh
            </Button>
          </div>
          <FormGroup label="프로젝트">
            <HTMLSelect
              value={selectedProject}
              onChange={(event) => setSelectedProject(event.currentTarget.value)}
              options={[
                { label: 'Select project', value: '' },
                ...(databasesQuery.data ?? []).map((name) => ({ label: name, value: name })),
              ]}
            />
          </FormGroup>
          <div className="db-actions">
            <Button icon="folder-open" intent={Intent.PRIMARY} onClick={handleOpen} disabled={!selectedProject}>
              Open
            </Button>
            <Button
              icon="trash"
              intent={Intent.DANGER}
              onClick={() => setDeleteOpen(true)}
              disabled={!selectedProject}
            >
              Delete
            </Button>
          </div>
          <div className="muted small">
            expected_seq: {expectedSeqQuery.isFetching ? '...' : expectedSeqQuery.data ?? 'n/a'}
          </div>
        </Card>

        <Card className="db-card" elevation={1}>
          <div className="card-title">
            <H5>프로젝트 생성</H5>
          </div>
          <FormGroup label="Project name">
            <InputGroup
              placeholder="e.g. demo_project"
              value={createName}
              onChange={(event) => setCreateName(event.currentTarget.value)}
            />
          </FormGroup>
          <FormGroup label="Description">
            <TextArea
              placeholder="간단한 설명"
              value={createDescription}
              onChange={(event) => setCreateDescription(event.currentTarget.value)}
            />
          </FormGroup>
          <Button
            icon="add"
            intent={Intent.PRIMARY}
            onClick={() => createMutation.mutate()}
            loading={createMutation.isPending}
            disabled={!createName.trim()}
          >
            Create project
          </Button>
        </Card>
      </div>

      <DangerConfirmDialog
        isOpen={deleteOpen}
        title="프로젝트 삭제"
        description="삭제하면 되돌릴 수 없습니다."
        confirmLabel="Delete"
        cancelLabel="Cancel"
        confirmTextToType={selectedProject}
        reasonLabel="Change reason"
        reasonPlaceholder="삭제 사유"
        typedLabel="Type project name"
        typedPlaceholder="프로젝트 이름 입력"
        onCancel={() => setDeleteOpen(false)}
        onConfirm={({ reason }) => deleteMutation.mutate(reason)}
        loading={deleteMutation.isPending}
      />
    </div>
  )
}
