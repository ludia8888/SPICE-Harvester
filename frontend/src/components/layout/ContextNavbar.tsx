import { useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery } from '@tanstack/react-query'
import {
  Alignment,
  Button,
  HTMLSelect,
  Navbar,
  NavbarDivider,
  NavbarGroup,
  NavbarHeading,
  Tag,
} from '@blueprintjs/core'
import { listBranches, listDatabases } from '../../api/bff'
import { qk } from '../../query/queryKeys'
import { useAppStore } from '../../store/useAppStore'

export const ContextNavbar = ({ onOpenCommands }: { onOpenCommands: () => void }) => {
  const navigate = useNavigate()
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)
  const setProject = useAppStore((state) => state.setProject)
  const setBranch = useAppStore((state) => state.setBranch)
  const commands = useAppStore((state) => state.commands)

  const requestContext = useMemo(
    () => ({ language: context.language, authToken, adminToken }),
    [adminToken, authToken, context.language],
  )

  const databasesQuery = useQuery<string[]>({
    queryKey: qk.databases(context.language),
    queryFn: () => listDatabases(requestContext),
  })

  const branchesQuery = useQuery({
    queryKey: context.project ? qk.branches(context.project, context.language) : ['bff', 'branches', 'empty'],
    queryFn: () => listBranches(requestContext, context.project ?? ''),
    enabled: Boolean(context.project),
  })

  const branchOptions = useMemo<string[]>(() => {
    const list = Array.isArray((branchesQuery.data as any)?.branches)
      ? (branchesQuery.data as any).branches
      : []
    const values: string[] = list
      .map((branch: any) => branch?.name)
      .filter((name: unknown): name is string => typeof name === 'string')
    const normalized = new Set<string>(values)
    if (context.branch) {
      normalized.add(context.branch)
    }
    return Array.from(normalized)
  }, [branchesQuery.data, context.branch])

  const activeCount = useMemo(
    () =>
      Object.values(commands).filter((command) =>
        command.expired ? false : command.writePhase === 'SUBMITTED' || command.writePhase === 'WRITE_DONE',
      ).length,
    [commands],
  )

  return (
    <Navbar className="top-nav">
      <NavbarGroup align={Alignment.LEFT}>
        <NavbarHeading>SPICE Harvester</NavbarHeading>
        <NavbarDivider />
        <HTMLSelect
          minimal
          value={context.project ?? ''}
          onChange={(event) => {
            const value = event.currentTarget.value
            if (!value) {
              return
            }
            setProject(value)
            navigate(`/db/${encodeURIComponent(value)}/overview`)
          }}
          options={[
            { label: context.project ?? 'Select DB', value: context.project ?? '' },
            ...(databasesQuery.data ?? []).map((db) => ({ label: db, value: db })),
          ]}
        />
        <HTMLSelect
          minimal
          value={context.branch}
          onChange={(event) => setBranch(event.currentTarget.value)}
          options={branchOptions.map((branch) => ({ label: branch, value: branch }))}
        />
      </NavbarGroup>
      <NavbarGroup align={Alignment.RIGHT}>
        <Button minimal icon="history" onClick={onOpenCommands}>
          Commands
          {activeCount ? (
            <Tag minimal intent="primary" style={{ marginLeft: 8 }}>
              {activeCount}
            </Tag>
          ) : null}
        </Button>
      </NavbarGroup>
    </Navbar>
  )
}
