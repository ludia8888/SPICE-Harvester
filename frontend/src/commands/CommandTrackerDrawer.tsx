import { useEffect, useMemo, useState, type ReactNode } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  Button,
  Callout,
  Drawer,
  HTMLTable,
  InputGroup,
  Intent,
  Tab,
  Tabs,
  Tag,
  Text,
} from '@blueprintjs/core'
import { getCommandStatus, HttpError } from '../api/bff'
import { qk } from '../query/queryKeys'
import { useAppStore, type TrackedCommand } from '../store/useAppStore'

export type CommandDrawerCopy = {
  title: string
  tabs: {
    active: string
    completed: string
    failed: string
    expired: string
  }
  addLabel: string
  addPlaceholder: string
  addButton: string
  clearExpired: string
  removeLabel: string
  noGlobalList: string
  emptyState: string
  columns: {
    id: string
    status: string
    context: string
    submitted: string
    actions: string
  }
  detailsTitle: string
  detailsHint: string
  detailsTokenHint: string
  detailsStatusLabel: string
  detailsErrorLabel: string
  detailsResultLabel: string
  detailsContextLabel: string
  detailsSubmittedLabel: string
  detailsUnknown: string
}

type CommandTab = 'active' | 'completed' | 'failed' | 'expired'

const isCompleted = (command: TrackedCommand) =>
  command.writePhase === 'WRITE_DONE' && command.indexPhase === 'VISIBLE_IN_SEARCH'

const isFailed = (command: TrackedCommand) =>
  command.writePhase === 'FAILED' || command.writePhase === 'CANCELLED'

const isActive = (command: TrackedCommand) => !command.expired && !isFailed(command) && !isCompleted(command)

const formatTimestamp = (value: string, fallback: string, language: string) => {
  const date = new Date(value)
  if (Number.isNaN(date.valueOf())) {
    return value || fallback
  }
  try {
    return date.toLocaleString(language)
  } catch {
    return date.toISOString()
  }
}

const getStatusTag = (command: TrackedCommand) => {
  if (command.expired) {
    return { label: 'EXPIRED', intent: Intent.WARNING }
  }
  if (command.writePhase === 'FAILED') {
    return { label: 'FAILED', intent: Intent.DANGER }
  }
  if (command.writePhase === 'CANCELLED') {
    return { label: 'CANCELLED', intent: Intent.WARNING }
  }
  if (command.writePhase === 'WRITE_DONE') {
    if (command.indexPhase === 'VISIBLE_IN_SEARCH') {
      return { label: 'COMPLETED', intent: Intent.SUCCESS }
    }
    return { label: command.status ?? 'INDEXING', intent: Intent.PRIMARY }
  }
  return { label: command.status ?? 'PENDING', intent: Intent.PRIMARY }
}

export const CommandTrackerDrawer = ({
  isOpen,
  onClose,
  copy,
}: {
  isOpen: boolean
  onClose: () => void
  copy: CommandDrawerCopy
}) => {
  const context = useAppStore((state) => state.context)
  const adminToken = useAppStore((state) => state.adminToken)
  const commands = useAppStore((state) => state.commands)
  const trackCommand = useAppStore((state) => state.trackCommand)
  const removeCommand = useAppStore((state) => state.removeCommand)

  const [tabId, setTabId] = useState<CommandTab>('active')
  const [inputValue, setInputValue] = useState('')
  const [selectedId, setSelectedId] = useState<string | null>(null)

  useEffect(() => {
    if (selectedId && !commands[selectedId]) {
      setSelectedId(null)
    }
  }, [commands, selectedId])

  const commandList = useMemo(
    () => Object.values(commands).sort((a, b) => b.submittedAt.localeCompare(a.submittedAt)),
    [commands],
  )

  const filtered = useMemo(() => {
    switch (tabId) {
      case 'completed':
        return commandList.filter((command) => isCompleted(command))
      case 'failed':
        return commandList.filter((command) => isFailed(command))
      case 'expired':
        return commandList.filter((command) => command.expired)
      default:
        return commandList.filter((command) => isActive(command))
    }
  }, [commandList, tabId])

  const requestContext = useMemo(
    () => ({ language: context.language, adminToken }),
    [adminToken, context.language],
  )

  const selectedQuery = useQuery({
    queryKey: selectedId ? qk.commandStatus(selectedId, context.language) : ['command-status', 'none'],
    queryFn: () => getCommandStatus(requestContext, selectedId ?? ''),
    enabled: Boolean(selectedId && adminToken && isOpen),
    retry: false,
  })

  const handleAdd = () => {
    const trimmed = inputValue.trim()
    if (!trimmed) {
      return
    }
    if (!commands[trimmed]) {
      trackCommand({
        id: trimmed,
        kind: 'UNKNOWN',
        target: { dbName: context.project ?? '' },
        context: { project: context.project, branch: context.branch },
        submittedAt: new Date().toISOString(),
        writePhase: 'SUBMITTED',
        indexPhase: 'UNKNOWN',
      })
    }
    setSelectedId(trimmed)
    setInputValue('')
    setTabId('active')
  }

  const handleClearExpired = () => {
    commandList
      .filter((command) => command.expired)
      .forEach((command) => removeCommand(command.id))
  }

  const selectedCommand = selectedId ? commands[selectedId] : null
  const selectedError = selectedQuery.error
  const selectedStatus = selectedQuery.data
  const statusTag = selectedCommand ? getStatusTag(selectedCommand) : null

  const detailContext =
    selectedCommand?.context.project || selectedCommand?.context.branch
      ? `${selectedCommand?.context.project ?? copy.detailsUnknown} / ${selectedCommand?.context.branch ?? copy.detailsUnknown}`
      : copy.detailsUnknown

  return (
    <Drawer
      isOpen={isOpen}
      onClose={onClose}
      position="right"
      title={copy.title}
      className="command-drawer"
    >
      <div className="command-drawer-body">
        <Callout className="command-callout" icon="info-sign" intent={Intent.PRIMARY}>
          {copy.noGlobalList}
        </Callout>

        <div className="command-toolbar">
          <FormGroupShim label={copy.addLabel}>
            <InputGroup
              className="command-input"
              placeholder={copy.addPlaceholder}
              value={inputValue}
              onChange={(event) => setInputValue(event.currentTarget.value)}
              onKeyDown={(event) => {
                if (event.key === 'Enter') {
                  handleAdd()
                }
              }}
            />
          </FormGroupShim>
          <Button intent={Intent.PRIMARY} icon="plus" onClick={handleAdd}>
            {copy.addButton}
          </Button>
          <Button minimal icon="trash" onClick={handleClearExpired}>
            {copy.clearExpired}
          </Button>
        </div>

        <Tabs id="command-tabs" selectedTabId={tabId} onChange={(value) => setTabId(value as CommandTab)}>
          <Tab id="active" title={copy.tabs.active} />
          <Tab id="completed" title={copy.tabs.completed} />
          <Tab id="failed" title={copy.tabs.failed} />
          <Tab id="expired" title={copy.tabs.expired} />
        </Tabs>

        {filtered.length === 0 ? (
          <Text className="command-empty">{copy.emptyState}</Text>
        ) : (
          <HTMLTable className="command-table" striped interactive>
            <thead>
              <tr>
                <th>{copy.columns.id}</th>
                <th>{copy.columns.status}</th>
                <th>{copy.columns.context}</th>
                <th>{copy.columns.submitted}</th>
                <th>{copy.columns.actions}</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((command) => {
                const tag = getStatusTag(command)
                const isSelected = command.id === selectedId
                return (
                  <tr
                    key={command.id}
                    className={isSelected ? 'command-row is-selected' : 'command-row'}
                    onClick={() => setSelectedId(command.id)}
                  >
                    <td className="command-id">{command.id}</td>
                    <td>
                      <Tag minimal intent={tag.intent}>
                        {tag.label}
                      </Tag>
                    </td>
                    <td>
                      {command.context.project ?? copy.detailsUnknown}
                      <div className="command-meta">{command.context.branch}</div>
                    </td>
                    <td className="command-meta">
                      {formatTimestamp(command.submittedAt, copy.detailsUnknown, context.language)}
                    </td>
                    <td>
                      <Button
                        minimal
                        icon="cross"
                        onClick={(event) => {
                          event.stopPropagation()
                          removeCommand(command.id)
                        }}
                      >
                        {copy.removeLabel}
                      </Button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </HTMLTable>
        )}

        <div className="command-details">
          <div className="command-details-title">{copy.detailsTitle}</div>
          {!selectedCommand ? (
            <Text className="command-meta">{copy.detailsHint}</Text>
          ) : !adminToken ? (
            <Text className="command-meta">{copy.detailsTokenHint}</Text>
          ) : selectedError instanceof HttpError && selectedError.status === 404 ? (
            <Text className="command-meta">{copy.tabs.expired}</Text>
          ) : selectedQuery.isFetching ? (
            <Text className="command-meta">...</Text>
          ) : (
            <div className="command-details-grid">
              <div className="command-details-label">{copy.detailsStatusLabel}</div>
              <div>
                {statusTag ? (
                  <Tag minimal intent={statusTag.intent}>
                    {statusTag.label}
                  </Tag>
                ) : (
                  copy.detailsUnknown
                )}
              </div>
              <div className="command-details-label">{copy.detailsContextLabel}</div>
              <div>{detailContext}</div>
              <div className="command-details-label">{copy.detailsSubmittedLabel}</div>
              <div>
                {selectedCommand.submittedAt
                  ? formatTimestamp(selectedCommand.submittedAt, copy.detailsUnknown, context.language)
                  : copy.detailsUnknown}
              </div>
              <div className="command-details-label">{copy.detailsErrorLabel}</div>
              <div className="command-details-value">
                {selectedStatus?.error ?? selectedCommand.error ?? copy.detailsUnknown}
              </div>
              <div className="command-details-label">{copy.detailsResultLabel}</div>
              <div className="command-details-value">
                {selectedStatus?.result ? JSON.stringify(selectedStatus.result, null, 2) : copy.detailsUnknown}
              </div>
            </div>
          )}
        </div>
      </div>
    </Drawer>
  )
}

const FormGroupShim = ({ label, children }: { label: string; children: ReactNode }) => (
  <label className="command-form-group">
    <div className="command-form-label">{label}</div>
    {children}
  </label>
)
