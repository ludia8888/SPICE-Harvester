import { useEffect, useMemo, useState, type ReactNode } from 'react'
import { Link } from 'react-router-dom'
import { Button, Callout, Card, H5, HTMLTable, Tab, Tabs, Tag, Text } from '@blueprintjs/core'
import { useAppStore } from '../../store/useAppStore'
import { JsonView } from '../JsonView'

export const InspectorPanel = () => {
  const inspector = useAppStore((state) => state.inspector)
  const clearInspector = useAppStore((state) => state.clearInspector)
  const context = useAppStore((state) => state.context)
  const [tabId, setTabId] = useState('summary')

  useEffect(() => {
    setTabId('summary')
  }, [inspector])

  const data = inspector?.data as any
  const commandId =
    inspector?.auditCommandId ??
    data?.command_id ??
    data?.commandId ??
    data?.metadata?.command_id ??
    null
  const lineageRoot =
    inspector?.lineageRootId ??
    data?.node_id ??
    data?.id ??
    data?.instance_id ??
    null

  const summaryFields = useMemo(() => {
    if (!inspector) {
      return []
    }
    const fields: Array<{ label: string; value: string | ReactNode }> = []
    if (inspector.kind) {
      fields.push({ label: 'Type', value: inspector.kind })
    }
    const id =
      data?.id ??
      data?.node_id ??
      data?.instance_id ??
      data?.command_id ??
      data?.commandId ??
      null
    if (id) {
      fields.push({ label: 'ID', value: String(id) })
    }
    if (data?.data_status) {
      fields.push({ label: 'Data status', value: <Tag minimal>{String(data.data_status)}</Tag> })
    }
    if (data?.status) {
      fields.push({ label: 'Status', value: String(data.status) })
    }
    return fields
  }, [data, inspector])

  const auditLink =
    context.project && commandId
      ? `/db/${encodeURIComponent(context.project)}/audit?command_id=${encodeURIComponent(String(commandId))}`
      : null
  const lineageLink =
    context.project && lineageRoot
      ? `/db/${encodeURIComponent(context.project)}/lineage?root=${encodeURIComponent(String(lineageRoot))}`
      : null

  return (
    <aside className="inspector-panel">
      <Card className="inspector-card" elevation={0}>
        <div className="inspector-header">
          <H5>Inspector</H5>
          {inspector ? (
            <Button minimal icon="cross" onClick={clearInspector} />
          ) : null}
        </div>
        {!inspector ? (
          <Text className="muted">Select an item to inspect.</Text>
        ) : (
          <div className="inspector-body">
            <div className="inspector-title">{inspector.title}</div>
            {inspector.subtitle ? <div className="inspector-subtitle">{inspector.subtitle}</div> : null}
            <Tabs id="inspector-tabs" selectedTabId={tabId} onChange={(value) => setTabId(value as string)}>
              <Tab id="summary" title="Summary" />
              <Tab id="json" title="JSON" />
              <Tab id="audit" title="Audit" />
              <Tab id="lineage" title="Lineage" />
            </Tabs>
            {tabId === 'summary' ? (
              summaryFields.length ? (
                <HTMLTable className="full-width">
                  <tbody>
                    {summaryFields.map((field) => (
                      <tr key={field.label}>
                        <td className="muted">{field.label}</td>
                        <td>{field.value}</td>
                      </tr>
                    ))}
                  </tbody>
                </HTMLTable>
              ) : (
                <Text className="muted">No summary available.</Text>
              )
            ) : null}
            {tabId === 'json' ? <JsonView value={inspector.data} fallback="No details." /> : null}
            {tabId === 'audit' ? (
              auditLink ? (
                <Link to={auditLink}>
                  <Button minimal icon="document-open">
                    Open Audit
                  </Button>
                </Link>
              ) : (
                <Callout intent="primary">command_id가 없습니다.</Callout>
              )
            ) : null}
            {tabId === 'lineage' ? (
              lineageLink ? (
                <Link to={lineageLink}>
                  <Button minimal icon="diagram-tree">
                    Open Lineage
                  </Button>
                </Link>
              ) : (
                <Callout intent="primary">lineage root가 없습니다.</Callout>
              )
            ) : null}
          </div>
        )}
      </Card>
    </aside>
  )
}
