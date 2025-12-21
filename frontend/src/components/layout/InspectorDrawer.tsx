import { useState } from 'react'
import { Drawer, Tab, Tabs, Text } from '@blueprintjs/core'
import { useAppStore } from '../../store/useAppStore'

export const InspectorDrawer = () => {
  const inspector = useAppStore((state) => state.inspector)
  const setInspector = useAppStore((state) => state.setInspector)
  const [tabId, setTabId] = useState('summary')

  const jsonText = (() => {
    if (!inspector?.data) {
      return ''
    }
    try {
      return JSON.stringify(inspector.data, null, 2)
    } catch {
      return String(inspector.data)
    }
  })()

  return (
    <Drawer
      isOpen={Boolean(inspector)}
      onClose={() => setInspector(null)}
      position="right"
      title={inspector?.title ?? 'Inspector'}
      className="inspector-drawer"
    >
      <div className="inspector-body">
        <Tabs id="inspector-tabs" selectedTabId={tabId} onChange={(value) => setTabId(value as string)}>
          <Tab id="summary" title="Summary" panel={<Text>{inspector?.kind ?? 'Details'}</Text>} />
          <Tab id="json" title="JSON" panel={<pre className="inspector-json">{jsonText}</pre>} />
        </Tabs>
      </div>
    </Drawer>
  )
}
