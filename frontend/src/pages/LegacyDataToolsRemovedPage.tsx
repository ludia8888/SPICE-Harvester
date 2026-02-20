import { Callout, Card, Intent, Text } from '@blueprintjs/core'
import { PageHeader } from '../components/layout/PageHeader'

export const LegacyDataToolsRemovedPage = () => {
  return (
    <div>
      <PageHeader
        title="Legacy data tools removed"
        subtitle="This route is no longer available in the Foundry-aligned runtime."
      />
      <Card className="card-stack">
        <Callout intent={Intent.WARNING}>
          Legacy v1 schema suggestion and Google Sheets import endpoints were removed from the backend contract.
        </Callout>
        <Text className="muted" style={{ marginTop: 12 }}>
          Use Foundry-aligned APIs and flows (for example ontology v2 routes and pipeline dataset uploads).
        </Text>
      </Card>
    </div>
  )
}
