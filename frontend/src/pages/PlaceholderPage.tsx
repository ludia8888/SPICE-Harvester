import { Card, H3, Text } from '@blueprintjs/core'

export const PlaceholderPage = ({ title }: { title: string }) => (
  <div className="page">
    <H3>{title}</H3>
    <Card className="card">
      <Text>Content coming soon.</Text>
    </Card>
  </div>
)
