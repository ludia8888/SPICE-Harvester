import { H1, Text } from '@blueprintjs/core'
import type { ReactNode } from 'react'

export const PageHeader = ({
  title,
  subtitle,
  actions,
}: {
  title: string
  subtitle?: string
  actions?: ReactNode
}) => (
  <div className="page-header">
    <div>
      <H1>{title}</H1>
      {subtitle ? <Text className="page-subtitle">{subtitle}</Text> : null}
    </div>
    {actions ? <div className="page-actions">{actions}</div> : null}
  </div>
)
