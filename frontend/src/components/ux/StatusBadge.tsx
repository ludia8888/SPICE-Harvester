import { Icon, Intent, Tag } from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/icons'

type StatusLevel = 'success' | 'warning' | 'danger' | 'running' | 'idle' | 'unknown'

const CONFIG: Record<StatusLevel, { icon: IconName; intent: Intent; pulse: boolean }> = {
  success: { icon: 'tick-circle', intent: Intent.SUCCESS, pulse: false },
  warning: { icon: 'warning-sign', intent: Intent.WARNING, pulse: false },
  danger:  { icon: 'error', intent: Intent.DANGER, pulse: false },
  running: { icon: 'refresh', intent: Intent.PRIMARY, pulse: true },
  idle:    { icon: 'circle', intent: Intent.NONE, pulse: false },
  unknown: { icon: 'help', intent: Intent.NONE, pulse: false },
}

type StatusBadgeProps = {
  status: StatusLevel
  label?: string
  minimal?: boolean
}

export const StatusBadge = ({ status, label, minimal = true }: StatusBadgeProps) => {
  const cfg = CONFIG[status] ?? CONFIG.unknown
  return (
    <Tag
      intent={cfg.intent}
      minimal={minimal}
      icon={<Icon icon={cfg.icon} className={cfg.pulse ? 'status-badge-pulse' : undefined} />}
    >
      {label ?? status}
    </Tag>
  )
}
