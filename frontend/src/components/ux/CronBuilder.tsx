import { useCallback, useMemo } from 'react'
import { Button, FormGroup, HTMLSelect, Intent, Tag } from '@blueprintjs/core'

type CronBuilderProps = {
  value: string
  onChange: (cron: string) => void
}

const PRESETS: { label: string; cron: string }[] = [
  { label: 'Every minute', cron: '* * * * *' },
  { label: 'Every hour',   cron: '0 * * * *' },
  { label: 'Every day (midnight)', cron: '0 0 * * *' },
  { label: 'Every week (Mon)', cron: '0 0 * * 1' },
  { label: 'Every month (1st)', cron: '0 0 1 * *' },
]

const MINUTES  = ['*', '0', '5', '10', '15', '20', '30', '45']
const HOURS    = ['*', ...Array.from({ length: 24 }, (_, i) => String(i))]
const DAYS     = ['*', ...Array.from({ length: 31 }, (_, i) => String(i + 1))]
const MONTHS   = ['*', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
const WEEKDAYS = ['*', '0 (Sun)', '1 (Mon)', '2 (Tue)', '3 (Wed)', '4 (Thu)', '5 (Fri)', '6 (Sat)']

const parseCron = (cron: string) => {
  const parts = cron.trim().split(/\s+/)
  return {
    minute:  parts[0] ?? '*',
    hour:    parts[1] ?? '*',
    day:     parts[2] ?? '*',
    month:   parts[3] ?? '*',
    weekday: parts[4] ?? '*',
  }
}

const describeCron = (cron: string): string => {
  const { minute, hour, day, month, weekday } = parseCron(cron)
  const parts: string[] = []
  if (minute === '*' && hour === '*') parts.push('Every minute')
  else if (minute !== '*' && hour === '*') parts.push(`At minute ${minute} of every hour`)
  else if (minute !== '*' && hour !== '*') parts.push(`At ${hour}:${minute.padStart(2, '0')}`)
  else parts.push(`At minute ${minute}`)
  if (day !== '*') parts.push(`on day ${day}`)
  if (month !== '*') parts.push(`of month ${month}`)
  if (weekday !== '*') parts.push(`on weekday ${weekday.split(' ')[0]}`)
  return parts.join(' ')
}

export const CronBuilder = ({ value, onChange }: CronBuilderProps) => {
  const parsed = useMemo(() => parseCron(value), [value])

  const update = useCallback(
    (field: string, val: string) => {
      const clean = val.split(' ')[0]
      const next = { ...parsed, [field]: clean }
      onChange(`${next.minute} ${next.hour} ${next.day} ${next.month} ${next.weekday}`)
    },
    [parsed, onChange],
  )

  return (
    <div>
      <div className="cron-builder-grid">
        <FormGroup label="Minute" style={{ marginBottom: 0 }}>
          <HTMLSelect
            fill
            value={parsed.minute}
            options={MINUTES}
            onChange={(e) => update('minute', e.target.value)}
          />
        </FormGroup>
        <FormGroup label="Hour" style={{ marginBottom: 0 }}>
          <HTMLSelect
            fill
            value={parsed.hour}
            options={HOURS}
            onChange={(e) => update('hour', e.target.value)}
          />
        </FormGroup>
        <FormGroup label="Day" style={{ marginBottom: 0 }}>
          <HTMLSelect
            fill
            value={parsed.day}
            options={DAYS}
            onChange={(e) => update('day', e.target.value)}
          />
        </FormGroup>
        <FormGroup label="Month" style={{ marginBottom: 0 }}>
          <HTMLSelect
            fill
            value={parsed.month}
            options={MONTHS}
            onChange={(e) => update('month', e.target.value)}
          />
        </FormGroup>
        <FormGroup label="Weekday" style={{ marginBottom: 0 }}>
          <HTMLSelect
            fill
            value={WEEKDAYS.find((w) => w.startsWith(parsed.weekday)) ?? '*'}
            options={WEEKDAYS}
            onChange={(e) => update('weekday', e.target.value)}
          />
        </FormGroup>
      </div>

      <div className="cron-builder-presets">
        {PRESETS.map((p) => (
          <Tag
            key={p.cron}
            minimal
            interactive
            intent={value.trim() === p.cron ? Intent.PRIMARY : Intent.NONE}
            onClick={() => onChange(p.cron)}
          >
            {p.label}
          </Tag>
        ))}
      </div>

      <div className="cron-builder-desc">{describeCron(value)}</div>
    </div>
  )
}
