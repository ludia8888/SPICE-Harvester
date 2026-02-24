import { useState } from 'react'
import {
  Button,
  Callout,
  Dialog,
  DialogBody,
  DialogFooter,
  FormGroup,
  HTMLSelect,
  InputGroup,
  Intent,
} from '@blueprintjs/core'

type Props = {
  isOpen: boolean
  onClose: () => void
  onDeploy: (schedule?: { interval_seconds?: number; cron?: string }) => void
  loading?: boolean
}

const CRON_PRESETS = [
  { label: 'Custom...', value: '' },
  { label: 'Every hour', value: '0 * * * *' },
  { label: 'Every 6 hours', value: '0 */6 * * *' },
  { label: 'Daily at midnight', value: '0 0 * * *' },
  { label: 'Daily at 6 AM', value: '0 6 * * *' },
  { label: 'Weekly (Sunday midnight)', value: '0 0 * * 0' },
  { label: 'Monthly (1st at midnight)', value: '0 0 1 * *' },
]

/** Basic 5-field cron validation matching backend _is_valid_cron_expression */
function isValidCron(expr: string): boolean {
  const parts = expr.trim().split(/\s+/)
  if (parts.length !== 5) return false
  return parts.every((field) => {
    if (field === '*') return true
    return field.split(',').every((option) => {
      const opt = option.trim()
      if (!opt) return false
      if (opt === '*') return true
      if (opt.startsWith('*/')) {
        const step = Number(opt.slice(2))
        return Number.isInteger(step) && step > 0
      }
      // a-b or a-b/n
      const dashMatch = opt.match(/^(\d+)-(\d+)(?:\/(\d+))?$/)
      if (dashMatch) {
        const a = Number(dashMatch[1])
        const b = Number(dashMatch[2])
        return a <= b && (!dashMatch[3] || Number(dashMatch[3]) > 0)
      }
      // plain number
      return /^\d+$/.test(opt)
    })
  })
}

export const ScheduleDialog = ({ isOpen, onClose, onDeploy, loading }: Props) => {
  const [scheduleType, setScheduleType] = useState<'none' | 'interval' | 'cron'>('none')
  const [intervalMinutes, setIntervalMinutes] = useState('')
  const [cronExpr, setCronExpr] = useState('')
  const [cronPreset, setCronPreset] = useState('')

  const intervalSeconds = intervalMinutes ? Math.round(Number(intervalMinutes) * 60) : 0
  const isValidInterval = intervalSeconds > 0
  const isValidCronExpr = cronExpr.trim() ? isValidCron(cronExpr.trim()) : false

  const canDeploy =
    scheduleType === 'none' ||
    (scheduleType === 'interval' && isValidInterval) ||
    (scheduleType === 'cron' && isValidCronExpr)

  const handleDeploy = () => {
    if (scheduleType === 'none') {
      onDeploy(undefined)
    } else if (scheduleType === 'interval') {
      onDeploy({ interval_seconds: intervalSeconds })
    } else {
      onDeploy({ cron: cronExpr.trim() })
    }
  }

  return (
    <Dialog isOpen={isOpen} onClose={onClose} title="Deploy Pipeline" icon="rocket-slant">
      <DialogBody>
        <FormGroup label="Schedule" labelFor="schedule-type">
          <HTMLSelect
            id="schedule-type"
            value={scheduleType}
            onChange={(e) => setScheduleType(e.target.value as 'none' | 'interval' | 'cron')}
            fill
            options={[
              { value: 'none', label: 'No schedule (one-time deploy)' },
              { value: 'interval', label: 'Repeating interval' },
              { value: 'cron', label: 'Cron expression' },
            ]}
          />
        </FormGroup>

        {scheduleType === 'interval' && (
          <FormGroup
            label="Interval (minutes)"
            labelFor="schedule-interval"
            helperText={intervalSeconds > 0 ? `= ${intervalSeconds.toLocaleString()} seconds` : undefined}
          >
            <InputGroup
              id="schedule-interval"
              type="number"
              value={intervalMinutes}
              placeholder="e.g. 60"
              onChange={(e) => setIntervalMinutes(e.target.value)}
              min={1}
            />
          </FormGroup>
        )}

        {scheduleType === 'cron' && (
          <>
            <FormGroup label="Preset" labelFor="cron-preset">
              <HTMLSelect
                id="cron-preset"
                value={cronPreset}
                onChange={(e) => {
                  setCronPreset(e.target.value)
                  if (e.target.value) setCronExpr(e.target.value)
                }}
                fill
                options={CRON_PRESETS}
              />
            </FormGroup>
            <FormGroup
              label="Cron Expression (5-field)"
              labelFor="cron-expr"
              helperText="Format: minute hour day-of-month month day-of-week"
            >
              <InputGroup
                id="cron-expr"
                value={cronExpr}
                placeholder="e.g. 0 */6 * * *"
                onChange={(e) => {
                  setCronExpr(e.target.value)
                  setCronPreset('')
                }}
                intent={cronExpr.trim() && !isValidCronExpr ? Intent.DANGER : Intent.NONE}
              />
            </FormGroup>
            {cronExpr.trim() && !isValidCronExpr && (
              <Callout intent={Intent.WARNING} compact icon="warning-sign">
                Invalid cron expression. Use 5 fields: minute hour day month weekday.
                Supports: *, */n, a-b, a-b/n, n, and comma-separated values.
              </Callout>
            )}
          </>
        )}
      </DialogBody>
      <DialogFooter
        actions={
          <>
            <Button onClick={onClose}>Cancel</Button>
            <Button
              intent={Intent.PRIMARY}
              icon="rocket-slant"
              loading={loading}
              disabled={!canDeploy}
              onClick={handleDeploy}
            >
              Deploy{scheduleType !== 'none' ? ' with Schedule' : ''}
            </Button>
          </>
        }
      />
    </Dialog>
  )
}
