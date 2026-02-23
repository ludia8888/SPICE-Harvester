import { FormGroup, InputGroup, NumericInput, HTMLSelect, Switch } from '@blueprintjs/core'
import { Tooltip } from '@blueprintjs/core'

type ParamSpec = {
  name: string
  type: 'string' | 'number' | 'boolean' | 'date' | 'select'
  required?: boolean
  description?: string
  options?: string[]
  defaultValue?: string | number | boolean
}

type ActionParameterFormProps = {
  params: ParamSpec[]
  values: Record<string, unknown>
  onChange: (values: Record<string, unknown>) => void
}

export const ActionParameterForm = ({
  params,
  values,
  onChange,
}: ActionParameterFormProps) => {
  const update = (name: string, val: unknown) => {
    onChange({ ...values, [name]: val })
  }

  return (
    <div className="action-param-form">
      {params.map((p) => {
        const label = (
          <span className="tooltip-label">
            {p.name}
            {p.required && <span style={{ color: '#db3737' }}> *</span>}
            {p.description && (
              <Tooltip content={p.description} placement="top">
                <span style={{ cursor: 'help', opacity: 0.5, fontSize: 11 }}>?</span>
              </Tooltip>
            )}
          </span>
        )

        const val = values[p.name] ?? p.defaultValue ?? ''

        switch (p.type) {
          case 'number':
            return (
              <FormGroup key={p.name} label={label}>
                <NumericInput
                  fill
                  value={typeof val === 'number' ? val : Number(val) || 0}
                  onValueChange={(n) => update(p.name, n)}
                  placeholder={`Enter ${p.name}`}
                />
              </FormGroup>
            )
          case 'boolean':
            return (
              <FormGroup key={p.name} label={label}>
                <Switch
                  checked={Boolean(val)}
                  onChange={(e) => update(p.name, (e.target as HTMLInputElement).checked)}
                />
              </FormGroup>
            )
          case 'select':
            return (
              <FormGroup key={p.name} label={label}>
                <HTMLSelect
                  fill
                  value={String(val)}
                  options={p.options ?? []}
                  onChange={(e) => update(p.name, e.target.value)}
                />
              </FormGroup>
            )
          case 'date':
            return (
              <FormGroup key={p.name} label={label}>
                <InputGroup
                  type="date"
                  value={String(val)}
                  onChange={(e) => update(p.name, e.target.value)}
                  placeholder="YYYY-MM-DD"
                />
              </FormGroup>
            )
          default:
            return (
              <FormGroup key={p.name} label={label}>
                <InputGroup
                  fill
                  value={String(val)}
                  onChange={(e) => update(p.name, e.target.value)}
                  placeholder={`Enter ${p.name}`}
                />
              </FormGroup>
            )
        }
      })}
    </div>
  )
}
