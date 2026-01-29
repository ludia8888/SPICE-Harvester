import { useCallback, useMemo, useState } from 'react'
import {
  Button,
  FormGroup,
  InputGroup,
  NumericInput,
  Switch,
  MenuItem,
  type MenuItemProps,
} from '@blueprintjs/core'
import { Select, type ItemRendererProps } from '@blueprintjs/select'
import type { ActionParameter, ActionTypeDetail, Instance } from '../../api/bff'

type SelectOption = { value: string; label: string }

type ActionFormProps = {
  actionType: ActionTypeDetail
  instances?: Map<string, Instance[]>
  onSimulate: (params: Record<string, unknown>) => void
  onExecute: (params: Record<string, unknown>) => void
  isSimulating?: boolean
  isExecuting?: boolean
}

export const ActionForm = ({
  actionType,
  instances = new Map(),
  onSimulate,
  onExecute,
  isSimulating = false,
  isExecuting = false,
}: ActionFormProps) => {
  const [formValues, setFormValues] = useState<Record<string, unknown>>(() => {
    const initial: Record<string, unknown> = {}
    actionType.parameters.forEach((param) => {
      if (param.defaultValue !== undefined) {
        initial[param.name] = param.defaultValue
      } else if (param.type === 'boolean') {
        initial[param.name] = false
      } else if (param.type === 'number') {
        initial[param.name] = 0
      } else {
        initial[param.name] = ''
      }
    })
    return initial
  })

  const handleValueChange = useCallback((name: string, value: unknown) => {
    setFormValues((prev) => ({ ...prev, [name]: value }))
  }, [])

  const isValid = useMemo(() => {
    return actionType.parameters.every((param) => {
      if (!param.required) return true
      const value = formValues[param.name]
      if (value === undefined || value === null || value === '') return false
      return true
    })
  }, [actionType.parameters, formValues])

  const handleSimulate = useCallback(() => {
    onSimulate(formValues)
  }, [onSimulate, formValues])

  const handleExecute = useCallback(() => {
    onExecute(formValues)
  }, [onExecute, formValues])

  const renderField = (param: ActionParameter) => {
    const value = formValues[param.name]

    switch (param.type) {
      case 'number':
        return (
          <NumericInput
            value={value as number}
            onValueChange={(num) => handleValueChange(param.name, num)}
            fill
            min={0}
          />
        )

      case 'boolean':
        return (
          <Switch
            checked={value as boolean}
            onChange={(e) => handleValueChange(param.name, e.currentTarget.checked)}
            label={value ? '예' : '아니오'}
          />
        )

      case 'select':
        if (!param.options) return null
        return (
          <Select<SelectOption>
            items={param.options}
            itemRenderer={(option: SelectOption, { handleClick, modifiers }: ItemRendererProps) => (
              <MenuItem
                key={option.value}
                text={option.label}
                active={modifiers.active}
                onClick={handleClick}
                selected={option.value === value}
              />
            )}
            onItemSelect={(option: SelectOption) => handleValueChange(param.name, option.value)}
            filterable={false}
            popoverProps={{ minimal: true }}
          >
            <Button
              text={param.options.find((o) => o.value === value)?.label || '선택...'}
              rightIcon="caret-down"
              fill
            />
          </Select>
        )

      case 'instance':
        const classInstances = instances.get(param.instanceClassId || '') || []
        return (
          <Select<Instance>
            items={classInstances}
            itemRenderer={(instance: Instance, { handleClick, modifiers }: ItemRendererProps) => (
              <MenuItem
                key={instance.id}
                text={instance.label}
                active={modifiers.active}
                onClick={handleClick}
                selected={instance.id === value}
              />
            )}
            onItemSelect={(instance: Instance) => handleValueChange(param.name, instance.id)}
            filterable={classInstances.length > 10}
            itemPredicate={(query: string, instance: Instance) =>
              instance.label.toLowerCase().includes(query.toLowerCase())
            }
            noResults={<MenuItem disabled text="인스턴스가 없습니다" />}
            popoverProps={{ minimal: true }}
          >
            <Button
              text={classInstances.find((i) => i.id === value)?.label || '선택...'}
              rightIcon="caret-down"
              fill
            />
          </Select>
        )

      case 'string':
      default:
        return (
          <InputGroup
            value={value as string}
            onChange={(e) => handleValueChange(param.name, e.target.value)}
            fill
            placeholder={param.description || `${param.label} 입력...`}
          />
        )
    }
  }

  return (
    <div className="action-form">
      <div className="action-form-fields">
        {actionType.parameters.map((param) => (
          <FormGroup
            key={param.name}
            label={param.label}
            labelInfo={param.required ? '(필수)' : ''}
            helperText={param.description}
            className="action-form-field"
          >
            {renderField(param)}
          </FormGroup>
        ))}
      </div>

      <div className="action-form-actions">
        <Button
          icon="play"
          text="시뮬레이션"
          onClick={handleSimulate}
          disabled={!isValid || isExecuting}
          loading={isSimulating}
          outlined
        />
        <Button
          icon="confirm"
          text="실행"
          intent="primary"
          onClick={handleExecute}
          disabled={!isValid || isSimulating}
          loading={isExecuting}
        />
      </div>
    </div>
  )
}
