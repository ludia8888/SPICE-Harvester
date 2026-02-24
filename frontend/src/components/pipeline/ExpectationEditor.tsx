import { Icon, InputGroup, Menu, MenuItem, Popover, Button } from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/core'

/* ── Expectation type ─── */
export type Expectation = {
  rule: string
  column?: string
  value?: string | number
}

/* ── Rule catalog ─── */
type RuleDef = {
  rule: string
  label: string
  icon: IconName
  needsColumn: boolean
  needsValue: boolean
  valuePlaceholder?: string
  columnPlaceholder?: string
}

const RULE_CATALOG: RuleDef[] = [
  { rule: 'unique', label: 'Primary Key', icon: 'key', needsColumn: true, needsValue: false, columnPlaceholder: 'e.g. id or id,sub_id' },
  { rule: 'row_count_min', label: 'Min Row Count', icon: 'numerical', needsColumn: false, needsValue: true, valuePlaceholder: 'e.g. 100' },
  { rule: 'row_count_max', label: 'Max Row Count', icon: 'numerical', needsColumn: false, needsValue: true, valuePlaceholder: 'e.g. 1000000' },
  { rule: 'not_null', label: 'Not Null', icon: 'disable', needsColumn: true, needsValue: false, columnPlaceholder: 'e.g. email,name' },
  { rule: 'non_empty', label: 'Not Empty', icon: 'blank', needsColumn: true, needsValue: false, columnPlaceholder: 'e.g. description' },
  { rule: 'min', label: 'Min Value', icon: 'arrow-down', needsColumn: true, needsValue: true, columnPlaceholder: 'e.g. age', valuePlaceholder: 'e.g. 0' },
  { rule: 'max', label: 'Max Value', icon: 'arrow-up', needsColumn: true, needsValue: true, columnPlaceholder: 'e.g. age', valuePlaceholder: 'e.g. 150' },
  { rule: 'regex', label: 'Regex Match', icon: 'regex', needsColumn: true, needsValue: true, columnPlaceholder: 'e.g. email', valuePlaceholder: 'e.g. ^[a-z]+@.+' },
  { rule: 'in_set', label: 'In Set', icon: 'th-list', needsColumn: true, needsValue: true, columnPlaceholder: 'e.g. status', valuePlaceholder: 'e.g. active,inactive,pending' },
]

const getRuleDef = (rule: string): RuleDef =>
  RULE_CATALOG.find((r) => r.rule === rule) ?? {
    rule,
    label: rule,
    icon: 'cog' as IconName,
    needsColumn: false,
    needsValue: false,
  }

/* ── Props ─── */
type Props = {
  expectations: Expectation[]
  onChange: (exps: Expectation[]) => void
}

/* ── Component ─── */
export const ExpectationEditor = ({ expectations, onChange }: Props) => {
  const addExpectation = (rule: string) => {
    const def = getRuleDef(rule)
    const newExp: Expectation = { rule }
    if (def.needsColumn) newExp.column = ''
    if (def.needsValue) newExp.value = ''
    onChange([...expectations, newExp])
  }

  const updateExpectation = (index: number, updates: Partial<Expectation>) => {
    const updated = expectations.map((exp, i) => (i === index ? { ...exp, ...updates } : exp))
    onChange(updated)
  }

  const removeExpectation = (index: number) => {
    onChange(expectations.filter((_, i) => i !== index))
  }

  return (
    <div className="expectation-editor">
      <div className="expectation-editor-header">
        <Icon icon="shield" size={12} />
        <span>Data Expectations</span>
        <span className="expectation-editor-count">
          {expectations.length > 0 ? `${expectations.length} check${expectations.length > 1 ? 's' : ''}` : ''}
        </span>
      </div>

      {/* Expectation cards */}
      {expectations.map((exp, i) => {
        const def = getRuleDef(exp.rule)
        return (
          <div key={i} className="expectation-card">
            <div className="expectation-card-header">
              <Icon icon={def.icon} size={12} />
              <span className="expectation-card-label">{def.label}</span>
              <button
                className="expectation-card-remove"
                onClick={() => removeExpectation(i)}
                title="Remove"
              >
                <Icon icon="small-cross" size={12} />
              </button>
            </div>
            <div className="expectation-card-body">
              {def.needsColumn && (
                <InputGroup
                  small
                  placeholder={def.columnPlaceholder ?? 'Column(s)'}
                  value={String(exp.column ?? '')}
                  onChange={(e) => updateExpectation(i, { column: e.target.value })}
                  leftIcon="column-layout"
                />
              )}
              {def.needsValue && (
                <InputGroup
                  small
                  placeholder={def.valuePlaceholder ?? 'Value'}
                  value={String(exp.value ?? '')}
                  onChange={(e) => {
                    const raw = e.target.value
                    // Try to parse as number for numeric rules
                    const numVal = Number(raw)
                    const isNumericRule = ['row_count_min', 'row_count_max', 'min', 'max'].includes(exp.rule)
                    updateExpectation(i, { value: isNumericRule && !isNaN(numVal) && raw !== '' ? numVal : raw })
                  }}
                  leftIcon="edit"
                />
              )}
            </div>
          </div>
        )
      })}

      {/* Add button */}
      <Popover
        content={
          <Menu>
            {RULE_CATALOG.map((def) => (
              <MenuItem
                key={def.rule}
                icon={<Icon icon={def.icon} />}
                text={def.label}
                onClick={() => addExpectation(def.rule)}
              />
            ))}
          </Menu>
        }
        placement="bottom-start"
      >
        <Button small minimal icon="plus" className="expectation-add-button">
          Add expectation
        </Button>
      </Popover>
    </div>
  )
}
