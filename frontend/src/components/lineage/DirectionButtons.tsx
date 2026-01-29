import { Button, ButtonGroup, Icon } from '@blueprintjs/core'

export type LineageDirection = 'upstream' | 'downstream' | 'both'

type DirectionButtonsProps = {
  direction: LineageDirection
  onDirectionChange: (direction: LineageDirection) => void
  disabled?: boolean
}

const directionConfig: Array<{
  value: LineageDirection
  label: string
  icon: string
  description: string
}> = [
  {
    value: 'upstream',
    label: '원본 추적',
    icon: 'arrow-left',
    description: '이 데이터가 어디서 왔는지 추적',
  },
  {
    value: 'downstream',
    label: '사용처 추적',
    icon: 'arrow-right',
    description: '이 데이터가 어디에 사용되는지 추적',
  },
  {
    value: 'both',
    label: '전체 보기',
    icon: 'arrows-horizontal',
    description: '전체 계보 보기',
  },
]

export const DirectionButtons = ({
  direction,
  onDirectionChange,
  disabled = false,
}: DirectionButtonsProps) => {
  return (
    <div className="direction-buttons">
      <label className="direction-buttons-label">추적 방향</label>
      <ButtonGroup className="direction-buttons-group">
        {directionConfig.map((config) => (
          <Button
            key={config.value}
            icon={config.icon as never}
            text={config.label}
            active={direction === config.value}
            onClick={() => onDirectionChange(config.value)}
            disabled={disabled}
            className="direction-button"
          />
        ))}
      </ButtonGroup>
      <div className="direction-buttons-description">
        <Icon icon="info-sign" size={12} />
        <span>{directionConfig.find((c) => c.value === direction)?.description}</span>
      </div>
    </div>
  )
}
