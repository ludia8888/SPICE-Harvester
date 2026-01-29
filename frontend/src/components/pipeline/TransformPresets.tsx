import { Button, Tooltip } from '@blueprintjs/core'

export type TransformPreset = {
  id: string
  label: string
  icon: string
  description: string
  transformType: string  // 백엔드 변환 타입
}

// 자주 사용하는 변환 - 백엔드 SUPPORTED_TRANSFORMS 기반
const PRESETS: TransformPreset[] = [
  {
    id: 'filter',
    label: '필터',
    icon: 'filter',
    description: '조건에 맞는 행 필터링',
    transformType: 'filter',
  },
  {
    id: 'join',
    label: '조인',
    icon: 'merge-links',
    description: '두 데이터셋 결합',
    transformType: 'join',
  },
  {
    id: 'groupBy',
    label: '그룹 집계',
    icon: 'group-objects',
    description: '그룹별 집계',
    transformType: 'groupBy',
  },
  {
    id: 'compute',
    label: '계산 열',
    icon: 'function',
    description: '새 계산 열 추가',
    transformType: 'compute',
  },
]

type TransformPresetsProps = {
  onSelectPreset: (preset: TransformPreset) => void
  disabled?: boolean
}

export const TransformPresets = ({ onSelectPreset, disabled = false }: TransformPresetsProps) => {
  return (
    <div className="pipeline-toolbox-group">
      <div className="pipeline-toolbox-button-group">
        {PRESETS.map((preset) => (
          <Tooltip key={preset.id} content={preset.description} placement="bottom">
            <Button
              minimal
              icon={preset.icon as never}
              onClick={() => onSelectPreset(preset)}
              disabled={disabled}
              className="pipeline-tool-button"
            />
          </Tooltip>
        ))}
      </div>
      <span className="pipeline-toolbox-label">Transform</span>
    </div>
  )
}
