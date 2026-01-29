import { useState, useCallback } from 'react'
import {
  Button,
  Dialog,
  DialogBody,
  DialogFooter,
  FormGroup,
  InputGroup,
  MenuItem,
  Icon,
  ProgressBar,
} from '@blueprintjs/core'
import { Select, type ItemRendererProps } from '@blueprintjs/select'

type WizardStep = 'input' | 'transform' | 'output' | 'review'

type DatasetOption = {
  id: string
  name: string
}

type WizardState = {
  inputType: 'dataset' | 'file' | 'api' | null
  selectedDataset: DatasetOption | null
  transformGoal: string
  outputType: 'object-type' | 'export' | null
  outputName: string
}

type PipelineWizardDialogProps = {
  isOpen: boolean
  onClose: () => void
  onComplete: (config: WizardState) => void
  datasets?: DatasetOption[]
}

const STEPS: Array<{ id: WizardStep; label: string; icon: string }> = [
  { id: 'input', label: '입력', icon: 'import' },
  { id: 'transform', label: '변환', icon: 'changes' },
  { id: 'output', label: '출력', icon: 'export' },
  { id: 'review', label: '확인', icon: 'tick' },
]

export const PipelineWizardDialog = ({
  isOpen,
  onClose,
  onComplete,
  datasets = [],
}: PipelineWizardDialogProps) => {
  const [currentStep, setCurrentStep] = useState<WizardStep>('input')
  const [state, setState] = useState<WizardState>({
    inputType: null,
    selectedDataset: null,
    transformGoal: '',
    outputType: null,
    outputName: '',
  })

  const currentStepIndex = STEPS.findIndex((s) => s.id === currentStep)
  const progress = (currentStepIndex + 1) / STEPS.length

  const handleNext = useCallback(() => {
    const nextIndex = currentStepIndex + 1
    if (nextIndex < STEPS.length) {
      setCurrentStep(STEPS[nextIndex].id)
    }
  }, [currentStepIndex])

  const handleBack = useCallback(() => {
    const prevIndex = currentStepIndex - 1
    if (prevIndex >= 0) {
      setCurrentStep(STEPS[prevIndex].id)
    }
  }, [currentStepIndex])

  const handleComplete = useCallback(() => {
    onComplete(state)
    onClose()
  }, [state, onComplete, onClose])

  const canProceed = (() => {
    switch (currentStep) {
      case 'input':
        return state.inputType !== null && (state.inputType !== 'dataset' || state.selectedDataset !== null)
      case 'transform':
        return state.transformGoal.trim().length > 0
      case 'output':
        return state.outputType !== null && state.outputName.trim().length > 0
      case 'review':
        return true
      default:
        return false
    }
  })()

  const renderInputStep = () => (
    <div className="wizard-step">
      <h3 className="wizard-step-title">데이터 소스 선택</h3>
      <p className="wizard-step-desc">파이프라인의 입력 데이터를 선택하세요.</p>

      <div className="wizard-options">
        <button
          type="button"
          className={`wizard-option ${state.inputType === 'dataset' ? 'is-selected' : ''}`}
          onClick={() => setState((s) => ({ ...s, inputType: 'dataset' }))}
        >
          <Icon icon="database" size={24} />
          <span className="wizard-option-label">데이터셋</span>
          <span className="wizard-option-desc">기존 데이터셋 사용</span>
        </button>
        <button
          type="button"
          className={`wizard-option ${state.inputType === 'file' ? 'is-selected' : ''}`}
          onClick={() => setState((s) => ({ ...s, inputType: 'file' }))}
        >
          <Icon icon="document" size={24} />
          <span className="wizard-option-label">파일</span>
          <span className="wizard-option-desc">파일 업로드</span>
        </button>
        <button
          type="button"
          className={`wizard-option ${state.inputType === 'api' ? 'is-selected' : ''}`}
          onClick={() => setState((s) => ({ ...s, inputType: 'api' }))}
        >
          <Icon icon="cloud" size={24} />
          <span className="wizard-option-label">API</span>
          <span className="wizard-option-desc">외부 API 연결</span>
        </button>
      </div>

      {state.inputType === 'dataset' && (
        <FormGroup label="데이터셋 선택" className="wizard-form-group">
          <Select<DatasetOption>
            items={datasets}
            itemRenderer={(item: DatasetOption, { handleClick, modifiers }: ItemRendererProps) => (
              <MenuItem
                key={item.id}
                text={item.name}
                active={modifiers.active}
                onClick={handleClick}
                selected={state.selectedDataset?.id === item.id}
              />
            )}
            onItemSelect={(item: DatasetOption) => setState((s) => ({ ...s, selectedDataset: item }))}
            filterable={datasets.length > 10}
            itemPredicate={(query: string, item: DatasetOption) =>
              item.name.toLowerCase().includes(query.toLowerCase())
            }
            noResults={<MenuItem disabled text="데이터셋이 없습니다" />}
            popoverProps={{ minimal: true }}
          >
            <Button
              text={state.selectedDataset?.name || '데이터셋 선택...'}
              rightIcon="caret-down"
              fill
            />
          </Select>
        </FormGroup>
      )}
    </div>
  )

  const renderTransformStep = () => (
    <div className="wizard-step">
      <h3 className="wizard-step-title">변환 목표</h3>
      <p className="wizard-step-desc">데이터를 어떻게 변환하고 싶은지 설명해주세요.</p>

      <FormGroup label="변환 목표 설명" className="wizard-form-group">
        <InputGroup
          large
          placeholder="예: 이메일에서 도메인 추출, 금액 컬럼 합계 계산..."
          value={state.transformGoal}
          onChange={(e) => setState((s) => ({ ...s, transformGoal: e.target.value }))}
        />
      </FormGroup>

      <div className="wizard-suggestions">
        <span className="wizard-suggestions-label">자주 사용하는 변환:</span>
        <div className="wizard-suggestions-list">
          {['중복 제거', '빈 값 필터링', '날짜 형식 통일', '텍스트 정제'].map((suggestion) => (
            <button
              key={suggestion}
              type="button"
              className="wizard-suggestion"
              onClick={() => setState((s) => ({ ...s, transformGoal: suggestion }))}
            >
              {suggestion}
            </button>
          ))}
        </div>
      </div>
    </div>
  )

  const renderOutputStep = () => (
    <div className="wizard-step">
      <h3 className="wizard-step-title">출력 설정</h3>
      <p className="wizard-step-desc">변환된 데이터를 어디에 저장할지 선택하세요.</p>

      <div className="wizard-options">
        <button
          type="button"
          className={`wizard-option ${state.outputType === 'object-type' ? 'is-selected' : ''}`}
          onClick={() => setState((s) => ({ ...s, outputType: 'object-type' }))}
        >
          <Icon icon="cube" size={24} />
          <span className="wizard-option-label">Object Type</span>
          <span className="wizard-option-desc">온톨로지 객체로 저장</span>
        </button>
        <button
          type="button"
          className={`wizard-option ${state.outputType === 'export' ? 'is-selected' : ''}`}
          onClick={() => setState((s) => ({ ...s, outputType: 'export' }))}
        >
          <Icon icon="export" size={24} />
          <span className="wizard-option-label">파일 내보내기</span>
          <span className="wizard-option-desc">CSV/Excel로 저장</span>
        </button>
      </div>

      <FormGroup label="출력 이름" className="wizard-form-group">
        <InputGroup
          placeholder="출력 이름 입력..."
          value={state.outputName}
          onChange={(e) => setState((s) => ({ ...s, outputName: e.target.value }))}
        />
      </FormGroup>
    </div>
  )

  const renderReviewStep = () => (
    <div className="wizard-step">
      <h3 className="wizard-step-title">설정 확인</h3>
      <p className="wizard-step-desc">파이프라인 설정을 확인하세요.</p>

      <div className="wizard-review">
        <div className="wizard-review-item">
          <span className="wizard-review-label">입력</span>
          <span className="wizard-review-value">
            {state.inputType === 'dataset'
              ? state.selectedDataset?.name
              : state.inputType === 'file'
              ? '파일 업로드'
              : 'API 연결'}
          </span>
        </div>
        <div className="wizard-review-item">
          <span className="wizard-review-label">변환 목표</span>
          <span className="wizard-review-value">{state.transformGoal}</span>
        </div>
        <div className="wizard-review-item">
          <span className="wizard-review-label">출력</span>
          <span className="wizard-review-value">
            {state.outputType === 'object-type' ? 'Object Type' : '파일 내보내기'}: {state.outputName}
          </span>
        </div>
      </div>
    </div>
  )

  const renderStep = () => {
    switch (currentStep) {
      case 'input':
        return renderInputStep()
      case 'transform':
        return renderTransformStep()
      case 'output':
        return renderOutputStep()
      case 'review':
        return renderReviewStep()
      default:
        return null
    }
  }

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title="파이프라인 마법사"
      className="pipeline-wizard-dialog"
    >
      <DialogBody>
        <div className="wizard-progress">
          <ProgressBar value={progress} intent="primary" />
          <div className="wizard-steps-indicator">
            {STEPS.map((step, index) => (
              <div
                key={step.id}
                className={`wizard-step-indicator ${
                  index <= currentStepIndex ? 'is-active' : ''
                } ${step.id === currentStep ? 'is-current' : ''}`}
              >
                <Icon icon={step.icon as never} size={14} />
                <span>{step.label}</span>
              </div>
            ))}
          </div>
        </div>
        {renderStep()}
      </DialogBody>
      <DialogFooter
        actions={
          <>
            <Button text="취소" onClick={onClose} />
            {currentStepIndex > 0 && (
              <Button text="이전" onClick={handleBack} />
            )}
            {currentStep === 'review' ? (
              <Button
                text="파이프라인 생성"
                intent="primary"
                onClick={handleComplete}
              />
            ) : (
              <Button
                text="다음"
                intent="primary"
                onClick={handleNext}
                disabled={!canProceed}
              />
            )}
          </>
        }
      />
    </Dialog>
  )
}
