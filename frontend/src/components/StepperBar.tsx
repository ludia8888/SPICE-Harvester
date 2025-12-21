import { Button, Intent } from '@blueprintjs/core'

type StepperBarProps = {
  steps: string[]
  activeStep: number
  onStepChange: (step: number) => void
}

export const StepperBar = ({ steps, activeStep, onStepChange }: StepperBarProps) => {
  return (
    <div className="stepper-bar">
      {steps.map((title, index) => (
        <Button
          key={`${title}-${index}`}
          minimal
          intent={index === activeStep ? Intent.PRIMARY : Intent.NONE}
          onClick={() => onStepChange(index)}
        >
          {index + 1}. {title}
        </Button>
      ))}
    </div>
  )
}
