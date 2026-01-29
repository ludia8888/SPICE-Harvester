import { useCallback, useEffect, useMemo, useState } from 'react'
import { Button, Callout, Dialog, DialogBody, DialogFooter, FormGroup, InputGroup } from '@blueprintjs/core'

export type CreatePipelineDialogValues = {
  name: string
  branch: string
}

type CreatePipelineDialogProps = {
  isOpen: boolean
  title?: string
  defaultName: string
  defaultBranch: string
  isSubmitting?: boolean
  error?: string | null
  onClose: () => void
  onSubmit: (values: CreatePipelineDialogValues) => void | Promise<void>
}

export const CreatePipelineDialog = ({
  isOpen,
  title = 'Create pipeline',
  defaultName,
  defaultBranch,
  isSubmitting = false,
  error,
  onClose,
  onSubmit,
}: CreatePipelineDialogProps) => {
  const [name, setName] = useState(defaultName)
  const [branch, setBranch] = useState(defaultBranch)

  useEffect(() => {
    if (!isOpen) {
      return
    }
    setName(defaultName)
    setBranch(defaultBranch)
  }, [isOpen, defaultName, defaultBranch])

  const trimmedName = useMemo(() => name.trim(), [name])
  const trimmedBranch = useMemo(() => branch.trim(), [branch])
  const canSubmit = trimmedName.length > 0 && trimmedBranch.length > 0 && !isSubmitting

  const handleSubmit = useCallback(() => {
    if (!canSubmit) {
      return
    }
    void onSubmit({ name: trimmedName, branch: trimmedBranch })
  }, [canSubmit, onSubmit, trimmedName, trimmedBranch])

  return (
    <Dialog
      isOpen={isOpen}
      title={title}
      onClose={onClose}
      canEscapeKeyClose={!isSubmitting}
      canOutsideClickClose={!isSubmitting}
      className="pipeline-dialog"
    >
      <DialogBody>
        {error ? (
          <Callout intent="danger" title="Create failed" style={{ marginBottom: 12 }}>
            {error}
          </Callout>
        ) : null}
        <FormGroup label="Name" labelFor="pipeline-name" helperText="Pipeline names are unique per branch.">
          <InputGroup
            id="pipeline-name"
            placeholder="e.g. orders_enrichment"
            value={name}
            onChange={(e) => setName(e.target.value)}
            disabled={isSubmitting}
            autoFocus
          />
        </FormGroup>
        <FormGroup label="Branch" labelFor="pipeline-branch" helperText="Use a working branch (e.g. dev).">
          <InputGroup
            id="pipeline-branch"
            placeholder="e.g. dev"
            value={branch}
            onChange={(e) => setBranch(e.target.value)}
            disabled={isSubmitting}
          />
        </FormGroup>
      </DialogBody>
      <DialogFooter
        actions={
          <>
            <Button onClick={onClose} disabled={isSubmitting}>
              Cancel
            </Button>
            <Button intent="primary" onClick={handleSubmit} loading={isSubmitting} disabled={!canSubmit}>
              Create
            </Button>
          </>
        }
      />
    </Dialog>
  )
}

