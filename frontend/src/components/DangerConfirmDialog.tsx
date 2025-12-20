import { Button, Classes, Dialog, FormGroup, InputGroup, Intent, Text } from '@blueprintjs/core'
import { useMemo, useState } from 'react'

type DangerConfirmDialogProps = {
  isOpen: boolean
  title: string
  description: string
  confirmLabel: string
  cancelLabel: string
  confirmIntent?: Intent
  confirmTextToType: string
  reasonLabel: string
  reasonPlaceholder: string
  typedLabel: string
  typedPlaceholder: string
  onCancel: () => void
  onConfirm: (payload: { reason: string }) => void
  loading?: boolean
  footerHint?: string
}

export const DangerConfirmDialog = ({
  isOpen,
  title,
  description,
  confirmLabel,
  cancelLabel,
  confirmIntent = Intent.DANGER,
  confirmTextToType,
  reasonLabel,
  reasonPlaceholder,
  typedLabel,
  typedPlaceholder,
  onCancel,
  onConfirm,
  loading = false,
  footerHint,
}: DangerConfirmDialogProps) => {
  const [typed, setTyped] = useState('')
  const [reason, setReason] = useState('')

  const isValid = useMemo(() => {
    if (!typed.trim() || !reason.trim()) {
      return false
    }
    return typed.trim() === confirmTextToType
  }, [confirmTextToType, reason, typed])

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onCancel}
      title={title}
      canEscapeKeyClose={!loading}
      canOutsideClickClose={!loading}
    >
      <div className={Classes.DIALOG_BODY}>
        <Text>{description}</Text>
        <FormGroup label={reasonLabel} helperText={footerHint}>
          <InputGroup
            placeholder={reasonPlaceholder}
            value={reason}
            onChange={(event) => setReason(event.currentTarget.value)}
            disabled={loading}
          />
        </FormGroup>
        <FormGroup label={typedLabel}>
          <InputGroup
            placeholder={typedPlaceholder}
            value={typed}
            onChange={(event) => setTyped(event.currentTarget.value)}
            disabled={loading}
          />
        </FormGroup>
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button onClick={onCancel} disabled={loading}>
            {cancelLabel}
          </Button>
          <Button
            intent={confirmIntent}
            onClick={() => onConfirm({ reason: reason.trim() })}
            disabled={!isValid || loading}
            loading={loading}
          >
            {confirmLabel}
          </Button>
        </div>
      </div>
    </Dialog>
  )
}
