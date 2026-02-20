import { useMemo, useState } from 'react'
import { Button, Dialog, FormGroup, InputGroup, Text, TextArea } from '@blueprintjs/core'

type ConfirmPayload = {
  reason: string
}

type DangerConfirmDialogProps = {
  isOpen: boolean
  title: string
  description: string
  confirmLabel: string
  cancelLabel: string
  confirmTextToType?: string
  reasonLabel: string
  reasonPlaceholder?: string
  typedLabel: string
  typedPlaceholder?: string
  footerHint?: string
  loading?: boolean
  onCancel: () => void
  onConfirm: (payload: ConfirmPayload) => void
}

export const DangerConfirmDialog = ({
  isOpen,
  title,
  description,
  confirmLabel,
  cancelLabel,
  confirmTextToType = '',
  reasonLabel,
  reasonPlaceholder,
  typedLabel,
  typedPlaceholder,
  footerHint,
  loading = false,
  onCancel,
  onConfirm,
}: DangerConfirmDialogProps) => {
  const [reason, setReason] = useState('')
  const [typed, setTyped] = useState('')

  const typedMatches = useMemo(() => {
    const expected = confirmTextToType.trim()
    if (!expected) {
      return true
    }
    return typed.trim() === expected
  }, [confirmTextToType, typed])

  const canSubmit = !loading && typedMatches && reason.trim().length > 0

  const handleClose = () => {
    if (loading) {
      return
    }
    onCancel()
    setReason('')
    setTyped('')
  }

  const handleConfirm = () => {
    if (!canSubmit) {
      return
    }
    onConfirm({ reason: reason.trim() })
  }

  return (
    <Dialog
      isOpen={isOpen}
      onClose={handleClose}
      title={title}
      icon="warning-sign"
      className="danger-confirm-dialog"
    >
      <div style={{ padding: 16 }}>
        <Text>{description}</Text>

        <FormGroup label={reasonLabel} style={{ marginTop: 12 }}>
          <TextArea
            fill
            rows={3}
            placeholder={reasonPlaceholder}
            value={reason}
            onChange={(event) => setReason(event.currentTarget.value)}
          />
        </FormGroup>

        <FormGroup label={typedLabel}>
          <InputGroup
            placeholder={typedPlaceholder}
            value={typed}
            onChange={(event) => setTyped(event.currentTarget.value)}
          />
        </FormGroup>

        {footerHint ? (
          <Text className="muted small" style={{ marginBottom: 12 }}>
            {footerHint}
          </Text>
        ) : null}

        <div className="form-row" style={{ justifyContent: 'flex-end' }}>
          <Button minimal text={cancelLabel} onClick={handleClose} disabled={loading} />
          <Button
            intent="danger"
            text={loading ? `${confirmLabel}...` : confirmLabel}
            onClick={handleConfirm}
            disabled={!canSubmit}
            loading={loading}
          />
        </div>
      </div>
    </Dialog>
  )
}
