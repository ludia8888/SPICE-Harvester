import { Button, Callout, Dialog, Intent } from '@blueprintjs/core'
import { SettingsForm, type SettingsCopy } from './SettingsPopoverContent'
import { useAppStore, type SettingsDialogReason } from '../../store/useAppStore'

const copy = {
  en: {
    tokenMissing: 'Access token required. Set a token to continue.',
    unauthorized: 'Unauthorized. Check your token and try again.',
    forbidden: 'Forbidden. Your token does not have permission.',
    unavailable: 'Service unavailable. Check the backend and retry.',
    retryHint: 'We will retry the last failed request once after saving.',
    saveLabel: 'Save & retry',
    closeLabel: 'Close',
  },
  ko: {
    tokenMissing: '액세스 토큰이 필요합니다. 토큰을 입력한 뒤 다시 시도하세요.',
    unauthorized: '인증에 실패했습니다. 토큰을 확인한 뒤 다시 시도하세요.',
    forbidden: '권한이 없습니다. Admin 토큰이 필요한 작업일 수 있습니다.',
    unavailable: '서비스를 사용할 수 없습니다. 백엔드 상태를 확인하세요.',
    retryHint: '저장 후 마지막 실패 요청을 1회 재시도합니다.',
    saveLabel: '저장 후 재시도',
    closeLabel: '닫기',
  },
} as const

const resolveMessage = (reason: SettingsDialogReason | null, language: 'en' | 'ko') => {
  if (!reason) {
    return null
  }
  const c = copy[language]
  switch (reason) {
    case 'TOKEN_MISSING':
      return c.tokenMissing
    case 'UNAUTHORIZED':
      return c.unauthorized
    case 'FORBIDDEN':
      return c.forbidden
    case 'SERVICE_UNAVAILABLE':
      return c.unavailable
    default:
      return c.tokenMissing
  }
}

export const SettingsDialog = ({
  isOpen,
  reason,
  onClose,
  onSave,
  copy: formCopy,
}: {
  isOpen: boolean
  reason: SettingsDialogReason | null
  onClose: () => void
  onSave: () => void
  copy: SettingsCopy
}) => {
  const language = useAppStore((state) => state.context.language)
  const message = resolveMessage(reason, language)
  const c = copy[language]

  return (
    <Dialog
      isOpen={isOpen}
      onClose={onClose}
      title={formCopy.settingsTitle}
      className="settings-dialog"
    >
      <div className="settings-dialog-body">
        {message ? (
          <Callout intent={Intent.WARNING} icon="lock">
            <div>{message}</div>
            <div className="muted small" style={{ marginTop: 6 }}>
              {c.retryHint}
            </div>
          </Callout>
        ) : null}
        <SettingsForm copy={formCopy} showTitle={false} className="settings-form" />
      </div>
      <div className="settings-dialog-footer">
        <Button minimal onClick={onClose}>
          {c.closeLabel}
        </Button>
        <Button intent={Intent.PRIMARY} onClick={onSave}>
          {c.saveLabel}
        </Button>
      </div>
    </Dialog>
  )
}
