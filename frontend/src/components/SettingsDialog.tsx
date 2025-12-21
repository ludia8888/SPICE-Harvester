import { Dialog } from '@blueprintjs/core'
import { SettingsPopoverContent, type SettingsCopy } from './layout/SettingsPopoverContent'
import { useAppStore } from '../store/useAppStore'

export const SettingsDialog = ({ copy }: { copy: SettingsCopy }) => {
  const isOpen = useAppStore((state) => state.settingsOpen)
  const setSettingsOpen = useAppStore((state) => state.setSettingsOpen)

  return (
    <Dialog
      isOpen={isOpen}
      onClose={() => setSettingsOpen(false)}
      title={copy.settingsTitle}
      className="settings-dialog"
    >
      <SettingsPopoverContent copy={copy} />
    </Dialog>
  )
}
