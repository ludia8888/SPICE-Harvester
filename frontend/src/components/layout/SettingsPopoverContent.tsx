import { Card, FormGroup, HTMLSelect, InputGroup, Switch, Text } from '@blueprintjs/core'
import { API_BASE_URL } from '../../api/config'
import { useAppStore } from '../../store/useAppStore'
import type { Language } from '../../types/app'

type LanguageOption = { label: string; value: Language }

export type SettingsCopy = {
  settingsTitle: string
  languageLabel: string
  languageHelper: string
  languageOptions: ReadonlyArray<LanguageOption>
  branchLabel: string
  branchHelper: string
  branchPlaceholder: string
  authTokenLabel: string
  authTokenHelper: string
  authTokenPlaceholder: string
  tokenLabel: string
  tokenHelper: string
  tokenPlaceholder: string
  rememberTokenLabel: string
  darkModeLabel: string
  themeHelper: string
  adminModeLabel: string
  adminModeWarning: string
  auditLinkLabel: string
}

const buildAuditUrl = () => `${API_BASE_URL.replace(/\/+$/, '')}/audit/logs?limit=50`

export const SettingsForm = ({
  copy,
  showTitle = true,
  className,
}: {
  copy: SettingsCopy
  showTitle?: boolean
  className?: string
}) => {
  const context = useAppStore((state) => state.context)
  const theme = useAppStore((state) => state.theme)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)
  const rememberToken = useAppStore((state) => state.rememberToken)
  const adminMode = useAppStore((state) => state.adminMode)

  const setLanguage = useAppStore((state) => state.setLanguage)
  const setBranch = useAppStore((state) => state.setBranch)
  const setAuthToken = useAppStore((state) => state.setAuthToken)
  const setAdminToken = useAppStore((state) => state.setAdminToken)
  const setRememberToken = useAppStore((state) => state.setRememberToken)
  const setTheme = useAppStore((state) => state.setTheme)
  const setAdminMode = useAppStore((state) => state.setAdminMode)

  return (
    <div className={className}>
      {showTitle ? <div className="settings-title">{copy.settingsTitle}</div> : null}
      <FormGroup label={copy.languageLabel} helperText={copy.languageHelper}>
        <HTMLSelect
          options={copy.languageOptions}
          value={context.language}
          onChange={(event) => setLanguage(event.currentTarget.value as Language)}
        />
      </FormGroup>
      <FormGroup label={copy.branchLabel} helperText={copy.branchHelper}>
        <InputGroup
          placeholder={copy.branchPlaceholder}
          value={context.branch}
          onChange={(event) => setBranch(event.currentTarget.value)}
        />
      </FormGroup>
      <FormGroup label={copy.authTokenLabel} helperText={copy.authTokenHelper}>
        <InputGroup
          type="password"
          placeholder={copy.authTokenPlaceholder}
          value={authToken}
          onChange={(event) => setAuthToken(event.currentTarget.value)}
        />
      </FormGroup>
      <FormGroup label={copy.tokenLabel} helperText={copy.tokenHelper}>
        <InputGroup
          type="password"
          placeholder={copy.tokenPlaceholder}
          value={adminToken}
          onChange={(event) => setAdminToken(event.currentTarget.value)}
        />
      </FormGroup>
      <Switch
        checked={rememberToken}
        label={copy.rememberTokenLabel}
        onChange={(event) => setRememberToken(event.currentTarget.checked)}
      />
      <Switch
        checked={theme === 'dark'}
        label={copy.darkModeLabel}
        onChange={(event) => setTheme(event.currentTarget.checked ? 'dark' : 'light')}
      />
      <Text className="muted small">{copy.themeHelper}</Text>
      <Switch
        checked={adminMode}
        disabled={!adminToken}
        label={copy.adminModeLabel}
        onChange={(event) => setAdminMode(event.currentTarget.checked)}
      />
      {adminMode ? (
        <>
          <Text className="muted small">{copy.adminModeWarning}</Text>
          <Text className="muted small">
            <a href={buildAuditUrl()} target="_blank" rel="noreferrer">
              {copy.auditLinkLabel}
            </a>
          </Text>
        </>
      ) : null}
    </div>
  )
}

export const SettingsPopoverContent = ({ copy }: { copy: SettingsCopy }) => (
  <Card className="settings-popover" elevation={2}>
    <SettingsForm copy={copy} />
  </Card>
)
