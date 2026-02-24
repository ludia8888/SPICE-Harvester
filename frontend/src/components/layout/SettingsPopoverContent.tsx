import { Button, Code, FormGroup, HTMLSelect, InputGroup, Switch, Text } from '@blueprintjs/core'
import { navigate } from '../../state/pathname'
import { useAppStore } from '../../store/useAppStore'
import type { Language } from '../../types/app'

type LanguageOption = {
  label: string
  value: Language
}

export type SettingsCopy = {
  settingsTitle: string
  languageLabel: string
  languageHelper: string
  languageOptions: ReadonlyArray<LanguageOption>
  branchLabel: string
  branchHelper: string
  branchPlaceholder: string
  tokenLabel: string
  tokenHelper: string
  tokenPlaceholder: string
  rememberTokenLabel: string
  darkModeLabel: string
  themeHelper: string
  adminModeLabel: string
  adminModeWarning: string
  auditLinkLabel: string
  devModeLabel: string
  devModeHelper: string
  devEnvTitle: string
}

export const SettingsPopoverContent = ({ copy }: { copy: SettingsCopy }) => {
  const context = useAppStore((state) => state.context)
  const setLanguage = useAppStore((state) => state.setLanguage)
  const setBranch = useAppStore((state) => state.setBranch)
  const adminToken = useAppStore((state) => state.adminToken)
  const setAdminToken = useAppStore((state) => state.setAdminToken)
  const rememberToken = useAppStore((state) => state.rememberToken)
  const setRememberToken = useAppStore((state) => state.setRememberToken)
  const theme = useAppStore((state) => state.theme)
  const setTheme = useAppStore((state) => state.setTheme)
  const adminMode = useAppStore((state) => state.adminMode)
  const setAdminMode = useAppStore((state) => state.setAdminMode)
  const devMode = useAppStore((state) => state.devMode)
  const setDevMode = useAppStore((state) => state.setDevMode)
  const accessToken = useAppStore((state) => state.accessToken)
  const project = context.project

  return (
    <div className="settings-popover" style={{ minWidth: 320, padding: 12 }}>
      <Text className="sidebar-title">{copy.settingsTitle}</Text>

      <FormGroup label={copy.languageLabel} helperText={copy.languageHelper}>
        <HTMLSelect
          fill
          value={context.language}
          onChange={(event) => setLanguage(event.currentTarget.value as Language)}
          options={copy.languageOptions.map((option) => ({
            label: option.label,
            value: option.value,
          }))}
        />
      </FormGroup>

      <FormGroup label={copy.branchLabel} helperText={copy.branchHelper}>
        <InputGroup
          placeholder={copy.branchPlaceholder}
          value={context.branch}
          onChange={(event) => setBranch(event.currentTarget.value)}
        />
      </FormGroup>

      <Switch
        checked={theme === 'dark'}
        label={copy.darkModeLabel}
        onChange={(event) => setTheme((event.currentTarget as HTMLInputElement).checked ? 'dark' : 'light')}
      />
      <Text className="muted small" style={{ marginBottom: 8 }}>
        {copy.themeHelper}
      </Text>

      <Switch
        checked={adminMode}
        label={copy.adminModeLabel}
        onChange={(event) => setAdminMode((event.currentTarget as HTMLInputElement).checked)}
      />
      <Text className="muted small" style={{ marginBottom: 10 }}>
        {copy.adminModeWarning}
      </Text>

      <Button
        minimal
        icon="history"
        text={copy.auditLinkLabel}
        onClick={() => {
          if (project) {
            navigate(`/db/${encodeURIComponent(project)}/audit`)
          }
        }}
        disabled={!project}
      />

      <div style={{ borderTop: '1px solid var(--pt-divider-black, rgba(17,20,24,.15))', marginTop: 12, paddingTop: 12 }}>
        <Switch
          checked={devMode}
          label={copy.devModeLabel}
          onChange={(event) => setDevMode((event.currentTarget as HTMLInputElement).checked)}
        />
        <Text className="muted small" style={{ marginBottom: 8 }}>
          {copy.devModeHelper}
        </Text>

        {devMode && (
          <div style={{ marginTop: 8 }}>
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
              onChange={(event) => setRememberToken((event.currentTarget as HTMLInputElement).checked)}
            />

            <Text className="sidebar-title" style={{ marginTop: 12, marginBottom: 8 }}>
              {copy.devEnvTitle}
            </Text>
            <div style={{ fontSize: 11, lineHeight: '18px', fontFamily: 'monospace' }}>
              <div>
                <span className="muted">Auth: </span>
                <Code>{accessToken ? 'JWT' : adminToken ? 'Admin Token' : 'None'}</Code>
              </div>
              <div>
                <span className="muted">Project: </span>
                <Code>{project ?? '(none)'}</Code>
              </div>
              <div>
                <span className="muted">Branch: </span>
                <Code>{context.branch}</Code>
              </div>
              <div>
                <span className="muted">BFF: </span>
                <Code>{window.location.origin}/api</Code>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
