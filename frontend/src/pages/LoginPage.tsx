import { useState, useCallback, type FormEvent } from 'react'
import {
  Button,
  Callout,
  Card,
  FormGroup,
  H2,
  InputGroup,
  Intent,
  Spinner,
  Tag,
} from '@blueprintjs/core'
import { useAppStore } from '../store/useAppStore'
import { navigate } from '../state/pathname'

type LoginResponse = {
  access_token: string
  refresh_token: string
  token_type: string
  expires_in: number
}

export const LoginPage = () => {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  const setAccessToken = useAppStore((s) => s.setAccessToken)
  const setRefreshToken = useAppStore((s) => s.setRefreshToken)

  const handleSubmit = useCallback(
    async (e: FormEvent) => {
      e.preventDefault()
      setError(null)
      setLoading(true)

      try {
        const res = await fetch('/api/v1/auth/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ username, password }),
        })

        if (!res.ok) {
          const body = await res.json().catch(() => null)
          const detail =
            (body as { detail?: string })?.detail ??
            `Login failed (HTTP ${res.status})`
          setError(detail)
          return
        }

        const data = (await res.json()) as LoginResponse
        setAccessToken(data.access_token)
        setRefreshToken(data.refresh_token)
        navigate('/')
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Network error')
      } finally {
        setLoading(false)
      }
    },
    [username, password, setAccessToken, setRefreshToken],
  )

  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        minHeight: '100vh',
        background: 'var(--pt-app-background-color, #f6f7f9)',
      }}
    >
      <Card elevation={2} style={{ width: 380, padding: 32 }}>
        <div style={{ textAlign: 'center', marginBottom: 24 }}>
          <H2 style={{ margin: 0 }}>SPICE Harvester</H2>
          <Tag minimal intent={Intent.PRIMARY} style={{ marginTop: 8 }}>
            Sign In
          </Tag>
        </div>

        {error && (
          <Callout intent={Intent.DANGER} style={{ marginBottom: 16 }}>
            {error}
          </Callout>
        )}

        <form onSubmit={handleSubmit}>
          <FormGroup label="Username" labelFor="login-username">
            <InputGroup
              id="login-username"
              placeholder="admin"
              leftIcon="person"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              autoFocus
              disabled={loading}
            />
          </FormGroup>

          <FormGroup label="Password" labelFor="login-password">
            <InputGroup
              id="login-password"
              placeholder="password"
              leftIcon="lock"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              disabled={loading}
            />
          </FormGroup>

          <Button
            type="submit"
            intent={Intent.PRIMARY}
            fill
            large
            disabled={loading || !username || !password}
            icon={loading ? <Spinner size={16} /> : 'log-in'}
            text={loading ? 'Signing in...' : 'Sign In'}
            style={{ marginTop: 8 }}
          />
        </form>
      </Card>
    </div>
  )
}
