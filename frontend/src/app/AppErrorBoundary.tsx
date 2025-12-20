import { Component } from 'react'
import { Card, Code, H3, Pre, Text } from '@blueprintjs/core'

type AppErrorBoundaryProps = {
  children: React.ReactNode
}

type AppErrorBoundaryState = {
  error: Error | null
  componentStack?: string
}

export class AppErrorBoundary extends Component<AppErrorBoundaryProps, AppErrorBoundaryState> {
  state: AppErrorBoundaryState = { error: null }

  static getDerivedStateFromError(error: Error): AppErrorBoundaryState {
    return { error }
  }

  componentDidCatch(_error: Error, info: React.ErrorInfo): void {
    this.setState({ componentStack: info.componentStack ?? undefined })
  }

  render() {
    const { error, componentStack } = this.state
    if (!error) {
      return this.props.children
    }

    return (
      <div style={{ padding: '2rem', maxWidth: 920, margin: '0 auto' }}>
        <Card elevation={2}>
          <H3>Frontend crashed</H3>
          <Text className="muted">
            Check the browser console for details. This error boundary prevents a blank screen.
          </Text>
          <div style={{ marginTop: '1rem' }}>
            <Text>
              <Code>{error.name}</Code> {error.message}
            </Text>
          </div>
          {error.stack ? (
            <div style={{ marginTop: '1rem' }}>
              <Text className="muted small">Stack</Text>
              <Pre style={{ maxHeight: 260, overflow: 'auto' }}>{error.stack}</Pre>
            </div>
          ) : null}
          {componentStack ? (
            <div style={{ marginTop: '1rem' }}>
              <Text className="muted small">Component stack</Text>
              <Pre style={{ maxHeight: 260, overflow: 'auto' }}>{componentStack}</Pre>
            </div>
          ) : null}
        </Card>
      </div>
    )
  }
}
