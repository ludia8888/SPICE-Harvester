import { Component, type ErrorInfo, type ReactNode } from 'react'
import { Callout, Intent } from '@blueprintjs/core'

type Props = {
  children: ReactNode
}

type State = {
  hasError: boolean
}

export class AppErrorBoundary extends Component<Props, State> {
  state: State = { hasError: false }

  static getDerivedStateFromError(): State {
    return { hasError: true }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('Unhandled app error:', error, info)
  }

  render() {
    if (this.state.hasError) {
      return (
        <div style={{ padding: 16 }}>
          <Callout intent={Intent.DANGER} title="Frontend error">
            An unexpected UI error occurred. Reload the page and retry.
          </Callout>
        </div>
      )
    }
    return this.props.children
  }
}
