import { Button, Card, H1, Text } from '@blueprintjs/core'
import './App.css'
import { useAppStore } from './store/useAppStore'

function App() {
  const { count, increment, decrement, reset } = useAppStore()

  return (
    <div className="app">
      <H1>SPICE Harvester</H1>
      <Text className="subtitle">Zustand 상태 스토어 기본 설정</Text>
      <Card className="card" elevation={2}>
        <div className="counter">{count}</div>
        <div className="controls">
          <Button icon="minus" onClick={decrement}>
            감소
          </Button>
          <Button icon="refresh" onClick={reset} variant="minimal">
            초기화
          </Button>
          <Button icon="add" intent="primary" onClick={increment}>
            증가
          </Button>
        </div>
      </Card>
    </div>
  )
}

export default App
