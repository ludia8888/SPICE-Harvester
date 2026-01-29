import { useState, useCallback } from 'react'
import { Button, InputGroup, Spinner, Icon } from '@blueprintjs/core'

type NaturalLanguagePipelineInputProps = {
  onSubmit: (query: string) => void
  isProcessing?: boolean
  placeholder?: string
}

const SUGGESTIONS = [
  '이메일 컬럼에서 도메인만 추출해줘',
  '금액이 100만원 이상인 행만 필터링해줘',
  '날짜 형식을 YYYY-MM-DD로 통일해줘',
  '중복된 행을 제거해줘',
]

export const NaturalLanguagePipelineInput = ({
  onSubmit,
  isProcessing = false,
  placeholder = '자연어로 파이프라인을 만들어보세요...',
}: NaturalLanguagePipelineInputProps) => {
  const [query, setQuery] = useState('')
  const [showSuggestions, setShowSuggestions] = useState(false)

  const handleSubmit = useCallback(() => {
    if (query.trim() && !isProcessing) {
      onSubmit(query.trim())
    }
  }, [query, isProcessing, onSubmit])

  const handleKeyDown = useCallback(
    (event: React.KeyboardEvent<HTMLInputElement>) => {
      if (event.key === 'Enter') {
        handleSubmit()
      }
    },
    [handleSubmit],
  )

  const handleSuggestionClick = useCallback((suggestion: string) => {
    setQuery(suggestion)
    setShowSuggestions(false)
  }, [])

  return (
    <div className="nl-pipeline-input">
      <InputGroup
        leftIcon="search"
        placeholder={placeholder}
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        onKeyDown={handleKeyDown}
        onFocus={() => setShowSuggestions(true)}
        onBlur={() => setTimeout(() => setShowSuggestions(false), 200)}
        rightElement={
          isProcessing ? (
            <Spinner size={18} className="nl-pipeline-spinner" />
          ) : query.trim() ? (
            <Button
              minimal
              icon="arrow-right"
              onClick={handleSubmit}
              aria-label="실행"
            />
          ) : undefined
        }
        className="nl-pipeline-input-field"
        disabled={isProcessing}
      />
      {showSuggestions && !query && (
        <div className="nl-pipeline-suggestions">
          <div className="nl-pipeline-suggestions-header">
            <Icon icon="lightbulb" size={12} />
            <span>예시 명령어</span>
          </div>
          <div className="nl-pipeline-suggestions-list">
            {SUGGESTIONS.map((suggestion, index) => (
              <button
                key={index}
                type="button"
                className="nl-pipeline-suggestion"
                onClick={() => handleSuggestionClick(suggestion)}
              >
                {suggestion}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
