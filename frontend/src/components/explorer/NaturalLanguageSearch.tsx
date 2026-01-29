import { useState, useCallback } from 'react'
import { Button, Icon, InputGroup, Spinner } from '@blueprintjs/core'

type NaturalLanguageSearchProps = {
  value: string
  onChange: (value: string) => void
  onSearch: (query: string) => void
  isLoading?: boolean
  placeholder?: string
  suggestions?: string[]
}

export const NaturalLanguageSearch = ({
  value,
  onChange,
  onSearch,
  isLoading = false,
  placeholder = '자연어로 검색하세요...',
  suggestions = [],
}: NaturalLanguageSearchProps) => {
  const [isFocused, setIsFocused] = useState(false)

  const handleKeyDown = useCallback(
    (event: React.KeyboardEvent<HTMLInputElement>) => {
      if (event.key === 'Enter' && value.trim()) {
        onSearch(value.trim())
      }
    },
    [value, onSearch],
  )

  const handleSuggestionClick = useCallback(
    (suggestion: string) => {
      onChange(suggestion)
      onSearch(suggestion)
    },
    [onChange, onSearch],
  )

  const showSuggestions = isFocused && !value.trim() && suggestions.length > 0

  return (
    <div className="nl-search">
      <InputGroup
        large
        leftIcon="search"
        placeholder={placeholder}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={handleKeyDown}
        onFocus={() => setIsFocused(true)}
        onBlur={() => setTimeout(() => setIsFocused(false), 200)}
        rightElement={
          isLoading ? (
            <Spinner size={18} className="nl-search-spinner" />
          ) : value.trim() ? (
            <Button
              minimal
              icon="arrow-right"
              onClick={() => onSearch(value.trim())}
              aria-label="검색"
            />
          ) : undefined
        }
        className="nl-search-input"
        aria-label="자연어 검색"
      />
      {showSuggestions && (
        <div className="nl-search-suggestions">
          <div className="nl-search-suggestions-header">
            <Icon icon="lightbulb" size={12} />
            <span>예시 검색어</span>
          </div>
          {suggestions.map((suggestion, index) => (
            <button
              key={index}
              type="button"
              className="nl-search-suggestion"
              onClick={() => handleSuggestionClick(suggestion)}
            >
              {suggestion}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
