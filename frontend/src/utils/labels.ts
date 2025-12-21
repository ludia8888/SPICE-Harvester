export type LocalizedText = string | Record<string, string> | null | undefined

export const formatLocalizedText = (value: LocalizedText, lang: string, fallback = '') => {
  if (!value) {
    return fallback
  }
  if (typeof value === 'string') {
    return value
  }
  const normalized = value as Record<string, string>
  if (normalized[lang]) {
    return normalized[lang]
  }
  if (normalized.ko) {
    return normalized.ko
  }
  if (normalized.en) {
    return normalized.en
  }
  const first = Object.values(normalized).find((entry) => typeof entry === 'string' && entry.trim())
  return first ?? fallback
}

export const formatLabel = (value: LocalizedText, lang: string, fallback = '') =>
  formatLocalizedText(value, lang, fallback)
