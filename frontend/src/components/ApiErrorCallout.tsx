import { Link } from 'react-router-dom'
import { Button, Callout, HTMLTable, Intent, NonIdealState } from '@blueprintjs/core'
import type { Language } from '../types/app'
import { HttpError } from '../api/bff'
import { classifyError } from '../errors/classifyError'
import { extractOccActualSeq } from '../utils/occ'
import { asRecord } from '../utils/typed'

const copy = {
  en: {
    notFoundTitle: 'Not found',
    notFoundBody: 'The requested resource could not be found. Check DB/branch context.',
    unknownLabelsTitle: 'unknown_label_keys',
    mappingsCta: 'Open Mappings',
    occTitle: 'OCC conflict',
    occBody: 'The resource has changed. Retry with actual_seq.',
    useActual: 'Use actual_seq and retry',
    rateLimitTitle: 'Rate limit',
    rateLimitBody: 'Please wait before retrying.',
    retry: 'Retry now',
  },
  ko: {
    notFoundTitle: '찾을 수 없습니다',
    notFoundBody: '리소스를 찾을 수 없습니다. DB/브랜치 컨텍스트를 확인하세요.',
    unknownLabelsTitle: 'unknown_label_keys',
    mappingsCta: 'Open Mappings',
    occTitle: 'OCC 충돌',
    occBody: '최신 seq로 재시도해야 합니다.',
    useActual: 'actual_seq로 재시도',
    rateLimitTitle: '레이트리밋',
    rateLimitBody: '잠시 후 다시 시도하세요.',
    retry: '지금 재시도',
  },
} as const

const extractUnknownLabels = (error: unknown): string[] | null => {
  if (!(error instanceof HttpError)) {
    return null
  }
  const detail = asRecord(error.detail)
  const nestedDetail = asRecord(detail.detail)
  const labels =
    detail.labels ??
    nestedDetail.labels ??
    (detail.error === 'unknown_label_keys' ? detail.labels : null) ??
    (nestedDetail.error === 'unknown_label_keys' ? nestedDetail.labels : null)
  if (!Array.isArray(labels)) {
    return null
  }
  const normalized = labels.filter((label): label is string => typeof label === 'string')
  return normalized.length ? normalized : null
}

export const ApiErrorCallout = ({
  error,
  language,
  mappingsUrl,
  onOpenMappings,
  onUseActualSeq,
  onRetry,
}: {
  error: unknown
  language: Language
  mappingsUrl?: string
  onOpenMappings?: () => void
  onUseActualSeq?: (actualSeq: number) => void
  onRetry?: () => void
}) => {
  if (!error) {
    return null
  }
  const c = copy[language]
  const classified = classifyError(error)

  if (classified.kind === 'AUTH') {
    return null
  }

  if (classified.kind === 'NOT_FOUND') {
    return (
      <NonIdealState
        icon="search"
        title={c.notFoundTitle}
        description={c.notFoundBody}
      />
    )
  }

  const unknownLabels = extractUnknownLabels(error)
  if (unknownLabels?.length) {
    return (
      <Callout intent={Intent.WARNING} title={c.unknownLabelsTitle}>
        <HTMLTable striped className="full-width">
          <thead>
            <tr>
              <th>Label</th>
            </tr>
          </thead>
          <tbody>
            {unknownLabels.map((label) => (
              <tr key={label}>
                <td>{label}</td>
              </tr>
            ))}
          </tbody>
        </HTMLTable>
        <div>
          {onOpenMappings ? (
            <Button minimal icon="open-application" onClick={onOpenMappings}>
              {c.mappingsCta}
            </Button>
          ) : mappingsUrl ? (
            <Link to={mappingsUrl}>{c.mappingsCta}</Link>
          ) : null}
        </div>
      </Callout>
    )
  }

  const actualSeq = extractOccActualSeq(error)
  if (actualSeq !== null) {
    return (
      <Callout intent={Intent.WARNING} title={c.occTitle}>
        {c.occBody} (actual_seq: {actualSeq})
        {onUseActualSeq ? (
          <div className="button-row">
            <Button minimal icon="refresh" onClick={() => onUseActualSeq(actualSeq)}>
              {c.useActual}
            </Button>
          </div>
        ) : null}
      </Callout>
    )
  }

  if (classified.kind === 'RATE_LIMIT') {
    const retryAfter = error instanceof HttpError ? error.retryAfterSeconds : undefined
    return (
      <Callout intent={Intent.WARNING} title={c.rateLimitTitle}>
        {c.rateLimitBody}
        {typeof retryAfter === 'number' ? ` (${retryAfter}s)` : ''}
        {onRetry ? (
          <div className="button-row">
            <Button minimal icon="refresh" onClick={onRetry}>
              {c.retry}
            </Button>
          </div>
        ) : null}
      </Callout>
    )
  }

  return null
}
