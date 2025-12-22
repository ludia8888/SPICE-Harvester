import { Intent, type ToastProps } from '@blueprintjs/core'
import type { Language } from '../types/app'
import { HttpError } from '../api/bff'
import { showAppToast } from '../app/AppToaster'
import { navigate } from '../state/pathname'
import { useAppStore } from '../store/useAppStore'
import { classifyError } from './classifyError'

const copy = {
  en: {
    authTitle: 'Authentication required',
    authBody: 'Set an admin token in Settings, then retry.',
    notFoundTitle: 'Not found (404)',
    notFoundBody: 'The resource may have been deleted, or the URL context is wrong. Refresh and retry.',
    conflictTitle: 'Conflict (409)',
    conflictBody: 'The resource has changed. Refresh and retry your action.',
    duplicateTitle: 'Already exists',
    duplicateBody: 'A project with this name already exists or was used before. Choose a different name.',
    validationTitle: 'Invalid request',
    validationBody: 'Check your input and try again.',
    rateLimitTitle: 'Rate limited',
    rateLimitBody: 'Too many requests. Wait and retry.',
    temporaryTitle: 'Service unavailable',
    temporaryBody: 'Backend is temporarily unavailable. Check the local stack and retry.',
    unknownLabelsTitle: 'Unknown labels',
    unknownLabelsBody: 'Some labels are not mapped. Review mappings and retry.',
    unknownLabelsList: 'Missing labels',
    unknownLabelsLink: 'Open mappings',
    unknownTitle: 'Unexpected error',
    unknownBody: 'Something went wrong. Check the console/logs and retry.',
    detailLabel: 'Detail',
  },
  ko: {
    authTitle: '인증이 필요합니다',
    authBody: '설정에서 관리자 토큰을 입력한 뒤 다시 시도하세요.',
    notFoundTitle: '찾을 수 없음 (404)',
    notFoundBody: '리소스가 삭제되었거나 URL 컨텍스트(project/branch)가 올바르지 않을 수 있어요. 새로고침 후 재시도하세요.',
    conflictTitle: '충돌 (409)',
    conflictBody: '최신 상태와 충돌했습니다. 새로고침 후 다시 시도하세요.',
    duplicateTitle: '이미 존재합니다',
    duplicateBody: '이미 사용 중인 프로젝트 이름입니다. 다른 이름을 선택하세요.',
    validationTitle: '요청이 올바르지 않습니다',
    validationBody: '입력값을 확인한 뒤 다시 시도하세요.',
    rateLimitTitle: '레이트리밋',
    rateLimitBody: '요청이 많습니다. 잠시 후 다시 시도하세요.',
    temporaryTitle: '서비스를 사용할 수 없습니다',
    temporaryBody: '백엔드가 일시적으로 응답하지 않습니다. 로컬 스택 상태를 확인한 뒤 재시도하세요.',
    unknownLabelsTitle: '알 수 없는 라벨',
    unknownLabelsBody: '매핑되지 않은 라벨이 있습니다. 매핑을 확인한 뒤 재시도하세요.',
    unknownLabelsList: '누락 라벨',
    unknownLabelsLink: '매핑 열기',
    unknownTitle: '예기치 못한 오류',
    unknownBody: '문제가 발생했습니다. 콘솔/로그를 확인한 뒤 재시도하세요.',
    detailLabel: '상세',
  },
} as const

const coerceNumber = (value: unknown) => (typeof value === 'number' && Number.isFinite(value) ? value : undefined)

type OccConflictPayload = {
  error?: unknown
  expected_seq?: unknown
  actual_seq?: unknown
  detail?: unknown
}

const unwrapDetail = (detail: unknown) => {
  let current = detail
  for (let depth = 0; depth < 3; depth += 1) {
    if (!current || typeof current !== 'object') {
      return current
    }
    const inner = (current as { detail?: unknown }).detail
    if (inner === undefined || inner === null) {
      return current
    }
    current = inner
  }
  return current
}

const extractOccConflict = (detail: unknown) => {
  const unwrapped = unwrapDetail(detail)
  if (!unwrapped || typeof unwrapped !== 'object') {
    return null
  }
  const payload = unwrapped as OccConflictPayload
  if (payload.error && payload.error !== 'optimistic_concurrency_conflict') {
    return null
  }
  const expected = coerceNumber(payload.expected_seq)
  const actual = coerceNumber(payload.actual_seq)
  if (payload.error === 'optimistic_concurrency_conflict' && expected === undefined && actual === undefined) {
    return { expected: undefined, actual: undefined }
  }
  if (expected === undefined && actual === undefined) {
    return null
  }
  return { expected, actual }
}

const extractOccSuffix = (detail: unknown) => {
  const conflict = extractOccConflict(detail)
  if (!conflict) {
    return ''
  }
  if (conflict.expected === undefined || conflict.actual === undefined) {
    return ''
  }
  return ` (expected ${conflict.expected}, actual ${conflict.actual})`
}

const extractRateLimitSuffix = (error: HttpError) => {
  if (!error.retryAfter || error.retryAfter <= 0) {
    return ''
  }
  return ` (${error.retryAfter}s)`
}

const extractDetailText = (detail: unknown) => {
  if (!detail) {
    return ''
  }
  const unwrapped = unwrapDetail(detail)
  if (typeof unwrapped === 'string') {
    return unwrapped.trim()
  }
  if (typeof unwrapped === 'object') {
    const maybe = unwrapped as { detail?: unknown; message?: unknown }
    if (typeof maybe.detail === 'string') {
      return maybe.detail.trim()
    }
    if (typeof maybe.message === 'string') {
      return maybe.message.trim()
    }
    try {
      const json = JSON.stringify(unwrapped)
      return json.length > 300 ? `${json.slice(0, 300)}…` : json
    } catch {
      return ''
    }
  }
  return String(unwrapped)
}

type UnknownLabelPayload = {
  error?: unknown
  labels?: unknown
  class_id?: unknown
}

const extractUnknownLabels = (detail: unknown) => {
  if (!detail || typeof detail !== 'object') {
    return null
  }
  const payload = detail as UnknownLabelPayload & { detail?: UnknownLabelPayload }
  const candidate = (payload.error ? payload : payload.detail) as UnknownLabelPayload | undefined
  if (!candidate || candidate.error !== 'unknown_label_keys') {
    return null
  }
  const labelsRaw = candidate.labels
  const labels = Array.isArray(labelsRaw)
    ? labelsRaw
        .filter((item): item is string => typeof item === 'string' && item.trim().length > 0)
        .map((item) => item.trim())
    : []
  const classId = typeof candidate.class_id === 'string' ? candidate.class_id : undefined
  return { labels, classId }
}

export const toastApiError = (error: unknown, language: Language) => {
  const c = copy[language]
  const classified = classifyError(error)

  let title: string = c.unknownTitle
  let body: string = c.unknownBody
  let intent: ToastProps['intent'] = Intent.DANGER
  let key = 'api-error:unknown'

  if (classified.kind === 'AUTH') {
    title = c.authTitle
    body = c.authBody
    intent = Intent.DANGER
    key = 'api-error:auth'
  } else if (classified.kind === 'NOT_FOUND') {
    title = c.notFoundTitle
    body = c.notFoundBody
    intent = Intent.WARNING
    key = 'api-error:404'
  } else if (classified.kind === 'OCC_CONFLICT') {
    const conflict = extractOccConflict(classified.detail)
    if (conflict?.expected === 0) {
      title = c.duplicateTitle
      body = c.duplicateBody
      intent = Intent.WARNING
      key = 'api-error:duplicate'
    } else {
      title = c.conflictTitle
      body = `${c.conflictBody}${extractOccSuffix(classified.detail)}`
      intent = Intent.WARNING
      key = 'api-error:409'
    }
  } else if (classified.kind === 'VALIDATION') {
    title = c.validationTitle
    body = c.validationBody
    intent = Intent.WARNING
    key = 'api-error:validation'
  } else if (classified.kind === 'RATE_LIMIT') {
    title = c.rateLimitTitle
    body = c.rateLimitBody
    if (error instanceof HttpError) {
      body = `${body}${extractRateLimitSuffix(error)}`
    }
    intent = Intent.WARNING
    key = 'api-error:429'
  } else if (classified.kind === 'TEMPORARY') {
    title = c.temporaryTitle
    body = c.temporaryBody
    intent = Intent.DANGER
    key = 'api-error:temporary'
  } else if (error instanceof HttpError && typeof classified.status === 'number') {
    key = `api-error:${classified.status}`
  }

  const unknownLabels = error instanceof HttpError ? extractUnknownLabels(error.detail) : null
  const project = useAppStore.getState().context.project
  const mappingPath = project ? `/db/${encodeURIComponent(project)}/mappings` : null
  const detailText = error instanceof HttpError ? extractDetailText(error.detail) : ''

  const message: ToastProps['message'] = unknownLabels ? (
    <div>
      <div style={{ fontWeight: 600, marginBottom: 4 }}>{c.unknownLabelsTitle}</div>
      <div>{c.unknownLabelsBody}</div>
      {unknownLabels.labels.length ? (
        <div style={{ marginTop: 6, opacity: 0.85, fontSize: 12 }}>
          {c.unknownLabelsList}: {unknownLabels.labels.join(', ')}
        </div>
      ) : null}
      {mappingPath ? (
        <div style={{ marginTop: 6 }}>
          <a
            href={mappingPath}
            onClick={(event) => {
              event.preventDefault()
              navigate(mappingPath)
            }}
          >
            {c.unknownLabelsLink}
          </a>
        </div>
      ) : null}
    </div>
  ) : (
    <div>
      <div style={{ fontWeight: 600, marginBottom: 4 }}>{title}</div>
      <div>{body}</div>
      {detailText ? (
        <div style={{ marginTop: 6, opacity: 0.85, fontSize: 12 }}>
          {c.detailLabel}: {detailText}
        </div>
      ) : null}
    </div>
  )

  void showAppToast(
    {
      intent,
      message,
      timeout: 6000,
    },
    key,
  )
}
