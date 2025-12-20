import { Intent, type ToastProps } from '@blueprintjs/core'
import type { Language } from '../types/app'
import { HttpError } from '../api/bff'
import { showAppToast } from '../app/AppToaster'
import { classifyError } from './classifyError'

type OccConflictDetail = {
  detail?: { expected_seq?: number; actual_seq?: number }
  expected_seq?: number
  actual_seq?: number
}

const copy = {
  en: {
    authTitle: 'Authentication required',
    authBody: 'Set an admin token in Settings, then retry.',
    notFoundTitle: 'Not found (404)',
    notFoundBody: 'The resource may have been deleted, or the URL context is wrong. Refresh and retry.',
    conflictTitle: 'Conflict (409)',
    conflictBody: 'The resource has changed. Refresh and retry your action.',
    validationTitle: 'Invalid request',
    validationBody: 'Check your input and try again.',
    temporaryTitle: 'Service unavailable',
    temporaryBody: 'Backend is temporarily unavailable. Check the local stack and retry.',
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
    validationTitle: '요청이 올바르지 않습니다',
    validationBody: '입력값을 확인한 뒤 다시 시도하세요.',
    temporaryTitle: '서비스를 사용할 수 없습니다',
    temporaryBody: '백엔드가 일시적으로 응답하지 않습니다. 로컬 스택 상태를 확인한 뒤 재시도하세요.',
    unknownTitle: '예기치 못한 오류',
    unknownBody: '문제가 발생했습니다. 콘솔/로그를 확인한 뒤 재시도하세요.',
    detailLabel: '상세',
  },
} as const

const coerceNumber = (value: unknown) => (typeof value === 'number' && Number.isFinite(value) ? value : undefined)

const extractOccSuffix = (detail: unknown) => {
  if (!detail || typeof detail !== 'object') {
    return ''
  }
  const payload = detail as OccConflictDetail
  const expected = coerceNumber(payload.detail?.expected_seq ?? payload.expected_seq)
  const actual = coerceNumber(payload.detail?.actual_seq ?? payload.actual_seq)
  if (expected === undefined || actual === undefined) {
    return ''
  }
  return ` (expected ${expected}, actual ${actual})`
}

const extractDetailText = (detail: unknown) => {
  if (!detail) {
    return ''
  }
  if (typeof detail === 'string') {
    return detail.trim()
  }
  if (typeof detail === 'object') {
    const maybe = detail as { detail?: unknown; message?: unknown }
    if (typeof maybe.detail === 'string') {
      return maybe.detail.trim()
    }
    if (typeof maybe.message === 'string') {
      return maybe.message.trim()
    }
    try {
      const json = JSON.stringify(detail)
      return json.length > 300 ? `${json.slice(0, 300)}…` : json
    } catch {
      return ''
    }
  }
  return String(detail)
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
    title = c.conflictTitle
    body = `${c.conflictBody}${extractOccSuffix(classified.detail)}`
    intent = Intent.WARNING
    key = 'api-error:409'
  } else if (classified.kind === 'VALIDATION') {
    title = c.validationTitle
    body = c.validationBody
    intent = Intent.WARNING
    key = 'api-error:validation'
  } else if (classified.kind === 'TEMPORARY') {
    title = c.temporaryTitle
    body = c.temporaryBody
    intent = Intent.DANGER
    key = 'api-error:temporary'
  } else if (error instanceof HttpError && typeof classified.status === 'number') {
    key = `api-error:${classified.status}`
  }

  const detailText = error instanceof HttpError ? extractDetailText(error.detail) : ''
  const message: ToastProps['message'] = (
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
