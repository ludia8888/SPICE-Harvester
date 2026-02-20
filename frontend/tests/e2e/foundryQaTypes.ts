export type QABugSeverity = 'P0' | 'P1' | 'P2' | 'P3'

export type QABugRecord = {
  id: string
  severity: QABugSeverity
  phase: string
  repro_steps: string[]
  expected: string
  actual: string
  endpoint: string
  ui_path: string
  evidence: Record<string, unknown>
  hypothesis: string
  timestamp: string
  source: 'frontend_e2e'
}

export type ActionSideEffectEvidence = {
  actionTypeId: string
  mode: 'VALIDATE_ONLY' | 'VALIDATE_AND_EXECUTE'
  request: Record<string, unknown>
  responseStatus: number
  responseBody: unknown
  auditLogId?: string | null
  sideEffectDelivery?: unknown
  webhookDelivery: {
    received: boolean
    payload?: unknown
  }
  writebackStatus: 'confirmed' | 'missing' | 'not_configured'
}

export type ClosedLoopVerificationResult = {
  datasetId: string
  objectType: string
  queryBeforeCount: number
  queryAfterCount: number
  changedObjectIds: string[]
  auditLogEvidence: {
    hasLogs: boolean
    count: number
  }
  status: 'verified' | 'partial' | 'failed'
}
