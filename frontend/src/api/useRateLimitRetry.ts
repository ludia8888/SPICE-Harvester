import { useCallback, useEffect, useRef, useState } from 'react'
import { HttpError } from './bff'

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

export const useRateLimitRetry = (maxRetries = 1) => {
  const [cooldown, setCooldown] = useState(0)
  const timerRef = useRef<number | null>(null)

  useEffect(() => {
    if (cooldown <= 0) {
      if (timerRef.current) {
        window.clearInterval(timerRef.current)
        timerRef.current = null
      }
      return
    }

    if (timerRef.current) {
      return
    }

    timerRef.current = window.setInterval(() => {
      setCooldown((value) => Math.max(0, value - 1))
    }, 1000)

    return () => {
      if (timerRef.current) {
        window.clearInterval(timerRef.current)
        timerRef.current = null
      }
    }
  }, [cooldown])

  const startCooldown = useCallback((seconds: number | null) => {
    if (!seconds || seconds <= 0) {
      return
    }
    setCooldown(Math.max(0, Math.ceil(seconds)))
  }, [])

  const withRateLimitRetry = useCallback(
    async <T,>(fn: () => Promise<T>): Promise<T> => {
      let attempt = 0
      while (true) {
        try {
          return await fn()
        } catch (error) {
          if (!(error instanceof HttpError) || error.status !== 429) {
            throw error
          }
          if (attempt >= maxRetries) {
            throw error
          }
          attempt += 1
          const waitSeconds = error.retryAfter
          if (!waitSeconds || waitSeconds <= 0) {
            throw error
          }
          startCooldown(waitSeconds)
          await sleep(waitSeconds * 1000)
        }
      }
    },
    [maxRetries, startCooldown],
  )

  return { cooldown, startCooldown, withRateLimitRetry }
}
