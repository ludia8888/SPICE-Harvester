import { useEffect, useMemo, useState } from 'react'

export const useCooldown = () => {
  const [cooldownUntil, setCooldownUntil] = useState<number | null>(null)
  const [, setTick] = useState(0)

  useEffect(() => {
    if (!cooldownUntil) {
      return
    }
    const timer = window.setInterval(() => setTick((tick) => tick + 1), 1000)
    return () => window.clearInterval(timer)
  }, [cooldownUntil])

  const remainingSeconds = useMemo(() => {
    if (!cooldownUntil) {
      return 0
    }
    const remaining = Math.ceil((cooldownUntil - Date.now()) / 1000)
    return remaining > 0 ? remaining : 0
  }, [cooldownUntil])

  const startCooldown = (seconds: number | undefined) => {
    if (!seconds || seconds <= 0) {
      setCooldownUntil(null)
      return
    }
    setCooldownUntil(Date.now() + seconds * 1000)
  }

  const active = remainingSeconds > 0

  return { active, remainingSeconds, startCooldown }
}
