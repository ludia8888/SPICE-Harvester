import { useEffect, useMemo, useState } from 'react'

export const useCooldown = () => {
  const [cooldownUntil, setCooldownUntil] = useState<number | null>(null)
  const [now, setNow] = useState(() => Date.now())

  useEffect(() => {
    if (!cooldownUntil) {
      return
    }
    const timer = window.setInterval(() => setNow(Date.now()), 1000)
    return () => window.clearInterval(timer)
  }, [cooldownUntil])

  const remainingSeconds = useMemo(() => {
    if (!cooldownUntil) {
      return 0
    }
    const remaining = Math.ceil((cooldownUntil - now) / 1000)
    return remaining > 0 ? remaining : 0
  }, [cooldownUntil, now])

  const startCooldown = (seconds: number | undefined) => {
    if (!seconds || seconds <= 0) {
      setCooldownUntil(null)
      return
    }
    const nowValue = Date.now()
    setNow(nowValue)
    setCooldownUntil(nowValue + seconds * 1000)
  }

  const active = remainingSeconds > 0

  return { active, remainingSeconds, startCooldown }
}
