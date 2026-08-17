import { useEffect, useMemo, useState } from "react"

import {
  getLiveBars,
  subscribeLiveBarsSSE,
  type Bar,
  type BarType,
} from "@/api/bars"

/** Upsert by window_start; closed (non-partial) bars always win. */
export function upsertBar(bars: Bar[], bar: Bar): Bar[] {
  const idx = bars.findIndex((b) => b.window_start === bar.window_start)
  if (idx === -1) {
    return [...bars, bar].sort((a, b) =>
      a.window_start.localeCompare(b.window_start)
    )
  }
  const existing = bars[idx]
  if (existing && !existing.is_partial && bar.is_partial) {
    return bars
  }
  const copy = [...bars]
  copy[idx] = bar
  return copy
}

export function useBars(symbol: string | null, barType: BarType = "1m") {
  const [bars, setBars] = useState<Bar[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [live, setLive] = useState(false)

  useEffect(() => {
    if (!symbol) {
      setBars([])
      setError(null)
      setLoading(false)
      return
    }

    let cancelled = false
    setLoading(true)
    setBars([])
    void getLiveBars(symbol, { barType, limit: 240 })
      .then((res) => {
        if (!cancelled) {
          setBars(res.bars)
          setError(null)
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : String(err))
          setBars([])
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })

    return () => {
      cancelled = true
    }
  }, [symbol, barType])

  useEffect(() => {
    if (!symbol) {
      setLive(false)
      return
    }
    return subscribeLiveBarsSSE(
      [symbol],
      {
        onStatus: setLive,
        onBar: (bar) => {
          if (bar.symbol !== symbol) return
          setBars((prev) => upsertBar(prev, bar))
        },
        onError: (message) => setError(message),
      },
      barType
    )
  }, [symbol, barType])

  const latest = useMemo(
    () => (bars.length ? bars[bars.length - 1] : null),
    [bars]
  )

  return { bars, latest, loading, error, live }
}
