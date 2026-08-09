import { useCallback, useEffect, useState } from "react"

import {
  getSymbols,
  subscribeBarsSSE,
  type BarType,
  type SymbolSummary,
} from "@/api/bars"

export function useSymbols(barType: BarType = "1m", pollMs = 30_000) {
  const [symbols, setSymbols] = useState<SymbolSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const refresh = useCallback(async () => {
    try {
      const data = await getSymbols(barType)
      setSymbols(data)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }, [barType])

  useEffect(() => {
    setLoading(true)
    void refresh()
    const id = window.setInterval(() => void refresh(), pollMs)
    return () => window.clearInterval(id)
  }, [refresh, pollMs])

  useEffect(() => {
    return subscribeBarsSSE(
      undefined,
      {
        onBar: (bar) => {
          setSymbols((prev) => {
            const idx = prev.findIndex((s) => s.symbol === bar.symbol)
            if (idx === -1) {
              return [
                ...prev,
                {
                  ...bar,
                  prev_close: null,
                  change: null,
                  change_pct: null,
                },
              ].sort((a, b) => a.symbol.localeCompare(b.symbol))
            }
            const existing = prev[idx]
            const change =
              existing.close != null ? bar.close - existing.close : null
            const next: SymbolSummary = {
              ...bar,
              prev_close: existing.close,
              change,
              change_pct:
                existing.close && change != null
                  ? (change / existing.close) * 100
                  : null,
            }
            const copy = [...prev]
            copy[idx] = next
            return copy
          })
        },
      },
      barType
    )
  }, [barType])

  return { symbols, loading, error, refresh }
}
