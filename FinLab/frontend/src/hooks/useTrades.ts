import { useEffect, useState } from "react"

import {
  getTrades,
  subscribeTradesSSE,
  type TradeTick,
} from "@/api/trades"

const MAX_TRADES = 150

export function useTrades(symbol: string | null) {
  const [trades, setTrades] = useState<TradeTick[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [live, setLive] = useState(false)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    void getTrades({
      symbol: symbol ?? undefined,
      limit: 80,
    })
      .then((res) => {
        if (!cancelled) {
          setTrades(res.trades)
          setError(null)
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : String(err))
          setTrades([])
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })

    return () => {
      cancelled = true
    }
  }, [symbol])

  useEffect(() => {
    return subscribeTradesSSE(symbol ? [symbol] : undefined, {
      onStatus: setLive,
      onTrade: (trade) => {
        if (symbol && trade.symbol !== symbol) return
        setTrades((prev) => {
          const next = [...prev, trade]
          return next.length > MAX_TRADES
            ? next.slice(next.length - MAX_TRADES)
            : next
        })
      },
      onError: (message) => setError(message),
    })
  }, [symbol])

  return { trades, loading, error, live }
}
