export type TradeTick = {
  symbol: string
  price: number
  volume: number
  timestamp: string
}

export type TradesResponse = {
  trades: TradeTick[]
}

async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(path)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || `Request failed: ${res.status}`)
  }
  return res.json() as Promise<T>
}

export function getTrades(opts?: {
  symbol?: string
  limit?: number
}): Promise<TradesResponse> {
  const params = new URLSearchParams()
  if (opts?.symbol) params.set("symbol", opts.symbol)
  if (opts?.limit != null) params.set("limit", String(opts.limit))
  const qs = params.size ? `?${params}` : ""
  return apiGet(`/api/trades${qs}`)
}

export type TradesSSEHandlers = {
  onTrade?: (trade: TradeTick) => void
  onHello?: () => void
  onError?: (message: string) => void
  onStatus?: (connected: boolean) => void
}

export function subscribeTradesSSE(
  symbols: string[] | undefined,
  handlers: TradesSSEHandlers
): () => void {
  const params = new URLSearchParams()
  if (symbols?.length) params.set("symbols", symbols.join(","))
  const url = `/api/trades/stream${params.size ? `?${params}` : ""}`
  const es = new EventSource(url)

  es.addEventListener("open", () => handlers.onStatus?.(true))
  es.addEventListener("hello", () => {
    handlers.onStatus?.(true)
    handlers.onHello?.()
  })
  es.addEventListener("trade", (ev) => {
    try {
      const trade = JSON.parse((ev as MessageEvent).data) as TradeTick
      handlers.onTrade?.(trade)
    } catch (err) {
      handlers.onError?.(String(err))
    }
  })
  es.addEventListener("error", (ev) => {
    if (ev instanceof MessageEvent && ev.data) {
      try {
        const payload = JSON.parse(ev.data) as { error?: string }
        handlers.onError?.(payload.error ?? ev.data)
      } catch {
        handlers.onError?.(String(ev.data))
      }
    } else {
      handlers.onStatus?.(false)
    }
  })
  es.onerror = () => handlers.onStatus?.(false)

  return () => {
    handlers.onStatus?.(false)
    es.close()
  }
}
