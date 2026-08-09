export type BarType = "1m" | "5m" | "volume" | "dollar"

export const BAR_TYPES: { value: BarType; label: string }[] = [
  { value: "1m", label: "1 min" },
  { value: "5m", label: "5 min" },
  { value: "dollar", label: "Dollar" },
  { value: "volume", label: "Volume" },
]

export type Bar = {
  symbol: string
  window_start: string
  open: number
  high: number
  low: number
  close: number
  volume: number
  trade_count: number
  vwap: number
  notional?: number | null
}

export type SymbolSummary = Bar & {
  prev_close: number | null
  change: number | null
  change_pct: number | null
}

export type BarsResponse = {
  symbol: string
  bar_type: string
  bars: Bar[]
}

async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(path)
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || `Request failed: ${res.status}`)
  }
  return res.json() as Promise<T>
}

export function getSymbols(barType: BarType = "1m"): Promise<SymbolSummary[]> {
  const params = new URLSearchParams({ bar_type: barType })
  return apiGet(`/api/symbols?${params}`)
}

export function getBars(
  symbol: string,
  opts?: {
    barType?: BarType
    from?: string
    to?: string
    limit?: number
  }
): Promise<BarsResponse> {
  const params = new URLSearchParams({
    symbol,
    bar_type: opts?.barType ?? "1m",
  })
  if (opts?.from) params.set("from", opts.from)
  if (opts?.to) params.set("to", opts.to)
  if (opts?.limit != null) params.set("limit", String(opts.limit))
  return apiGet(`/api/bars?${params}`)
}

export type BarsSSEHandlers = {
  onBar?: (bar: Bar) => void
  onHello?: () => void
  onError?: (message: string) => void
  onStatus?: (connected: boolean) => void
}

export function subscribeBarsSSE(
  symbols: string[] | undefined,
  handlers: BarsSSEHandlers,
  barType: BarType = "1m"
): () => void {
  const params = new URLSearchParams({ bar_type: barType })
  if (symbols?.length) params.set("symbols", symbols.join(","))
  const url = `/api/bars/stream?${params}`
  const es = new EventSource(url)

  es.addEventListener("open", () => handlers.onStatus?.(true))
  es.addEventListener("hello", () => {
    handlers.onStatus?.(true)
    handlers.onHello?.()
  })
  es.addEventListener("bar", (ev) => {
    try {
      const bar = JSON.parse((ev as MessageEvent).data) as Bar
      handlers.onBar?.(bar)
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
