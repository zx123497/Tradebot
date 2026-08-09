import type { TradeTick } from "@/api/trades"
import { LiveBadge } from "@/components/LiveBadge"
import { Skeleton } from "@/components/ui/skeleton"
import { cn } from "@/lib/utils"

function fmtPrice(n: number) {
  return n.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 4,
  })
}

function fmtVol(n: number) {
  return n.toLocaleString(undefined, { maximumFractionDigits: 0 })
}

function fmtTime(iso: string) {
  const d = new Date(iso)
  const base = d.toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  })
  const ms = String(d.getMilliseconds()).padStart(3, "0")
  return `${base}.${ms}`
}

export function LiveTradesPanel({
  trades,
  loading,
  live,
  error,
  symbol,
}: {
  trades: TradeTick[]
  loading?: boolean
  live?: boolean
  error?: string | null
  symbol: string | null
}) {
  const newestFirst = [...trades].reverse()

  return (
    <div className="flex h-full min-h-[280px] flex-col">
      <div className="flex items-center justify-between gap-2 border-b px-4 py-3">
        <div>
          <p className="text-sm font-medium">Live trades</p>
          <p className="text-xs text-muted-foreground">
            {symbol ? `${symbol} ticks` : "All symbols"} · ClickHouse stream
          </p>
        </div>
        <LiveBadge live={!!live} />
      </div>

      {error && (
        <p className="px-4 py-2 text-xs text-destructive">{error}</p>
      )}

      <div className="grid grid-cols-[72px_1fr_1fr_1fr] gap-2 border-b px-4 py-2 text-[10px] font-medium tracking-wide text-muted-foreground uppercase">
        <span>Time</span>
        <span>Sym</span>
        <span className="text-right">Price</span>
        <span className="text-right">Size</span>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        {loading && newestFirst.length === 0 ? (
          <div className="space-y-2 p-4">
            {Array.from({ length: 8 }).map((_, i) => (
              <Skeleton key={i} className="h-6 w-full" />
            ))}
          </div>
        ) : newestFirst.length === 0 ? (
          <p className="p-4 text-sm text-muted-foreground">
            Waiting for trades…
          </p>
        ) : (
          <ul className="divide-y divide-border/60">
            {newestFirst.map((t, i) => (
              <li
                key={`${t.timestamp}-${t.symbol}-${t.price}-${t.volume}-${i}`}
                className={cn(
                  "grid grid-cols-[72px_1fr_1fr_1fr] gap-2 px-4 py-1.5 font-mono text-xs tabular-nums",
                  i === 0 && "bg-emerald-500/5"
                )}
              >
                <span className="truncate text-muted-foreground">
                  {fmtTime(t.timestamp)}
                </span>
                <span className="font-sans font-medium">{t.symbol}</span>
                <span className="text-right">{fmtPrice(t.price)}</span>
                <span className="text-right text-muted-foreground">
                  {fmtVol(t.volume)}
                </span>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  )
}
