import type { Bar } from "@/api/bars"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"

function fmt(n: number | null | undefined, digits = 2) {
  if (n == null || Number.isNaN(n)) return "—"
  return n.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  })
}

const BASE_STATS: { key: keyof Bar; label: string; digits?: number }[] = [
  { key: "open", label: "Open" },
  { key: "high", label: "High" },
  { key: "low", label: "Low" },
  { key: "close", label: "Close" },
  { key: "volume", label: "Volume", digits: 0 },
  { key: "vwap", label: "VWAP" },
  { key: "trade_count", label: "Trades", digits: 0 },
]

export function BarStats({
  bar,
  loading,
  showNotional = false,
}: {
  bar: Bar | null
  loading?: boolean
  showNotional?: boolean
}) {
  const stats = showNotional
    ? [...BASE_STATS, { key: "notional" as keyof Bar, label: "Notional", digits: 0 }]
    : BASE_STATS

  if (loading) {
    return (
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4 lg:grid-cols-7">
        {stats.map((s) => (
          <Skeleton key={String(s.key)} className="h-20 rounded-xl" />
        ))}
      </div>
    )
  }

  return (
    <div className="space-y-2">
      {bar?.is_partial && (
        <p className="text-xs text-muted-foreground">
          Forming candle (updates live until the Flink window closes)
        </p>
      )}
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4 lg:grid-cols-7">
        {stats.map((s) => (
          <Card key={String(s.key)} size="sm" className="gap-2 py-3">
            <CardHeader className="px-4 pb-0">
              <CardTitle className="text-xs font-medium tracking-wide text-muted-foreground uppercase">
                {s.label}
              </CardTitle>
            </CardHeader>
            <CardContent className="px-4 font-heading text-lg tabular-nums">
              {bar ? fmt(bar[s.key] as number, s.digits ?? 2) : "—"}
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  )
}
