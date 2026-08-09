import type { SymbolSummary } from "@/api/bars"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Skeleton } from "@/components/ui/skeleton"
import { cn } from "@/lib/utils"

function fmtPrice(n: number) {
  return n.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 4,
  })
}

function fmtTime(iso: string) {
  try {
    return new Date(iso).toLocaleTimeString(undefined, {
      hour: "2-digit",
      minute: "2-digit",
      timeZone: "UTC",
      hour12: false,
    })
  } catch {
    return "—"
  }
}

export function WatchlistTable({
  symbols,
  selected,
  onSelect,
  loading,
}: {
  symbols: SymbolSummary[]
  selected: string | null
  onSelect: (symbol: string) => void
  loading?: boolean
}) {
  if (loading) {
    return (
      <div className="space-y-2 p-3">
        {Array.from({ length: 6 }).map((_, i) => (
          <Skeleton key={i} className="h-9 w-full" />
        ))}
      </div>
    )
  }

  if (!symbols.length) {
    return (
      <div className="p-6 text-sm text-muted-foreground">
        No symbols in ClickHouse yet. Run the FinData pipeline / consumer so
        <code className="mx-1 font-mono text-xs">bars_1m</code>
        has rows.
      </div>
    )
  }

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Symbol</TableHead>
          <TableHead className="text-right">Last</TableHead>
          <TableHead className="text-right">Chg%</TableHead>
          <TableHead className="text-right">Vol</TableHead>
          <TableHead className="text-right">UTC</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {symbols.map((s) => {
          const up = (s.change ?? 0) > 0
          const down = (s.change ?? 0) < 0
          return (
            <TableRow
              key={s.symbol}
              data-state={selected === s.symbol ? "selected" : undefined}
              className="cursor-pointer"
              onClick={() => onSelect(s.symbol)}
            >
              <TableCell className="font-heading font-medium">
                {s.symbol}
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {fmtPrice(s.close)}
              </TableCell>
              <TableCell
                className={cn(
                  "text-right tabular-nums",
                  up && "text-emerald-600 dark:text-emerald-400",
                  down && "text-red-600 dark:text-red-400"
                )}
              >
                {s.change_pct == null
                  ? "—"
                  : `${s.change_pct >= 0 ? "+" : ""}${s.change_pct.toFixed(2)}%`}
              </TableCell>
              <TableCell className="text-right tabular-nums text-muted-foreground">
                {s.volume.toLocaleString(undefined, { maximumFractionDigits: 0 })}
              </TableCell>
              <TableCell className="text-right font-mono text-xs text-muted-foreground">
                {fmtTime(s.window_start)}
              </TableCell>
            </TableRow>
          )
        })}
      </TableBody>
    </Table>
  )
}
