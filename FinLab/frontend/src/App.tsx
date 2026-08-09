import { useEffect, useState } from "react"

import { BAR_TYPES, type BarType } from "@/api/bars"
import { LiveBadge } from "@/components/LiveBadge"
import { BarStats } from "@/components/BarStats"
import { LiveTradesPanel } from "@/components/LiveTradesPanel"
import { PriceChart } from "@/components/PriceChart"
import { ThemeToggle } from "@/components/ThemeToggle"
import { WatchlistTable } from "@/components/WatchlistTable"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Separator } from "@/components/ui/separator"
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { useBars } from "@/hooks/useBars"
import { useSymbols } from "@/hooks/useSymbols"
import { useTrades } from "@/hooks/useTrades"

export function App() {
  const [barType, setBarType] = useState<BarType>("1m")
  const { symbols, loading: symbolsLoading, error: symbolsError } =
    useSymbols(barType)
  const [selected, setSelected] = useState<string | null>(null)
  const { bars, latest, loading: barsLoading, error: barsError, live } =
    useBars(selected, barType)
  const {
    trades,
    loading: tradesLoading,
    error: tradesError,
    live: tradesLive,
  } = useTrades(selected)

  useEffect(() => {
    if (!selected && symbols.length) {
      setSelected(symbols[0].symbol)
    }
  }, [symbols, selected])

  const barLabel =
    BAR_TYPES.find((t) => t.value === barType)?.label ?? barType

  return (
    <div className="min-h-svh bg-background text-foreground">
      <header className="border-b border-border/80">
        <div className="mx-auto flex max-w-7xl items-center justify-between gap-4 px-4 py-4 sm:px-6">
          <div className="min-w-0">
            <p className="font-heading text-2xl tracking-tight">FinLab</p>
            
          </div>
          <div className="flex items-center gap-3">
            <LiveBadge live={live || tradesLive} />
            <ThemeToggle />
          </div>
        </div>
      </header>

      <main className="mx-auto grid max-w-7xl gap-4 px-4 py-4 sm:px-6 lg:grid-cols-[280px_1fr_300px]">
        <Card className="overflow-hidden py-0 lg:row-span-2">
          <CardHeader className="border-b px-4 py-3">
            <CardTitle className="text-sm font-medium">Watchlist</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            {symbolsError ? (
              <p className="p-4 text-sm text-destructive">{symbolsError}</p>
            ) : (
              <WatchlistTable
                symbols={symbols}
                selected={selected}
                onSelect={setSelected}
                loading={symbolsLoading}
              />
            )}
          </CardContent>
        </Card>

        <div className="flex min-w-0 flex-col gap-4">
          <Card className="overflow-hidden py-0">
            <CardHeader className="flex flex-col gap-3 border-b px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
              <CardTitle className="font-heading text-lg">
                {selected ?? "—"}{" "}
                <span className="text-sm font-normal text-muted-foreground">
                  {barLabel} candles
                </span>
              </CardTitle>
              <Tabs
                value={barType}
                onValueChange={(v) => setBarType(v as BarType)}
              >
                <TabsList variant="default" className="h-9">
                  {BAR_TYPES.map((t) => (
                    <TabsTrigger key={t.value} value={t.value} className="px-3">
                      {t.label}
                    </TabsTrigger>
                  ))}
                </TabsList>
              </Tabs>
            </CardHeader>
            {barsError && (
              <p className="px-4 pt-2 text-xs text-destructive">{barsError}</p>
            )}
            <CardContent className="p-2 sm:p-4">
              <PriceChart
                key={`${selected}-${barType}`}
                bars={bars}
                symbol={selected}
              />
            </CardContent>
          </Card>

          <div>
            <div className="mb-2 flex items-center gap-2">
              <h2 className="text-sm font-medium">Latest bar</h2>
              <Separator className="flex-1" />
            </div>
            <BarStats
              bar={latest}
              loading={barsLoading && !latest}
              showNotional={barType === "volume" || barType === "dollar"}
            />
          </div>
        </div>

        <Card className="overflow-hidden py-0 lg:row-span-2">
          <CardContent className="h-[min(70vh,640px)] p-0">
            <LiveTradesPanel
              trades={trades}
              loading={tradesLoading}
              live={tradesLive}
              error={tradesError}
              symbol={selected}
            />
          </CardContent>
        </Card>
      </main>
    </div>
  )
}

export default App
