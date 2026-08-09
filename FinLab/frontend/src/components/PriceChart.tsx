import { useEffect, useRef } from "react"
import {
  CandlestickSeries,
  ColorType,
  createChart,
  type IChartApi,
  type ISeriesApi,
  type CandlestickData,
  type Time,
} from "lightweight-charts"

import type { Bar } from "@/api/bars"
import { useTheme } from "@/components/theme-provider"

function toCandle(bar: Bar): CandlestickData<Time> {
  const t = Math.floor(new Date(bar.window_start).getTime() / 1000) as Time
  return {
    time: t,
    open: bar.open,
    high: bar.high,
    low: bar.low,
    close: bar.close,
  }
}

export function PriceChart({
  bars,
  symbol,
}: {
  bars: Bar[]
  symbol: string | null
}) {
  const containerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null)
  const { theme } = useTheme()

  useEffect(() => {
    const el = containerRef.current
    if (!el) return

    // lightweight-charts only accepts hex/rgb/rgba — not oklch() CSS vars.
    const isDark =
      document.documentElement.classList.contains("dark") ||
      theme === "dark" ||
      (theme === "system" &&
        window.matchMedia("(prefers-color-scheme: dark)").matches)

    const chart = createChart(el, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: isDark ? "#f5f5f4" : "#1c1917",
        fontFamily: "Figtree Variable, sans-serif",
      },
      grid: {
        vertLines: {
          color: isDark ? "rgba(255,255,255,0.06)" : "rgba(0,0,0,0.06)",
        },
        horzLines: {
          color: isDark ? "rgba(255,255,255,0.06)" : "rgba(0,0,0,0.06)",
        },
      },
      rightPriceScale: { borderVisible: false },
      timeScale: { borderVisible: false, timeVisible: true, secondsVisible: false },
      crosshair: { mode: 1 },
    })

    const series = chart.addSeries(CandlestickSeries, {
      upColor: "#10b981",
      downColor: "#ef4444",
      borderUpColor: "#10b981",
      borderDownColor: "#ef4444",
      wickUpColor: "#10b981",
      wickDownColor: "#ef4444",
    })

    chartRef.current = chart
    seriesRef.current = series

    return () => {
      chart.remove()
      chartRef.current = null
      seriesRef.current = null
    }
  }, [theme])

  useEffect(() => {
    const series = seriesRef.current
    const chart = chartRef.current
    if (!series || !chart) return
    const data = bars.map(toCandle)
    series.setData(data)
    if (data.length) chart.timeScale().fitContent()
  }, [bars, symbol])

  return (
    <div className="relative h-[420px] w-full min-w-0">
      {!symbol && (
        <div className="absolute inset-0 z-10 flex items-center justify-center text-sm text-muted-foreground">
          Select a symbol from the watchlist
        </div>
      )}
      {symbol && bars.length === 0 && (
        <div className="absolute inset-0 z-10 flex items-center justify-center text-sm text-muted-foreground">
          No bars for {symbol}
        </div>
      )}
      <div ref={containerRef} className="h-full w-full" />
    </div>
  )
}
