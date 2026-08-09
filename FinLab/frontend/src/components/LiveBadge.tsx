import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"

export function LiveBadge({ live }: { live: boolean }) {
  return (
    <Badge
      variant="outline"
      className={cn(
        "gap-1.5 font-mono text-[10px] tracking-wider uppercase",
        live
          ? "border-emerald-500/40 text-emerald-600 dark:text-emerald-400"
          : "text-muted-foreground"
      )}
    >
      <span
        className={cn(
          "size-1.5 rounded-full",
          live ? "animate-pulse bg-emerald-500" : "bg-muted-foreground/50"
        )}
      />
      {live ? "Live" : "Offline"}
    </Badge>
  )
}
