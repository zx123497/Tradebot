import { MoonIcon, SunIcon } from "@phosphor-icons/react"

import { useTheme } from "@/components/theme-provider"
import { Button } from "@/components/ui/button"

function resolveIsDark(theme: "dark" | "light" | "system") {
  if (theme === "dark") return true
  if (theme === "light") return false
  return window.matchMedia("(prefers-color-scheme: dark)").matches
}

export function ThemeToggle() {
  const { theme, setTheme } = useTheme()
  const isDark = resolveIsDark(theme)

  return (
    <Button
      type="button"
      variant="outline"
      size="icon"
      aria-label={isDark ? "Switch to light mode" : "Switch to dark mode"}
      onClick={() => setTheme(isDark ? "light" : "dark")}
    >
      {isDark ? (
        <SunIcon className="size-4" weight="duotone" />
      ) : (
        <MoonIcon className="size-4" weight="duotone" />
      )}
    </Button>
  )
}
