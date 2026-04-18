"use client";

import { useTheme } from "next-themes";
import { useEffect, useState } from "react";
import { Sun, Moon } from "lucide-react";

export function ThemeToggle() {
  const { resolvedTheme, setTheme } = useTheme();
  const [mounted, setMounted] = useState(false);

  useEffect(() => { setMounted(true); }, []);

  const isDark = mounted && resolvedTheme === "dark";

  return (
    <button
      type="button"
      disabled={!mounted}
      onClick={() => setTheme(isDark ? "light" : "dark")}
      aria-label={
        !mounted
          ? "Toggle color theme"
          : isDark
            ? "Switch to light mode"
            : "Switch to dark mode"
      }
      className="group relative flex h-9 w-9 items-center justify-center overflow-hidden rounded-full border border-border bg-card text-card-foreground shadow-sm transition hover:bg-accent hover:text-accent-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-70"
    >
      {/* Sun — shown in dark mode to switch to light */}
      <Sun
        className={[
          "absolute h-4 w-4 text-amber-500 transition-all duration-300",
          isDark ? "translate-y-0 rotate-0 opacity-100" : "-translate-y-5 rotate-90 opacity-0",
        ].join(" ")}
      />
      {/* Moon — shown in light mode to switch to dark */}
      <Moon
        className={[
          "absolute h-4 w-4 text-foreground transition-all duration-300",
          isDark ? "translate-y-5 -rotate-90 opacity-0" : "translate-y-0 rotate-0 opacity-100",
        ].join(" ")}
      />
    </button>
  );
}
