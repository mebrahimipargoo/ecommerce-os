"use client";

/**
 * AdminDebugContext — Global admin debug gate for the App Shell.
 *
 * Public API surface:
 *   isDebugMode    — boolean read by any consumer to gate debug UI
 *   toggleDebugMode — flip the master switch
 *
 * Implementation delegates to DebugModeContext (components/DebugModeContext.tsx)
 * for localStorage persistence, cross-tab sync via useSyncExternalStore, and
 * the granular sub-flags (showDbTableNames, showApiLogs, showRawJson).
 *
 * Provider hierarchy (set in app/layout.tsx):
 *   AdminDebugProvider
 *     └─ DebugModeProvider   ← storage + granular flags
 *       └─ AdminDebugContextBridge ← maps debugMode → isDebugMode
 */

import React, { createContext, useContext } from "react";
import {
  DebugModeProvider,
  useDebugMode,
} from "../components/DebugModeContext";

// ─── Public context shape ─────────────────────────────────────────────────────

export type AdminDebugContextValue = {
  /** True when the global admin debug mode is active. */
  isDebugMode: boolean;
  /** Toggle debug mode on / off. */
  toggleDebugMode: () => void;
};

const AdminDebugContext = createContext<AdminDebugContextValue>({
  isDebugMode: false,
  toggleDebugMode: () => {},
});

// ─── Bridge (maps DebugModeContext → AdminDebugContext) ───────────────────────

function AdminDebugContextBridge({ children }: { children: React.ReactNode }) {
  const { debugMode, setDebugMode } = useDebugMode();

  return (
    <AdminDebugContext.Provider
      value={{
        isDebugMode: debugMode,
        toggleDebugMode: () => setDebugMode(!debugMode),
      }}
    >
      {children}
    </AdminDebugContext.Provider>
  );
}

// ─── Public provider ──────────────────────────────────────────────────────────

/**
 * AdminDebugProvider — place at the root of the app (inside ThemeProvider).
 * Wraps DebugModeProvider internally so no separate DebugModeProvider is needed.
 */
export function AdminDebugProvider({ children }: { children: React.ReactNode }) {
  return (
    <DebugModeProvider>
      <AdminDebugContextBridge>{children}</AdminDebugContextBridge>
    </DebugModeProvider>
  );
}

// ─── Hook ─────────────────────────────────────────────────────────────────────

/**
 * useAdminDebug — consume isDebugMode / toggleDebugMode from any client component.
 *
 * @example
 *   const { isDebugMode, toggleDebugMode } = useAdminDebug();
 */
export function useAdminDebug(): AdminDebugContextValue {
  return useContext(AdminDebugContext);
}

// Re-export DebugModeContext utilities so callers can import everything from one path
export { useDebugMode } from "../components/DebugModeContext";
export type { DebugModeContextValue } from "../components/DebugModeContext";
