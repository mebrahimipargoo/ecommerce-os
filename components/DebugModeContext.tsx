"use client";

/**
 * Technical debug (#db: table tags).
 * — UI state is read only from localStorage (synchronous on the client) so the toggle never flips
 *   after a server round-trip. Server persistence is write-only via saveOrganizationDebugMode.
 */

import React, {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useSyncExternalStore,
} from "react";
import { saveOrganizationDebugMode } from "../app/settings/debug-mode-actions";

const DEBUG_MODE_LS_KEY = "ecommerce_os_debug_mode";
const DEBUG_MODE_STORE_EVENT = "ecommerce_os_debug_mode_changed";

function readDebugSnapshot(): boolean {
  if (typeof window === "undefined") return false;
  try {
    return localStorage.getItem(DEBUG_MODE_LS_KEY) === "1";
  } catch {
    return false;
  }
}

function subscribeDebugStore(onStore: () => void): () => void {
  if (typeof window === "undefined") return () => {};
  const run = () => onStore();
  window.addEventListener(DEBUG_MODE_STORE_EVENT, run);
  window.addEventListener("storage", run);
  return () => {
    window.removeEventListener(DEBUG_MODE_STORE_EVENT, run);
    window.removeEventListener("storage", run);
  };
}

type DebugModeContextValue = {
  debugMode: boolean;
  loaded: boolean;
  setDebugMode: (enabled: boolean) => void;
  /** Re-read localStorage and notify subscribers (no server fetch). */
  refresh: () => void;
};

const DebugModeContext = createContext<DebugModeContextValue | null>(null);

export function DebugModeProvider({ children }: { children: React.ReactNode }) {
  const debugMode = useSyncExternalStore(
    subscribeDebugStore,
    readDebugSnapshot,
    readDebugSnapshot,
  );
  const loaded = true;

  const refresh = useCallback(() => {
    if (typeof window === "undefined") return;
    window.dispatchEvent(new Event(DEBUG_MODE_STORE_EVENT));
  }, []);

  const setDebugMode = useCallback((enabled: boolean) => {
    try {
      localStorage.setItem(DEBUG_MODE_LS_KEY, enabled ? "1" : "0");
    } catch {
      /* ignore */
    }
    if (typeof window !== "undefined") {
      window.dispatchEvent(new Event(DEBUG_MODE_STORE_EVENT));
    }
    void saveOrganizationDebugMode(enabled).then((res) => {
      if (!res.ok) {
        console.warn("[DebugMode] server save failed (UI + localStorage unchanged):", res.error);
      }
    });
  }, []);

  const value = useMemo(
    () => ({ debugMode, loaded, setDebugMode, refresh }),
    [debugMode, loaded, setDebugMode, refresh],
  );

  return (
    <DebugModeContext.Provider value={value}>{children}</DebugModeContext.Provider>
  );
}

export function useDebugMode(): DebugModeContextValue {
  const ctx = useContext(DebugModeContext);
  if (!ctx) {
    return {
      debugMode: false,
      loaded: true,
      setDebugMode: () => {},
      refresh: () => {},
    };
  }
  return ctx;
}

export const useGlobalDebugMode = useDebugMode;
