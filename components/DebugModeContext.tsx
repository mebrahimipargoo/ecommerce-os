"use client";

/**
 * Technical debug (#db: table tags).
 * — localStorage + custom event update the UI immediately (no await on server).
 * — useSyncExternalStore keeps all subscribers in sync after refresh.
 * — Background save to organization_settings is fire-and-forget.
 */

import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useLayoutEffect,
  useMemo,
  useSyncExternalStore,
} from "react";
import {
  getOrganizationDebugMode,
  saveOrganizationDebugMode,
} from "../app/settings/debug-mode-actions";

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
  /** Always true after mount — toggle never waits on network. */
  loaded: boolean;
  setDebugMode: (enabled: boolean) => void;
  refresh: () => void;
};

const DebugModeContext = createContext<DebugModeContextValue | null>(null);

export function DebugModeProvider({ children }: { children: React.ReactNode }) {
  const debugMode = useSyncExternalStore(subscribeDebugStore, readDebugSnapshot, () => false);
  const loaded = true;

  /** Re-dispatch on mount so persisted LS applies after full page load. */
  useEffect(() => {
    if (typeof window === "undefined") return;
    try {
      if (localStorage.getItem(DEBUG_MODE_LS_KEY) != null) {
        window.dispatchEvent(new Event(DEBUG_MODE_STORE_EVENT));
      }
    } catch {
      /* ignore */
    }
  }, []);

  useLayoutEffect(() => {
    let cancelled = false;
    try {
      if (localStorage.getItem(DEBUG_MODE_LS_KEY) != null) return;
    } catch {
      return;
    }
    void (async () => {
      const v = await getOrganizationDebugMode();
      if (cancelled) return;
      try {
        localStorage.setItem(DEBUG_MODE_LS_KEY, v ? "1" : "0");
      } catch {
        /* ignore */
      }
      if (typeof window !== "undefined") {
        window.dispatchEvent(new Event(DEBUG_MODE_STORE_EVENT));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const refresh = useCallback(() => {
    void (async () => {
      const v = await getOrganizationDebugMode();
      try {
        localStorage.setItem(DEBUG_MODE_LS_KEY, v ? "1" : "0");
      } catch {
        /* ignore */
      }
      if (typeof window !== "undefined") {
        window.dispatchEvent(new Event(DEBUG_MODE_STORE_EVENT));
      }
    })();
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
