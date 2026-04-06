"use client";

/**
 * DebugModeContext — master debug toggle + granular developer flags.
 *
 * Master `debugMode` uses useSyncExternalStore for cross-tab sync.
 * Granular flags use useState + useEffect (localStorage after mount) for SSR safety.
 * Server persistence for master toggle: saveOrganizationDebugMode.
 */

import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  useSyncExternalStore,
} from "react";
import { saveOrganizationDebugMode } from "../app/settings/debug-mode-actions";

const DEBUG_MODE_LS_KEY = "ecommerce_os_debug_mode";
const DEBUG_MODE_STORE_EVENT = "ecommerce_os_debug_mode_changed";
const DEBUG_DB_TABLES_KEY = "ecommerce_os_debug_db_tables";
const DEBUG_API_LOGS_KEY = "ecommerce_os_debug_api_logs";
const DEBUG_RAW_JSON_KEY = "ecommerce_os_debug_raw_json";

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

export type DebugModeContextValue = {
  debugMode: boolean;
  loaded: boolean;
  setDebugMode: (enabled: boolean) => void;
  refresh: () => void;
  showDbTableNames: boolean;
  setShowDbTableNames: (v: boolean) => void;
  showApiLogs: boolean;
  setShowApiLogs: (v: boolean) => void;
  showRawJson: boolean;
  setShowRawJson: (v: boolean) => void;
  resetAllDebugFlags: () => void;
};

const DebugModeContext = createContext<DebugModeContextValue | null>(null);

export function DebugModeProvider({ children }: { children: React.ReactNode }) {
  const debugMode = useSyncExternalStore(
    subscribeDebugStore,
    readDebugSnapshot,
    readDebugSnapshot,
  );
  const loaded = true;

  const [showDbTableNames, setShowDbTableNamesState] = useState(false);
  const [showApiLogs, setShowApiLogsState] = useState(false);
  const [showRawJson, setShowRawJsonState] = useState(false);

  useEffect(() => {
    try {
      setShowDbTableNamesState(localStorage.getItem(DEBUG_DB_TABLES_KEY) === "1");
      setShowApiLogsState(localStorage.getItem(DEBUG_API_LOGS_KEY) === "1");
      setShowRawJsonState(localStorage.getItem(DEBUG_RAW_JSON_KEY) === "1");
    } catch {
      /* ignore */
    }
  }, []);

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

  const setShowDbTableNames = useCallback((v: boolean) => {
    setShowDbTableNamesState(v);
    try {
      localStorage.setItem(DEBUG_DB_TABLES_KEY, v ? "1" : "0");
    } catch {
      /* ignore */
    }
  }, []);

  const setShowApiLogs = useCallback((v: boolean) => {
    setShowApiLogsState(v);
    try {
      localStorage.setItem(DEBUG_API_LOGS_KEY, v ? "1" : "0");
    } catch {
      /* ignore */
    }
  }, []);

  const setShowRawJson = useCallback((v: boolean) => {
    setShowRawJsonState(v);
    try {
      localStorage.setItem(DEBUG_RAW_JSON_KEY, v ? "1" : "0");
    } catch {
      /* ignore */
    }
  }, []);

  const resetAllDebugFlags = useCallback(() => {
    setShowDbTableNames(false);
    setShowApiLogs(false);
    setShowRawJson(false);
    setDebugMode(false);
  }, [setShowDbTableNames, setShowApiLogs, setShowRawJson, setDebugMode]);

  const value = useMemo(
    () => ({
      debugMode,
      loaded,
      setDebugMode,
      refresh,
      showDbTableNames,
      setShowDbTableNames,
      showApiLogs,
      setShowApiLogs,
      showRawJson,
      setShowRawJson,
      resetAllDebugFlags,
    }),
    [
      debugMode,
      loaded,
      setDebugMode,
      refresh,
      showDbTableNames,
      setShowDbTableNames,
      showApiLogs,
      setShowApiLogs,
      showRawJson,
      setShowRawJson,
      resetAllDebugFlags,
    ],
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
      showDbTableNames: false,
      setShowDbTableNames: () => {},
      showApiLogs: false,
      setShowApiLogs: () => {},
      showRawJson: false,
      setShowRawJson: () => {},
      resetAllDebugFlags: () => {},
    };
  }
  return ctx;
}

export const useGlobalDebugMode = useDebugMode;
