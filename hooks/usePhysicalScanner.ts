"use client";

/**
 * usePhysicalScanner
 *
 * Detects hardware barcode-scanner input by monitoring global keydown events.
 * Physical scanners emulate a keyboard but fire keystrokes extremely fast
 * (typically 1–5 ms apart) and terminate the sequence with Enter.
 *
 * Detection logic:
 *  • Each printable character is appended to an in-memory buffer if it arrives
 *    within CHAR_INTERVAL_MS of the previous character.
 *  • When Enter is detected and the buffer has ≥ MIN_LENGTH characters, AND the
 *    Enter itself arrives within ENTER_INTERVAL_MS of the last character, the
 *    accumulated string is treated as a scanned barcode.
 *  • The Enter keydown event is prevented (stops accidental form submissions).
 *  • If keystrokes are too slow (human typing), the buffer resets with the
 *    latest character — normal keyboard input is unaffected.
 *
 * Usage:
 *   const { scannedBarcode, clearScan } = usePhysicalScanner({
 *     onScan: (code) => handleBarcode(code),
 *   });
 *
 *   // Or watch scannedBarcode in a useEffect if onScan isn't convenient.
 */

import { useState, useEffect, useCallback, useRef } from "react";

/** Max milliseconds between consecutive characters to consider scanner input. */
const CHAR_INTERVAL_MS = 30;

/**
 * Max milliseconds between the last character and the Enter key.
 * Slightly more lenient than CHAR_INTERVAL_MS because scanners sometimes
 * pause briefly before sending Enter.
 */
const ENTER_INTERVAL_MS = 100;

/** Minimum number of characters required to trigger a scan event. */
const MIN_LENGTH = 3;

export interface UsePhysicalScannerOptions {
  /** Callback fired when a barcode is successfully detected. */
  onScan?: (barcode: string) => void;
  /** Set to false to temporarily disable the listener. Defaults to true. */
  enabled?: boolean;
}

export interface UsePhysicalScannerResult {
  /**
   * The most recently scanned barcode string, or null if none since last clear.
   * Inputs / forms can watch this value with useEffect to auto-fill.
   */
  scannedBarcode: string | null;
  /** Call this to reset scannedBarcode back to null after you've consumed it. */
  clearScan: () => void;
}

export function usePhysicalScanner({
  onScan,
  enabled = true,
}: UsePhysicalScannerOptions = {}): UsePhysicalScannerResult {
  const [scannedBarcode, setScannedBarcode] = useState<string | null>(null);

  // Keep onScan ref-stable so the effect doesn't re-register on every render.
  const onScanRef = useRef(onScan);
  useEffect(() => { onScanRef.current = onScan; });

  useEffect(() => {
    if (!enabled) return;

    let buffer      = "";
    let lastKeyTime = 0;   // timestamp of last captured character (0 = none yet)

    function handleKeyDown(e: KeyboardEvent) {
      const now = Date.now();

      if (e.key === "Enter") {
        const gap = lastKeyTime > 0 ? now - lastKeyTime : Infinity;

        if (buffer.length >= MIN_LENGTH && gap <= ENTER_INTERVAL_MS) {
          // Rapid sequence ending with Enter → barcode scan detected
          e.preventDefault();
          e.stopPropagation();

          const barcode = buffer;
          buffer      = "";
          lastKeyTime = 0;

          setScannedBarcode(barcode);
          onScanRef.current?.(barcode);
        } else {
          // Normal Enter (human typing) — do not interfere
          buffer      = "";
          lastKeyTime = 0;
        }
        return;
      }

      // Only track printable single characters (ignore Shift, Ctrl, F-keys, etc.)
      if (e.key.length !== 1) return;

      const gap = lastKeyTime > 0 ? now - lastKeyTime : Infinity;

      if (gap <= CHAR_INTERVAL_MS) {
        // Fast enough → scanner mode, extend buffer
        buffer += e.key;
      } else {
        // Too slow for a scanner → human keystroke, start fresh
        buffer = e.key;
      }

      lastKeyTime = now;
    }

    document.addEventListener("keydown", handleKeyDown, { capture: true });
    return () => document.removeEventListener("keydown", handleKeyDown, { capture: true });
  }, [enabled]);

  const clearScan = useCallback(() => setScannedBarcode(null), []);

  return { scannedBarcode, clearScan };
}
