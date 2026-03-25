"use client";

/**
 * usePhysicalScanner
 *
 * Detects hardware barcode-scanner input by monitoring keydown events on a
 * SPECIFIC input element — NOT globally on document/window.
 *
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
 *  • If keystrokes are too slow (human typing), the buffer resets — normal
 *    keyboard input in Notes / Search fields is completely unaffected.
 *
 * Usage — attach onKeyDown directly to a designated barcode or tracking input:
 *
 *   const { onKeyDown } = usePhysicalScanner({
 *     onScan: (code) => handleBarcode(code),
 *   });
 *
 *   <input onKeyDown={(e) => { onKeyDown(e); /* your other handlers *\/ }} />
 *
 * Only inputs that explicitly receive onKeyDown participate in scanner
 * detection. Notes fields, search boxes, etc. are never affected.
 */

import { useState, useCallback, useRef, useEffect } from "react";

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
  /** Set to false to temporarily disable the scanner logic. Defaults to true. */
  enabled?: boolean;
}

export interface UsePhysicalScannerResult {
  /**
   * Attach this handler directly to the onKeyDown of a barcode / tracking
   * <input>. Do NOT spread it onto Notes or Search fields.
   */
  onKeyDown: (e: React.KeyboardEvent<HTMLInputElement>) => void;
  /**
   * The most recently scanned barcode string, or null if none since last clear.
   * Usually you consume the value via the onScan callback instead.
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

  // Keep onScan ref-stable so the handler doesn't go stale across renders.
  const onScanRef = useRef(onScan);
  useEffect(() => { onScanRef.current = onScan; });

  // Buffer lives in refs so it persists between keydown calls without
  // triggering re-renders or creating stale closures.
  const bufferRef      = useRef("");
  const lastKeyTimeRef = useRef(0);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement>) => {
      if (!enabled) return;
      const now = Date.now();

      if (e.key === "Enter") {
        const gap = lastKeyTimeRef.current > 0 ? now - lastKeyTimeRef.current : Infinity;

        if (bufferRef.current.length >= MIN_LENGTH && gap <= ENTER_INTERVAL_MS) {
          // Rapid sequence ending with Enter → barcode scan detected
          e.preventDefault();
          e.stopPropagation();

          const barcode = bufferRef.current;
          bufferRef.current      = "";
          lastKeyTimeRef.current = 0;

          setScannedBarcode(barcode);
          onScanRef.current?.(barcode);
        } else {
          // Normal Enter (human typing) — do not interfere
          bufferRef.current      = "";
          lastKeyTimeRef.current = 0;
        }
        return;
      }

      // Only track printable single characters (ignore Shift, Ctrl, F-keys, etc.)
      if (e.key.length !== 1) return;

      const gap = lastKeyTimeRef.current > 0 ? now - lastKeyTimeRef.current : Infinity;

      if (gap <= CHAR_INTERVAL_MS) {
        // Fast enough → scanner mode, extend buffer
        bufferRef.current += e.key;
      } else {
        // Too slow for a scanner → human keystroke, start fresh
        bufferRef.current = e.key;
      }

      lastKeyTimeRef.current = now;
    },
    [enabled],
  );

  const clearScan = useCallback(() => setScannedBarcode(null), []);

  return { onKeyDown: handleKeyDown, scannedBarcode, clearScan };
}
