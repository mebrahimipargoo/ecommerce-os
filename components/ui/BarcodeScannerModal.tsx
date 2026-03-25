"use client";

import React, { useEffect, useRef } from "react";
import { X, ScanLine } from "lucide-react";

interface BarcodeScannerModalProps {
  onDetected: (code: string) => void;
  onClose: () => void;
  title?: string;
}

function playBeep() {
  try {
    const ctx = new AudioContext();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.type = "sine";
    osc.frequency.value = 880;
    gain.gain.setValueAtTime(0.3, ctx.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.35);
    osc.start(ctx.currentTime);
    osc.stop(ctx.currentTime + 0.35);
  } catch {
    // AudioContext unavailable — silent fallback
  }
}

export function BarcodeScannerModal({ onDetected, onClose, title = "Scan Barcode" }: BarcodeScannerModalProps) {
  const scannerRef = useRef<unknown>(null);
  const containerId = "html5-qrcode-scanner-container";
  const detectedRef = useRef(false);

  useEffect(() => {
    let cancelled = false;

    import("html5-qrcode").then(({ Html5QrcodeScanner }) => {
      if (cancelled) return;

      const scanner = new Html5QrcodeScanner(
        containerId,
        {
          fps: 10,
          qrbox: { width: 260, height: 180 },
          rememberLastUsedCamera: true,
          supportedScanTypes: [],
        },
        /* verbose= */ false,
      );

      scanner.render(
        (decodedText: string) => {
          if (detectedRef.current) return;
          detectedRef.current = true;
          playBeep();
          scanner.clear().catch(() => {}).finally(() => {
            onDetected(decodedText);
            onClose();
          });
        },
        () => {
          // scan failure — keep scanning
        },
      );

      scannerRef.current = scanner;
    });

    return () => {
      cancelled = true;
      if (scannerRef.current) {
        (scannerRef.current as { clear: () => Promise<void> }).clear().catch(() => {});
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div className="fixed inset-0 z-[500] flex items-center justify-center bg-black/70 p-4 backdrop-blur-sm">
      <div className="w-full max-w-md overflow-hidden rounded-3xl border border-slate-200 bg-white shadow-2xl dark:border-slate-700 dark:bg-slate-950">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-slate-200 px-6 py-4 dark:border-slate-700">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-sky-100 dark:bg-sky-950/50">
              <ScanLine className="h-5 w-5 text-sky-600 dark:text-sky-400" />
            </div>
            <div>
              <p className="font-bold text-foreground">{title}</p>
              <p className="text-xs text-muted-foreground">Point camera at a barcode or QR code</p>
            </div>
          </div>
          <button
            onClick={onClose}
            className="rounded-full p-2 text-slate-400 transition hover:bg-slate-100 hover:text-slate-600 dark:hover:bg-slate-800"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Scanner container — html5-qrcode renders its UI here */}
        <div className="p-4">
          <div id={containerId} className="overflow-hidden rounded-2xl" />
        </div>

        <div className="border-t border-slate-200 px-6 py-3 dark:border-slate-700">
          <p className="text-center text-xs text-muted-foreground">
            Detection is automatic — no button needed. A beep plays on success.
          </p>
        </div>
      </div>
    </div>
  );
}
