"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { CheckCircle2, Package2, ScanLine, XCircle, AlertTriangle, RotateCcw } from "lucide-react";
import { supabase } from "../../src/lib/supabase";

// ─── Types ────────────────────────────────────────────────────────────────────

type PackageRow = {
  id: string;
  organization_id: string;
  upload_id: string | null;
  order_id: string;
  sku: string;
  tracking_number: string | null;
  requested_quantity: number | null;
  shipped_quantity: number | null;
  disposed_quantity: number | null;
  cancelled_quantity: number | null;
  order_status: string | null;
  disposition: string | null;
  order_date: string | null;
  created_at: string;
  updated_at: string;
  actual_scanned_count: number;
};

type ScanPhase = "idle" | "found" | "complete";

// ─── Helpers ──────────────────────────────────────────────────────────────────

function haptic(pattern: number | number[]) {
  if (typeof navigator !== "undefined" && "vibrate" in navigator) {
    navigator.vibrate(pattern);
  }
}

const HAPTIC_OK = 50;
const HAPTIC_ERR = [100, 50, 100];

function rowColorClass(row: PackageRow): string {
  const scanned = row.actual_scanned_count ?? 0;
  const expected = row.shipped_quantity ?? 0;
  if (scanned === 0) return "bg-card border-border";
  if (scanned > expected) return "bg-red-950/60 border-red-500";
  if (scanned >= expected) return "bg-emerald-950/60 border-emerald-500";
  return "bg-amber-950/60 border-amber-500";
}

function countColorClass(row: PackageRow): string {
  const scanned = row.actual_scanned_count ?? 0;
  const expected = row.shipped_quantity ?? 0;
  if (scanned === 0) return "text-muted-foreground";
  if (scanned > expected) return "text-red-400 font-bold";
  if (scanned >= expected) return "text-emerald-400 font-bold";
  return "text-amber-400 font-bold";
}

// ─── Component ────────────────────────────────────────────────────────────────

export default function ScannerPage() {
  const [phase, setPhase] = useState<ScanPhase>("idle");

  const [trackingValue, setTrackingValue] = useState("");
  const [skuValue, setSkuValue] = useState("");

  const [scannedTracking, setScannedTracking] = useState<string | null>(null);
  const [rows, setRows] = useState<PackageRow[]>([]);

  const [isLookingUp, setIsLookingUp] = useState(false);
  const [trackingError, setTrackingError] = useState<string | null>(null);
  const [skuError, setSkuError] = useState<string | null>(null);

  const trackingRef = useRef<HTMLInputElement>(null);
  const skuRef = useRef<HTMLInputElement>(null);

  // Auto-focus tracking input on mount
  useEffect(() => {
    trackingRef.current?.focus();
  }, []);

  // Auto-focus SKU input after package is found
  useEffect(() => {
    if (phase === "found") {
      const t = setTimeout(() => skuRef.current?.focus(), 100);
      return () => clearTimeout(t);
    }
  }, [phase]);

  // ── Step 1: Look up tracking number ────────────────────────────────────────
  const handleTrackingSubmit = useCallback(
    async (e: React.FormEvent) => {
      e.preventDefault();
      const tracking = trackingValue.trim();
      if (!tracking) return;

      setIsLookingUp(true);
      setTrackingError(null);

      try {
        const { data, error } = await supabase
          .from("expected_packages")
          .select("*")
          .eq("tracking_number", tracking);

        if (error) throw error;

        if (!data || data.length === 0) {
          setTrackingError(`Tracking number "${tracking}" not found in expected packages.`);
          haptic(HAPTIC_ERR);
          return;
        }

        setRows(data as PackageRow[]);
        setScannedTracking(tracking);
        const allDone = (data as PackageRow[]).every(
          (r) => (r.actual_scanned_count ?? 0) >= (r.shipped_quantity ?? 0),
        );
        setPhase(allDone ? "complete" : "found");
        haptic(HAPTIC_OK);
      } catch (err: unknown) {
        setTrackingError(
          err instanceof Error ? err.message : "Unexpected error. Please try again.",
        );
        haptic(HAPTIC_ERR);
      } finally {
        setIsLookingUp(false);
      }
    },
    [trackingValue],
  );

  // ── Step 2: Log a scanned SKU ───────────────────────────────────────────────
  const handleSkuSubmit = useCallback(
    async (e: React.FormEvent) => {
      e.preventDefault();
      const sku = skuValue.trim();
      if (!sku) return;

      setSkuError(null);

      const rowIndex = rows.findIndex((r) => r.sku === sku);
      if (rowIndex === -1) {
        setSkuError(`SKU "${sku}" is not in this package's expected list.`);
        haptic(HAPTIC_ERR);
        setSkuValue("");
        setTimeout(() => skuRef.current?.focus(), 50);
        return;
      }

      const target = rows[rowIndex];
      const newCount = (target.actual_scanned_count ?? 0) + 1;
      const expected = target.shipped_quantity ?? 0;

      // Optimistic UI
      setRows((prev) => {
        const next = [...prev];
        next[rowIndex] = { ...next[rowIndex], actual_scanned_count: newCount };
        return next;
      });
      setSkuValue("");
      haptic(newCount > expected ? HAPTIC_ERR : HAPTIC_OK);

      // Check completion
      const updatedRows = rows.map((r, i) =>
        i === rowIndex ? { ...r, actual_scanned_count: newCount } : r,
      );
      if (updatedRows.every((r) => (r.actual_scanned_count ?? 0) >= (r.shipped_quantity ?? 0))) {
        setPhase("complete");
      }

      // Persist to Supabase
      const { error } = await supabase
        .from("expected_packages")
        .update({ actual_scanned_count: newCount })
        .eq("id", target.id);

      if (error) {
        // Roll back on failure
        setRows((prev) => {
          const rolled = [...prev];
          rolled[rowIndex] = { ...rolled[rowIndex], actual_scanned_count: target.actual_scanned_count };
          return rolled;
        });
        setSkuError(`Failed to save: ${error.message}`);
        haptic(HAPTIC_ERR);
        if (phase === "complete") setPhase("found");
      }

      setTimeout(() => skuRef.current?.focus(), 50);
    },
    [skuValue, rows, phase],
  );

  // ── Reset scanner for next package ─────────────────────────────────────────
  const resetScanner = useCallback(() => {
    setPhase("idle");
    setTrackingValue("");
    setSkuValue("");
    setScannedTracking(null);
    setRows([]);
    setTrackingError(null);
    setSkuError(null);
    setTimeout(() => trackingRef.current?.focus(), 50);
  }, []);

  // ── Derived stats ───────────────────────────────────────────────────────────
  const completeCount = rows.filter(
    (r) => (r.actual_scanned_count ?? 0) >= (r.shipped_quantity ?? 0),
  ).length;

  // ─── Render ────────────────────────────────────────────────────────────────

  return (
    <div className="min-h-screen bg-background text-foreground flex flex-col">

      {/* ── Sticky Header ─────────────────────────────────────────────────── */}
      <header className="sticky top-0 z-20 bg-background/95 backdrop-blur-sm border-b border-border px-4 py-3 flex items-center gap-3">
        <ScanLine className="w-5 h-5 text-sky-400 shrink-0" />
        <div className="flex-1 min-w-0">
          <h1 className="text-sm font-semibold leading-tight">Warehouse Scanner</h1>
          {scannedTracking && (
            <p className="text-xs text-muted-foreground font-mono truncate">{scannedTracking}</p>
          )}
        </div>
        {phase !== "idle" && (
          <button
            onClick={resetScanner}
            className="shrink-0 flex items-center gap-1.5 text-xs px-3 h-9 rounded-lg bg-slate-700 hover:bg-slate-600 active:bg-slate-500 text-white transition-colors"
          >
            <RotateCcw className="w-3.5 h-3.5" />
            Reset
          </button>
        )}
      </header>

      <main className="flex-1 flex flex-col gap-4 p-4 pb-10 max-w-lg mx-auto w-full">

        {/* ── Phase: idle / error — Show tracking input ─────────────────── */}
        {phase === "idle" && (
          <section className="flex flex-col gap-3 mt-2">
            <StepBadge n={1} label="Scan Package Tracking Number" />

            <form onSubmit={handleTrackingSubmit} className="flex flex-col gap-3">
              <input
                ref={trackingRef}
                type="text"
                value={trackingValue}
                onChange={(e) => setTrackingValue(e.target.value)}
                placeholder="Scan or type tracking number…"
                className="w-full min-h-[52px] px-4 rounded-xl border-2 border-border bg-card text-foreground text-base placeholder:text-muted-foreground focus:outline-none focus:border-sky-500 transition-colors"
                autoComplete="off"
                autoCorrect="off"
                spellCheck={false}
                disabled={isLookingUp}
              />
              <button
                type="submit"
                disabled={isLookingUp || !trackingValue.trim()}
                className="min-h-[52px] rounded-xl bg-sky-600 hover:bg-sky-500 active:bg-sky-700 disabled:opacity-40 disabled:cursor-not-allowed text-white font-semibold text-base transition-colors"
              >
                {isLookingUp ? "Searching…" : "Look Up Package"}
              </button>
            </form>

            {trackingError && <ErrorBanner message={trackingError} />}
          </section>
        )}

        {/* ── Phase: found / complete — Show items list ─────────────────── */}
        {(phase === "found" || phase === "complete") && (
          <>
            {/* Status banner */}
            <div
              className={`flex items-center gap-3 px-4 py-3 rounded-xl border ${
                phase === "complete"
                  ? "bg-emerald-950/60 border-emerald-500 text-emerald-300"
                  : "bg-sky-950/60 border-sky-500 text-sky-300"
              }`}
            >
              {phase === "complete" ? (
                <CheckCircle2 className="w-6 h-6 shrink-0" />
              ) : (
                <Package2 className="w-6 h-6 shrink-0" />
              )}
              <div>
                <p className="font-semibold text-sm leading-tight">
                  {phase === "complete" ? "Package COMPLETE ✓" : "Package FOUND"}
                </p>
                <p className="text-xs opacity-75 mt-0.5">
                  {completeCount} of {rows.length} SKU{rows.length !== 1 ? "s" : ""} complete
                </p>
              </div>
            </div>

            {/* SKU rows */}
            <section className="flex flex-col gap-2">
              {rows.map((row) => {
                const scanned = row.actual_scanned_count ?? 0;
                const expected = row.shipped_quantity ?? 0;
                return (
                  <div
                    key={row.id}
                    className={`flex items-center gap-3 px-4 py-3 rounded-xl border transition-colors ${rowColorClass(row)}`}
                  >
                    {/* SKU + Order */}
                    <div className="flex-1 min-w-0">
                      <p className="font-mono text-sm font-semibold truncate">{row.sku}</p>
                      <p className="text-xs text-muted-foreground truncate">{row.order_id}</p>
                    </div>

                    {/* Count */}
                    <div className="text-right shrink-0">
                      <p className={`text-xl leading-none tabular-nums ${countColorClass(row)}`}>
                        {scanned}
                        <span className="text-sm font-normal text-muted-foreground">
                          /{expected}
                        </span>
                      </p>
                      <p className="text-[10px] text-muted-foreground mt-0.5">scanned / exp.</p>
                    </div>

                    {/* Status icon */}
                    <div className="shrink-0 w-5">
                      {scanned > expected && (
                        <AlertTriangle className="w-5 h-5 text-red-400" />
                      )}
                      {scanned === expected && expected > 0 && (
                        <CheckCircle2 className="w-5 h-5 text-emerald-400" />
                      )}
                    </div>
                  </div>
                );
              })}
            </section>

            {/* SKU error */}
            {skuError && <ErrorBanner message={skuError} />}

            {/* Step 2: SKU input (only when not complete) */}
            {phase === "found" && (
              <section className="flex flex-col gap-3">
                <StepBadge n={2} label="Scan Item SKU" />
                <form onSubmit={handleSkuScan} className="flex gap-2">
                  <input
                    ref={skuRef}
                    type="text"
                    value={skuValue}
                    onChange={(e) => setSkuValue(e.target.value)}
                    placeholder="Scan SKU barcode…"
                    className="flex-1 min-h-[52px] px-4 rounded-xl border-2 border-border bg-card text-foreground text-base placeholder:text-muted-foreground focus:outline-none focus:border-emerald-500 transition-colors"
                    autoComplete="off"
                    autoCorrect="off"
                    spellCheck={false}
                  />
                  <button
                    type="submit"
                    disabled={!skuValue.trim()}
                    className="min-h-[52px] px-5 rounded-xl bg-emerald-600 hover:bg-emerald-500 active:bg-emerald-700 disabled:opacity-40 disabled:cursor-not-allowed text-white font-semibold transition-colors"
                  >
                    Log
                  </button>
                </form>
              </section>
            )}

            {/* Complete: action buttons */}
            {phase === "complete" && (
              <button
                onClick={resetScanner}
                className="min-h-[52px] rounded-xl bg-emerald-600 hover:bg-emerald-500 active:bg-emerald-700 text-white font-semibold text-base w-full transition-colors"
              >
                Scan Next Package →
              </button>
            )}
          </>
        )}
      </main>
    </div>
  );
}

// ─── Sub-components ────────────────────────────────────────────────────────────

function StepBadge({ n, label }: { n: number; label: string }) {
  return (
    <div className="flex items-center gap-2">
      <span className="w-6 h-6 rounded-full bg-sky-600 text-white text-xs font-bold flex items-center justify-center shrink-0">
        {n}
      </span>
      <h2 className="text-sm font-semibold">{label}</h2>
    </div>
  );
}

function ErrorBanner({ message }: { message: string }) {
  return (
    <div className="flex items-start gap-3 px-4 py-3 rounded-xl bg-red-950/50 border border-red-500 text-red-300 text-sm">
      <XCircle className="w-5 h-5 shrink-0 mt-0.5" />
      <span>{message}</span>
    </div>
  );
}
