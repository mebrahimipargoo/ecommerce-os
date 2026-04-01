"use client";

/**
 * SmartCameraUpload — reusable evidence-photo component
 *
 * Behaviour by environment:
 *   Mobile / PDA  → single <input capture="environment"> that directly opens
 *                   the rear camera with no file-picker intermediate step.
 *   Desktop       → two-option CTA:
 *                     • "Use Webcam"   – live react-webcam feed with flip +
 *                                        capture, gracefully falls back on
 *                                        permission denial.
 *                     • "Browse Files" – standard multi-file picker.
 *
 * Usage:
 *   <SmartCameraUpload
 *     label="Shipping Label & LPN Barcode"
 *     hint="Full label showing LPN and tracking number."
 *     required
 *     icon={Barcode}
 *     iconColor="text-sky-600 dark:text-sky-400"
 *     accentClass="border-sky-200 dark:border-sky-800/50"
 *     files={photos}
 *     onChange={setPhotos}
 *   />
 */

import React, { useCallback, useEffect, useRef, useState } from "react";
import Webcam from "react-webcam";
import {
  Camera,
  CheckCircle2,
  FlipHorizontal,
  Upload,
  Video,
  VideoOff,
  X,
} from "lucide-react";

// ─── Types ────────────────────────────────────────────────────────────────────

export interface SmartCameraUploadProps {
  label: string;
  hint: string;
  required?: boolean;
  /** Array of already-captured File objects */
  files: File[];
  onChange: (files: File[]) => void;
  /** Max photos allowed in this zone (default 5) */
  maxPhotos?: number;
  /** Tailwind border+accent classes for the unfinished state */
  accentClass?: string;
  /** Tailwind text colour for the icon in the header */
  iconColor?: string;
  /** Lucide icon component for the header */
  icon?: React.ElementType;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

async function base64ToFile(dataUrl: string, name: string): Promise<File> {
  const res = await fetch(dataUrl);
  const blob = await res.blob();
  return new File([blob], name, { type: blob.type || "image/jpeg" });
}

// ─── Component ────────────────────────────────────────────────────────────────

export function SmartCameraUpload({
  label,
  hint,
  required = false,
  files,
  onChange,
  maxPhotos = 5,
  accentClass = "border-slate-300 dark:border-slate-700",
  iconColor = "text-slate-500 dark:text-slate-400",
  icon: Icon = Camera,
}: SmartCameraUploadProps) {
  // Default true → safe SSR render; updated after hydration
  const [isMobile, setIsMobile] = useState(true);
  const [webcamActive, setWebcamActive] = useState(false);
  const [webcamError, setWebcamError] = useState(false);
  const [facingMode, setFacingMode] = useState<"environment" | "user">("environment");

  // Two separate refs so capture attribute can differ
  const mobileInputRef = useRef<HTMLInputElement>(null);
  const desktopInputRef = useRef<HTMLInputElement>(null);
  const webcamRef = useRef<Webcam>(null);

  // Detect mobile after mount (avoids SSR mismatch)
  useEffect(() => {
    const mobile =
      navigator.maxTouchPoints > 1 ||
      /Mobi|Android|iPhone|iPad|PDA/i.test(navigator.userAgent);
    setIsMobile(mobile);
  }, []);

  const isComplete = files.length > 0;
  const canAddMore = files.length < maxPhotos;

  // ── File handlers ──

  // Reset the input's value after every selection so the browser fires onChange
  // even if the operator picks the exact same file again (e.g. after deleting it).
  function handleFileChange(e: React.ChangeEvent<HTMLInputElement>) {
    const list = e.target.files;
    if (list) {
      const slots = maxPhotos - files.length;
      const next  = Array.from(list).slice(0, Math.max(0, slots));
      onChange([...files, ...next]);
    }
    e.target.value = "";
  }

  function removeFile(i: number) {
    onChange(files.filter((_, idx) => idx !== i));
  }

  // ── Webcam capture ──

  const captureFromWebcam = useCallback(async () => {
    const screenshot = webcamRef.current?.getScreenshot();
    if (!screenshot) return;
    try {
      const file = await base64ToFile(screenshot, `webcam-${Date.now()}.jpg`);
      onChange([...files, file]);
      // Auto-exit webcam after each capture so operator sees the thumbnail
      setWebcamActive(false);
    } catch {
      // Silently ignore blob conversion errors
    }
  }, [files, onChange]);

  function openWebcam() {
    setWebcamError(false);
    setWebcamActive(true);
  }

  function closeWebcam() {
    setWebcamActive(false);
    setWebcamError(false);
  }

  // ─── Render ──────────────────────────────────────────────────────────────

  return (
    <div
      className={[
        "overflow-hidden rounded-2xl border-2 bg-white transition dark:bg-slate-900",
        isComplete ? "border-emerald-400 dark:border-emerald-600/60" : accentClass,
      ].join(" ")}
    >
      {/* ── Zone header ── */}
      <div className="flex items-start gap-3 border-b border-slate-200 px-4 py-3 dark:border-slate-800">
        <div
          className={[
            "flex h-10 w-10 shrink-0 items-center justify-center rounded-xl",
            isComplete ? "bg-emerald-100 dark:bg-emerald-950/40" : "bg-slate-100 dark:bg-slate-800",
          ].join(" ")}
        >
          {isComplete ? (
            <CheckCircle2 className="h-5 w-5 text-emerald-600 dark:text-emerald-400" />
          ) : (
            <Icon className={`h-5 w-5 ${iconColor}`} />
          )}
        </div>
        <div className="flex-1">
          <div className="flex flex-wrap items-center gap-1.5">
            <p className="text-sm font-semibold text-slate-900 dark:text-slate-50">
              {label}
              {required && <span className="text-rose-500"> *</span>}
            </p>
          </div>
          <p className="mt-0.5 text-[11px] leading-snug text-slate-500 dark:text-slate-400">{hint}</p>
        </div>
        {isComplete && (
          <span className="shrink-0 text-xs font-bold text-emerald-600 dark:text-emerald-400">
            {files.length}/{maxPhotos}
          </span>
        )}
      </div>

      {/* ── Capture CTA (thumbnails render below — matches mobile “capture then see proof” flow) ── */}
      {canAddMore && (
        <>
          {/* ── MOBILE: single large camera button ── */}
          {isMobile && (
            <button
              type="button"
              onClick={() => mobileInputRef.current?.click()}
              className="flex w-full items-center gap-4 px-4 py-4 transition hover:bg-slate-50 active:bg-slate-100 dark:hover:bg-slate-800/60"
            >
              <div className="flex h-11 w-11 shrink-0 items-center justify-center rounded-2xl border border-slate-200 bg-slate-100 dark:border-slate-700 dark:bg-slate-800">
                <Camera className="h-5 w-5 text-slate-500 dark:text-slate-400" />
              </div>
              <div className="text-left">
                <p className="text-sm font-semibold text-slate-700 dark:text-slate-200">
                  {files.length === 0 ? "Tap to Capture" : "Add Another Photo"}
                </p>
                <p className="text-[11px] text-slate-400">Opens rear camera directly · no file picker</p>
              </div>
            </button>
          )}

          {/* ── DESKTOP: webcam live feed ── */}
          {!isMobile && webcamActive && (
            <div className="p-3">
              <div className="relative overflow-hidden rounded-xl bg-slate-950">
                {webcamError ? (
                  <div className="flex h-44 flex-col items-center justify-center gap-2">
                    <VideoOff className="h-8 w-8 text-slate-500" />
                    <p className="text-xs text-slate-400">Camera access denied or unavailable</p>
                    <button
                      type="button"
                      onClick={closeWebcam}
                      className="mt-1 text-xs text-sky-400 underline"
                    >
                      Use file upload instead
                    </button>
                  </div>
                ) : (
                  <Webcam
                    ref={webcamRef}
                    audio={false}
                    screenshotFormat="image/jpeg"
                    screenshotQuality={0.92}
                    videoConstraints={{ facingMode, width: 1280, height: 720 }}
                    onUserMediaError={() => setWebcamError(true)}
                    className="w-full"
                  />
                )}
                {/* Close button top-right */}
                <button
                  type="button"
                  onClick={closeWebcam}
                  className="absolute right-2 top-2 flex h-7 w-7 items-center justify-center rounded-full bg-black/60 text-white backdrop-blur-sm transition hover:bg-black/80"
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              </div>

              {/* Webcam controls */}
              <div className="mt-2 flex items-center gap-2">
                <button
                  type="button"
                  onClick={captureFromWebcam}
                  disabled={webcamError}
                  className="flex flex-1 items-center justify-center gap-2 rounded-xl bg-sky-500 py-2.5 text-sm font-bold text-white transition hover:bg-sky-400 active:scale-[0.98] disabled:opacity-50"
                >
                  <Camera className="h-4 w-4" /> Capture Photo
                </button>
                <button
                  type="button"
                  onClick={() => setFacingMode((m) => (m === "environment" ? "user" : "environment"))}
                  title="Flip camera"
                  className="flex h-10 w-10 items-center justify-center rounded-xl border border-slate-200 text-slate-500 transition hover:bg-slate-50 dark:border-slate-700 dark:hover:bg-slate-800"
                >
                  <FlipHorizontal className="h-4 w-4" />
                </button>
              </div>
            </div>
          )}

          {/* ── DESKTOP: two-option CTA (webcam inactive) ── */}
          {!isMobile && !webcamActive && (
            <div className="flex gap-2 p-3">
              <button
                type="button"
                onClick={openWebcam}
                className="flex flex-1 items-center justify-center gap-2 rounded-xl border border-slate-200 py-3 text-sm font-medium text-slate-600 transition hover:bg-slate-50 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800/60"
              >
                <Video className="h-4 w-4" />
                {files.length === 0 ? "Use Webcam" : "Add via Webcam"}
              </button>
              <button
                type="button"
                onClick={() => desktopInputRef.current?.click()}
                className="flex flex-1 items-center justify-center gap-2 rounded-xl border border-slate-200 py-3 text-sm font-medium text-slate-600 transition hover:bg-slate-50 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800/60"
              >
                <Upload className="h-4 w-4" />
                {files.length === 0 ? "Browse Files" : "Add More Files"}
              </button>
            </div>
          )}
        </>
      )}

      {/* ── Thumbnail grid (below capture controls) ── */}
      {files.length > 0 && (
        <div className="grid grid-cols-4 gap-2 px-4 pb-3 pt-2">
          {files.map((file, i) => {
            const url = URL.createObjectURL(file);
            return (
              <div
                key={i}
                className="relative aspect-square overflow-hidden rounded-xl border border-slate-200 bg-slate-100 dark:border-slate-700 dark:bg-slate-800"
              >
                {/* eslint-disable-next-line @next/next/no-img-element */}
                <img src={url} alt={`Evidence ${i + 1}`} className="h-full w-full object-contain" />
                {/* Always-visible delete button — visible on both touch and pointer devices */}
                <button
                  type="button"
                  onClick={(e) => { e.stopPropagation(); removeFile(i); }}
                  className="absolute right-1 top-1 flex h-6 w-6 items-center justify-center rounded-full bg-black/70 text-white shadow-md transition hover:bg-rose-600 active:scale-95"
                  aria-label={`Remove photo ${i + 1}`}
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              </div>
            );
          })}
        </div>
      )}

      {/* ── Hidden file inputs ── */}

      {/* Mobile: capture="environment" opens rear camera directly */}
      <input
        ref={mobileInputRef}
        type="file"
        accept="image/*"
        capture="environment"
        className="hidden"
        onChange={handleFileChange}
      />

      {/* Desktop: standard multi-file picker */}
      <input
        ref={desktopInputRef}
        type="file"
        accept="image/*"
        capture="environment"
        multiple
        className="hidden"
        onChange={handleFileChange}
      />
    </div>
  );
}
