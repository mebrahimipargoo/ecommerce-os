"use client";

/**
 * Supabase-backed evidence uploader — same interaction model as {@link SmartCameraUpload}
 * (capture controls first, thumbnails directly below). Uploads to the `media` bucket and
 * stores public URLs (no local-only File state).
 */
import React, { useCallback, useEffect, useRef, useState } from "react";
import Webcam from "react-webcam";
import {
  Camera,
  CheckCircle2,
  FlipHorizontal,
  Loader2,
  Upload,
  Video,
  VideoOff,
  X,
} from "lucide-react";
import { uploadToMedia } from "../lib/supabase/storage";
import { isUuidString } from "../lib/uuid";

export type MasterUploaderProps = {
  value: string[];
  onChange: (urls: string[]) => void;
  organizationId: string;
  maxFiles?: number;
  disabled?: boolean;
  label?: string;
  hint?: string;
  className?: string;
};

async function base64ToFile(dataUrl: string, name: string): Promise<File> {
  const res = await fetch(dataUrl);
  const blob = await res.blob();
  return new File([blob], name, { type: blob.type || "image/jpeg" });
}

export function MasterUploader({
  value,
  onChange,
  organizationId,
  maxFiles = 3,
  disabled = false,
  label = "Photos",
  hint = "Tap to capture or use desktop webcam / files — images upload automatically.",
  className = "",
}: MasterUploaderProps) {
  const [isMobile, setIsMobile] = useState(true);
  const [webcamActive, setWebcamActive] = useState(false);
  const [webcamError, setWebcamError] = useState(false);
  const [facingMode, setFacingMode] = useState<"environment" | "user">("environment");
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState("");

  const mobileInputRef = useRef<HTMLInputElement>(null);
  const desktopInputRef = useRef<HTMLInputElement>(null);
  const webcamRef = useRef<Webcam>(null);
  const valueRef = useRef(value);
  const onChangeRef = useRef(onChange);
  const uploadingRef = useRef(false);

  useEffect(() => {
    valueRef.current = value;
  }, [value]);
  useEffect(() => {
    onChangeRef.current = onChange;
  }, [onChange]);

  useEffect(() => {
    const mobile =
      navigator.maxTouchPoints > 1 || /Mobi|Android|iPhone|iPad|PDA/i.test(navigator.userAgent);
    setIsMobile(mobile);
  }, []);

  const canAdd = value.length < maxFiles && !disabled;
  const isComplete = value.length > 0;
  const accentClass = "border-slate-300 dark:border-slate-700";

  const runUpload = useCallback(
    async (files: File[]) => {
      if (disabled || uploadingRef.current) return;
      const org = organizationId?.trim() ?? "";
      if (!isUuidString(org)) {
        setError("Missing or invalid organization — cannot upload.");
        return;
      }
      const list = files.filter((f) => f.type.startsWith("image/"));
      if (list.length === 0) {
        setError("Choose image files only.");
        return;
      }
      const room = maxFiles - valueRef.current.length;
      const batch = list.slice(0, Math.max(0, room));
      if (batch.length === 0) return;
      setError("");
      uploadingRef.current = true;
      setUploading(true);
      const next = [...valueRef.current];
      try {
        for (const file of batch) {
          const url = await uploadToMedia(file, "incident", org);
          next.push(url);
        }
        valueRef.current = next;
        onChangeRef.current(next);
      } catch (e) {
        setError(e instanceof Error ? e.message : "Upload failed.");
      } finally {
        uploadingRef.current = false;
        setUploading(false);
      }
    },
    [disabled, maxFiles, organizationId],
  );

  function handleFileChange(e: React.ChangeEvent<HTMLInputElement>) {
    const list = e.target.files;
    if (list?.length) void runUpload(Array.from(list));
    e.target.value = "";
  }

  function removeAt(i: number) {
    if (disabled || uploadingRef.current) return;
    const next = value.filter((_, idx) => idx !== i);
    valueRef.current = next;
    onChangeRef.current(next);
  }

  const captureFromWebcam = useCallback(async () => {
    const screenshot = webcamRef.current?.getScreenshot();
    if (!screenshot) return;
    try {
      const file = await base64ToFile(screenshot, `webcam-${Date.now()}.jpg`);
      await runUpload([file]);
      setWebcamActive(false);
    } catch {
      /* ignore */
    }
  }, [runUpload]);

  return (
    <div className={`space-y-0 ${className}`}>
      <div
        className={[
          "overflow-hidden rounded-2xl border-2 bg-white transition dark:bg-slate-900",
          isComplete ? "border-emerald-400 dark:border-emerald-600/60" : accentClass,
        ].join(" ")}
      >
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
              <Camera className="h-5 w-5 text-slate-500 dark:text-slate-400" />
            )}
          </div>
          <div className="min-w-0 flex-1">
            <p className="text-sm font-semibold text-slate-900 dark:text-slate-50">
              {(label && label.trim()) || "Evidence"}
            </p>
            {hint ? (
              <p className="mt-0.5 text-[11px] leading-snug text-slate-500 dark:text-slate-400">{hint}</p>
            ) : null}
          </div>
          <span className="shrink-0 text-xs font-bold text-slate-500 dark:text-slate-400">
            {value.length}/{maxFiles}
          </span>
        </div>

        {canAdd && !uploading && (
          <>
            {isMobile && (
              <button
                type="button"
                disabled={!canAdd}
                onClick={() => mobileInputRef.current?.click()}
                className="flex w-full items-center gap-4 px-4 py-4 transition hover:bg-slate-50 active:bg-slate-100 disabled:opacity-50 dark:hover:bg-slate-800/60"
              >
                <div className="flex h-11 w-11 shrink-0 items-center justify-center rounded-2xl border border-slate-200 bg-slate-100 dark:border-slate-700 dark:bg-slate-800">
                  <Camera className="h-5 w-5 text-slate-500 dark:text-slate-400" />
                </div>
                <div className="text-left">
                  <p className="text-sm font-semibold text-slate-700 dark:text-slate-200">
                    {value.length === 0 ? "Tap to Capture" : "Add Another Photo"}
                  </p>
                  <p className="text-[11px] text-slate-400">Opens rear camera directly · no file picker</p>
                </div>
              </button>
            )}

            {!isMobile && webcamActive && (
              <div className="p-3">
                <div className="relative overflow-hidden rounded-xl bg-slate-950">
                  {webcamError ? (
                    <div className="flex h-44 flex-col items-center justify-center gap-2">
                      <VideoOff className="h-8 w-8 text-slate-500" />
                      <p className="text-xs text-slate-400">Camera access denied or unavailable</p>
                      <button
                        type="button"
                        onClick={() => { setWebcamActive(false); setWebcamError(false); }}
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
                  <button
                    type="button"
                    onClick={() => { setWebcamActive(false); setWebcamError(false); }}
                    className="absolute right-2 top-2 flex h-7 w-7 items-center justify-center rounded-full bg-black/60 text-white backdrop-blur-sm transition hover:bg-black/80"
                  >
                    <X className="h-3.5 w-3.5" />
                  </button>
                </div>
                <div className="mt-2 flex items-center gap-2">
                  <button
                    type="button"
                    onClick={() => void captureFromWebcam()}
                    disabled={webcamError}
                    className="flex flex-1 items-center justify-center gap-2 rounded-xl bg-sky-500 py-2.5 text-sm font-bold text-white transition hover:bg-sky-400 active:scale-[0.98] disabled:opacity-50"
                  >
                    <Camera className="h-4 w-4" /> Capture &amp; Upload
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

            {!isMobile && !webcamActive && (
              <div className="flex gap-2 p-3">
                <button
                  type="button"
                  onClick={() => { setWebcamError(false); setWebcamActive(true); }}
                  className="flex flex-1 items-center justify-center gap-2 rounded-xl border border-slate-200 py-3 text-sm font-medium text-slate-600 transition hover:bg-slate-50 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800/60"
                >
                  <Video className="h-4 w-4" />
                  {value.length === 0 ? "Use Webcam" : "Add via Webcam"}
                </button>
                <button
                  type="button"
                  onClick={() => desktopInputRef.current?.click()}
                  className="flex flex-1 items-center justify-center gap-2 rounded-xl border border-slate-200 py-3 text-sm font-medium text-slate-600 transition hover:bg-slate-50 dark:border-slate-700 dark:text-slate-300 dark:hover:bg-slate-800/60"
                >
                  <Upload className="h-4 w-4" />
                  {value.length === 0 ? "Browse Files" : "Add More Files"}
                </button>
              </div>
            )}
          </>
        )}

        {uploading && (
          <div className="flex items-center gap-2 px-4 py-4 text-sm font-medium text-slate-600 dark:text-slate-300">
            <Loader2 className="h-5 w-5 shrink-0 animate-spin text-sky-500" />
            Uploading…
          </div>
        )}

        <input
          ref={mobileInputRef}
          type="file"
          accept="image/*"
          capture="environment"
          className="hidden"
          onChange={handleFileChange}
        />
        <input
          ref={desktopInputRef}
          type="file"
          accept="image/*"
          multiple
          className="hidden"
          onChange={handleFileChange}
        />

        {value.length > 0 && (
          <div className="grid grid-cols-4 gap-2 px-4 pb-3 pt-2">
            {value.map((url, i) => (
              <div
                key={`${url}-${i}`}
                className="relative aspect-square overflow-hidden rounded-xl border border-slate-200 bg-slate-100 dark:border-slate-700 dark:bg-slate-800"
              >
                {/* eslint-disable-next-line @next/next/no-img-element */}
                <img src={url} alt="" className="h-full w-full object-contain" />
                <button
                  type="button"
                  disabled={disabled || uploading}
                  onClick={() => removeAt(i)}
                  className="absolute right-1 top-1 flex h-6 w-6 items-center justify-center rounded-full bg-black/70 text-white shadow-md transition hover:bg-rose-600 disabled:opacity-40"
                  aria-label="Remove photo"
                >
                  <X className="h-3.5 w-3.5" />
                </button>
              </div>
            ))}
          </div>
        )}
      </div>

      {error ? (
        <p className="mt-2 rounded-lg bg-rose-50 px-3 py-2 text-xs text-rose-700 dark:bg-rose-950/40 dark:text-rose-300">
          {error}
        </p>
      ) : null}
    </div>
  );
}
