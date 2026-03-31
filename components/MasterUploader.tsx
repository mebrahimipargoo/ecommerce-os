"use client";

import React, { useCallback, useRef, useState } from "react";
import { Camera, ImagePlus, Loader2, Trash2, Upload } from "lucide-react";
import { uploadToIncidentPhotos } from "../lib/supabase/storage";

export type MasterUploaderProps = {
  /** Public URLs already uploaded (controlled). */
  value: string[];
  onChange: (urls: string[]) => void;
  organizationId: string;
  /** Max images (default 24). */
  maxFiles?: number;
  disabled?: boolean;
  /** Short label above the drop zone. */
  label?: string;
  hint?: string;
  className?: string;
};

/**
 * Unified evidence uploader: drag-and-drop, file picker, and camera capture.
 * Uploads to the `incident-photos` bucket and returns public URLs.
 */
export function MasterUploader({
  value,
  onChange,
  organizationId,
  maxFiles = 24,
  disabled = false,
  label = "Photos",
  hint = "Drag images here, choose files, or use the camera.",
  className = "",
}: MasterUploaderProps) {
  const [dragOver, setDragOver] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState("");
  const fileRef = useRef<HTMLInputElement>(null);
  const cameraRef = useRef<HTMLInputElement>(null);

  const canAdd = value.length < maxFiles && !disabled;

  const uploadFiles = useCallback(
    async (files: FileList | File[]) => {
      const list = Array.from(files).filter((f) => f.type.startsWith("image/"));
      if (list.length === 0) {
        setError("Choose image files only.");
        return;
      }
      const room = maxFiles - value.length;
      const batch = list.slice(0, Math.max(0, room));
      if (batch.length === 0) return;
      setError("");
      setUploading(true);
      const next: string[] = [...value];
      try {
        for (const file of batch) {
          const url = await uploadToIncidentPhotos(file, "incident", organizationId);
          next.push(url);
        }
        onChange(next);
      } catch (e) {
        setError(e instanceof Error ? e.message : "Upload failed.");
      } finally {
        setUploading(false);
      }
    },
    [maxFiles, onChange, organizationId, value],
  );

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      setDragOver(false);
      if (!canAdd || uploading) return;
      void uploadFiles(e.dataTransfer.files);
    },
    [canAdd, uploadFiles, uploading],
  );

  const removeAt = (idx: number) => {
    onChange(value.filter((_, i) => i !== idx));
  };

  return (
    <div className={`space-y-3 ${className}`}>
      <div>
        <p className="text-sm font-semibold text-foreground">{label}</p>
        {hint ? <p className="text-xs text-muted-foreground">{hint}</p> : null}
      </div>

      <div
        role="button"
        tabIndex={0}
        onDragEnter={(e) => { e.preventDefault(); setDragOver(true); }}
        onDragLeave={() => setDragOver(false)}
        onDragOver={(e) => { e.preventDefault(); e.dataTransfer.dropEffect = "copy"; }}
        onDrop={onDrop}
        className={[
          "rounded-2xl border-2 border-dashed p-4 transition",
          dragOver ? "border-sky-400 bg-sky-50/80 dark:bg-sky-950/30" : "border-border bg-muted/30",
          disabled ? "pointer-events-none opacity-60" : "",
        ].join(" ")}
      >
        <div className="flex flex-col gap-3 sm:flex-row sm:flex-wrap sm:items-center sm:justify-center">
          <button
            type="button"
            disabled={!canAdd || uploading}
            onClick={() => fileRef.current?.click()}
            className="inline-flex items-center justify-center gap-2 rounded-xl border border-border bg-background px-4 py-2.5 text-sm font-semibold hover:bg-accent"
          >
            {uploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Upload className="h-4 w-4" />}
            Choose files
          </button>
          <button
            type="button"
            disabled={!canAdd || uploading}
            onClick={() => cameraRef.current?.click()}
            className="inline-flex items-center justify-center gap-2 rounded-xl border border-border bg-background px-4 py-2.5 text-sm font-semibold hover:bg-accent"
          >
            <Camera className="h-4 w-4" />
            Camera
          </button>
          <p className="text-center text-[11px] text-muted-foreground sm:ml-2">
            {value.length}/{maxFiles} saved
          </p>
        </div>
        <input
          ref={fileRef}
          type="file"
          accept="image/*"
          multiple
          className="hidden"
          disabled={!canAdd || uploading}
          onChange={(e) => {
            const f = e.target.files;
            e.target.value = "";
            if (f?.length) void uploadFiles(f);
          }}
        />
        <input
          ref={cameraRef}
          type="file"
          accept="image/*"
          capture="environment"
          className="hidden"
          disabled={!canAdd || uploading}
          onChange={(e) => {
            const f = e.target.files;
            e.target.value = "";
            if (f?.length) void uploadFiles(f);
          }}
        />
        <div className="mt-3 flex items-center justify-center gap-2 text-[11px] text-muted-foreground">
          <ImagePlus className="h-3.5 w-3.5" />
          Drop images anywhere in this area
        </div>
      </div>

      {error ? (
        <p className="rounded-lg bg-rose-50 px-3 py-2 text-xs text-rose-700 dark:bg-rose-950/40 dark:text-rose-300">
          {error}
        </p>
      ) : null}

      {value.length > 0 && (
        <ul className="grid grid-cols-2 gap-2 sm:grid-cols-3">
          {value.map((url, idx) => (
            <li key={`${url}-${idx}`} className="relative overflow-hidden rounded-xl border border-border bg-background">
              {/* eslint-disable-next-line @next/next/no-img-element */}
              <img src={url} alt="" className="h-28 w-full object-cover" />
              <button
                type="button"
                disabled={disabled || uploading}
                onClick={() => removeAt(idx)}
                className="absolute right-1 top-1 rounded-full bg-black/60 p-1 text-white hover:bg-black/80"
                aria-label="Remove photo"
              >
                <Trash2 className="h-3.5 w-3.5" />
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
