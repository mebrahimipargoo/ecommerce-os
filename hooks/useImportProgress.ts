"use client";

/**
 * useImportProgress — real-time dual progress bars for the Import pipeline.
 *
 * Listens to two progress signals for a single upload:
 *
 *   uploadPct   (0–100) — bytes uploaded to Supabase Storage (chunks API).
 *   processPct  (0–100) — CSV rows imported on the server (process API).
 *
 * Dual-table Realtime strategy:
 *   PRIMARY   → `file_processing_status` (slim dedicated row, tiny Realtime payloads).
 *               Updated at every chunk and every 1 000 rows during CSV processing.
 *   SECONDARY → `raw_report_uploads` metadata JSONB (fallback for legacy progress data).
 *
 *   When both subscriptions are live, `file_processing_status` wins for pct values
 *   because it carries the freshest granular counters. `raw_report_uploads` is still
 *   observed for final `status` transitions ("complete", "failed").
 *
 *   Polling falls back automatically if Realtime is unavailable for either table.
 *
 * Prerequisites (Supabase dashboard):
 *   Database → Replication → publication supabase_realtime →
 *   add tables `raw_report_uploads` AND `file_processing_status`.
 *   (Migration 20260419_file_processing_status.sql does this automatically.)
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { supabase } from "../src/lib/supabase";
import { parseRawReportMetadata } from "../lib/raw-report-upload-metadata";

// ── Types ─────────────────────────────────────────────────────────────────────

export type ImportProgressStatus =
  | "idle"
  | "pending"
  | "uploading"
  | "processing"
  | "complete"
  | "failed";

export type ImportProgressState = {
  /** 0–100: bytes uploaded to Supabase Storage. */
  uploadPct: number;
  /** 0–100: rows processed on the server. */
  processPct: number;
  /** Current status from `raw_report_uploads.status`. */
  status: ImportProgressStatus;
  /** Error message if status === "failed". */
  error: string | null;
  /** Total rows imported (available after processing). */
  rowCount: number | null;
};

const IDLE_STATE: ImportProgressState = {
  uploadPct:  0,
  processPct: 0,
  status:     "idle",
  error:      null,
  rowCount:   null,
};

/** Fallback polling interval in milliseconds (used if Realtime is unavailable). */
const POLL_MS = 3_000;

// ── Helper ────────────────────────────────────────────────────────────────────

function rowToState(row: Record<string, unknown>): ImportProgressState {
  const parsed = parseRawReportMetadata(row.metadata);
  const rawStatus = String(row.status ?? "pending").toLowerCase();
  const status = [
    "idle", "pending", "uploading", "processing", "complete", "failed",
  ].includes(rawStatus)
    ? (rawStatus as ImportProgressStatus)
    : "pending";

  return {
    uploadPct:  parsed.uploadProgress,
    processPct: parsed.processProgress,
    status,
    error:      parsed.errorMessage,
    rowCount:   parsed.rowCount,
  };
}

// ── Hook ──────────────────────────────────────────────────────────────────────

/**
 * Maps a `file_processing_status` row into ImportProgressState.
 * Takes priority over raw_report_uploads for pct values when available.
 */
function fpsRowToState(row: Record<string, unknown>, fallback: ImportProgressState): ImportProgressState {
  const rawStatus = String(row.status ?? "pending").toLowerCase();
  const status = [
    "idle", "pending", "uploading", "processing", "complete", "failed",
  ].includes(rawStatus)
    ? (rawStatus as ImportProgressStatus)
    : fallback.status;

  return {
    uploadPct:  Number(row.upload_pct  ?? fallback.uploadPct),
    processPct: Number(row.process_pct ?? fallback.processPct),
    status,
    error:      typeof row.error_message === "string" && row.error_message ? row.error_message : fallback.error,
    rowCount:   row.processed_rows != null ? Number(row.processed_rows) : fallback.rowCount,
  };
}

/**
 * @param uploadId  UUID of the `raw_report_uploads` row to track.
 *                  Pass `null` to reset to the idle state.
 */
export function useImportProgress(uploadId: string | null): ImportProgressState {
  const [state, setState] = useState<ImportProgressState>(IDLE_STATE);

  const uploadIdRef = useRef(uploadId);
  uploadIdRef.current = uploadId;

  // Two independent channel refs — one per table.
  const mainChannelRef = useRef<ReturnType<typeof supabase.channel> | null>(null);
  const fpsChannelRef  = useRef<ReturnType<typeof supabase.channel> | null>(null);
  const pollTimerRef   = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchOnce = useCallback(async (id: string) => {
    const { data, error } = await supabase
      .from("raw_report_uploads")
      .select("status, metadata")
      .eq("id", id)
      .maybeSingle();
    if (error || !data) return;
    setState(rowToState(data as Record<string, unknown>));
  }, []);

  const stopPoll = useCallback(() => {
    if (pollTimerRef.current) {
      clearInterval(pollTimerRef.current);
      pollTimerRef.current = null;
    }
  }, []);

  const startPoll = useCallback(
    (id: string) => {
      stopPoll();
      pollTimerRef.current = setInterval(() => {
        if (uploadIdRef.current === id) void fetchOnce(id);
      }, POLL_MS);
    },
    [fetchOnce, stopPoll],
  );

  const removeChannels = useCallback(async () => {
    const tasks: Promise<unknown>[] = [];
    if (mainChannelRef.current) {
      tasks.push(supabase.removeChannel(mainChannelRef.current));
      mainChannelRef.current = null;
    }
    if (fpsChannelRef.current) {
      tasks.push(supabase.removeChannel(fpsChannelRef.current));
      fpsChannelRef.current = null;
    }
    await Promise.all(tasks);
  }, []);

  useEffect(() => {
    if (!uploadId) {
      setState(IDLE_STATE);
      void removeChannels();
      stopPoll();
      return;
    }

    // 1. Immediate snapshot so the UI never starts blank.
    void fetchOnce(uploadId);

    // 2a. Primary Realtime: file_processing_status (slim, tiny payloads).
    //     Provides the freshest upload_pct / process_pct values.
    const fpsChannel = supabase
      .channel(`fps-progress:${uploadId}`)
      .on(
        "postgres_changes",
        {
          event:  "UPDATE",
          schema: "public",
          table:  "file_processing_status",
          filter: `upload_id=eq.${uploadId}`,
        },
        (payload) => {
          const row = payload.new as Record<string, unknown>;
          setState((prev) => fpsRowToState(row, prev));
        },
      )
      .subscribe((subStatus) => {
        if (subStatus === "CHANNEL_ERROR" || subStatus === "TIMED_OUT") {
          // fps table not in publication yet — fall back to polling the main table.
          startPoll(uploadId);
        }
      });
    fpsChannelRef.current = fpsChannel;

    // 2b. Secondary Realtime: raw_report_uploads (status transitions, legacy metadata).
    //     Stays live in parallel; used mainly for "complete" / "failed" state changes.
    const mainChannel = supabase
      .channel(`import-progress:${uploadId}`)
      .on(
        "postgres_changes",
        {
          event:  "UPDATE",
          schema: "public",
          table:  "raw_report_uploads",
          filter: `id=eq.${uploadId}`,
        },
        (payload) => {
          const row = payload.new as Record<string, unknown>;
          // Only overwrite if fps didn't already push a more-granular update.
          setState((prev) => {
            const next = rowToState(row);
            // Prefer fps pct values when the fps channel is alive and has real data.
            return {
              ...next,
              uploadPct:  prev.uploadPct  > next.uploadPct  ? prev.uploadPct  : next.uploadPct,
              processPct: prev.processPct > next.processPct ? prev.processPct : next.processPct,
            };
          });
        },
      )
      .subscribe((subStatus) => {
        if (subStatus === "SUBSCRIBED") {
          // At least one table is live — cancel polling.
          stopPoll();
        } else if (subStatus === "CHANNEL_ERROR" || subStatus === "TIMED_OUT") {
          startPoll(uploadId);
        }
      });
    mainChannelRef.current = mainChannel;

    return () => {
      void removeChannels();
      stopPoll();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [uploadId]);

  return state;
}
