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
 *   observed for final `status` transitions ("synced", "complete", "failed").
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
  | "syncing"
  | "staged"
  | "raw_synced"
  | "synced"
  | "complete"
  | "failed";

export type ImportProgressState = {
  /** 0–100: bytes uploaded to Supabase Storage. */
  uploadPct: number;
  /** 0–100: CSV rows staged (Phase 2). */
  processPct: number;
  /** 0–100: domain rows flushed (Phase 3). */
  syncPct: number;
  /** file_processing_status.current_phase when present (upload | process | staged | sync | complete | failed). */
  currentPhase: string | null;
  /** Current status from `raw_report_uploads.status` (and fps while uploading). */
  status: ImportProgressStatus;
  /** Error message if status === "failed". */
  error: string | null;
  /** Total rows imported (available after processing). */
  rowCount: number | null;
};

const IDLE_STATE: ImportProgressState = {
  uploadPct:  0,
  processPct: 0,
  syncPct:    0,
  currentPhase: null,
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
    "idle", "pending", "uploading", "processing", "staged", "raw_synced", "synced", "complete", "failed",
  ].includes(rawStatus)
    ? (rawStatus as ImportProgressStatus)
    : "pending";

  return {
    uploadPct:  parsed.uploadProgress,
    processPct: parsed.processProgress,
    syncPct:    parsed.syncProgress,
    currentPhase: null,
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
  let status: ImportProgressStatus = fallback.status;
  if (rawStatus === "uploading") status = "uploading";
  else if (rawStatus === "processing") status = "processing";
  else if (rawStatus === "syncing") status = "syncing";
  else if (rawStatus === "complete") status = "complete";
  else if (rawStatus === "failed") status = "failed";
  else if (rawStatus === "pending") status = "pending";

  const phase =
    typeof row.current_phase === "string" && row.current_phase.trim() !== ""
      ? row.current_phase.trim()
      : fallback.currentPhase;

  return {
    uploadPct:  Number(row.upload_pct ?? fallback.uploadPct),
    processPct: Number(row.process_pct ?? fallback.processPct),
    syncPct:    Number(row.sync_pct ?? fallback.syncPct),
    currentPhase: phase,
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
    const [rpu, fps] = await Promise.all([
      supabase.from("raw_report_uploads").select("status, metadata, report_type").eq("id", id).maybeSingle(),
      supabase.from("file_processing_status").select("*").eq("upload_id", id).maybeSingle(),
    ]);
    if (rpu.error || !rpu.data) return;
    let next = rowToState(rpu.data as Record<string, unknown>);
    if (!fps.error && fps.data) {
      next = fpsRowToState(fps.data as Record<string, unknown>, next);
    }
    // Prefer raw_report_uploads lifecycle for terminal states (synced / failed).
    const rStatus = String((rpu.data as { status?: string }).status ?? "").toLowerCase();
    if (rStatus === "staged") {
      next = { ...next, status: "staged", processPct: Math.max(next.processPct, 100) };
    } else if (rStatus === "raw_synced") {
      next = { ...next, status: "raw_synced", processPct: Math.max(next.processPct, 100), syncPct: Math.max(next.syncPct, 100) };
    } else if (rStatus === "synced" || rStatus === "complete") {
      next = { ...next, status: rStatus as ImportProgressStatus, syncPct: 100 };
    } else if (rStatus === "failed") {
      const fpsData = !fps.error && fps.data ? (fps.data as Record<string, unknown>) : null;
      const p3 = String(fpsData?.phase3_status ?? "").toLowerCase();
      const p4 = String(fpsData?.phase4_status ?? "").toLowerCase();
      const metaRaw = (rpu.data as { metadata?: unknown }).metadata;
      const rt = String((rpu.data as { report_type?: unknown }).report_type ?? "").trim();
      const m =
        metaRaw && typeof metaRaw === "object" && !Array.isArray(metaRaw)
          ? (metaRaw as Record<string, unknown>)
          : null;
      const etl = String(m?.etl_phase ?? "").toLowerCase();
      const catalogDone = String(m?.catalog_listing_import_phase ?? "").toLowerCase() === "done";
      const shipmentPipelineDone =
        rt === "REMOVAL_SHIPMENT" && p3 === "complete" && p4 === "complete" && etl === "complete";
      if (p4 === "complete" || etl === "complete" || catalogDone || shipmentPipelineDone) {
        next = {
          ...next,
          status: "complete",
          syncPct: 100,
          processPct: Math.max(next.processPct, 100),
        };
      } else {
        next = { ...next, status: "failed" };
      }
    } else if (rStatus === "processing" && next.currentPhase === "staged") {
      next = { ...next, status: "staged" };
    }
    setState(next);
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

    // New upload_id must never show the previous session's percentages or row counts.
    setState(IDLE_STATE);

    // 1. Immediate snapshot so the UI never starts blank.
    void fetchOnce(uploadId);

    // 2a. Primary Realtime: file_processing_status (slim, tiny payloads).
    //     INSERT + UPDATE — session create upserts a fresh row per upload_id.
    const fpsChannel = supabase
      .channel(`fps-progress:${uploadId}`)
      .on(
        "postgres_changes",
        {
          event:  "*",
          schema: "public",
          table:  "file_processing_status",
          filter: `upload_id=eq.${uploadId}`,
        },
        (payload) => {
          const row = payload.new as Record<string, unknown> | undefined;
          if (!row || typeof row !== "object") return;
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
    //     Stays live in parallel; used mainly for "synced" / "complete" / "failed" state changes.
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
          setState((prev) => {
            const next = rowToState(row);
            return {
              ...next,
              // Upload bytes can legitimately lag behind in metadata; do not let old process/sync % stick.
              uploadPct: Math.max(prev.uploadPct, next.uploadPct),
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
