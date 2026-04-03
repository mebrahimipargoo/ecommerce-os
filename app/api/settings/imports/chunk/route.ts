import { NextResponse } from "next/server";

import { updateUploadAfterChunk } from "../../../../../lib/import-upload-progress";
import { parseRawReportMetadata } from "../../../../../lib/raw-report-upload-metadata";
import { supabaseServer } from "../../../../../lib/supabase-server";
import { isUuidString } from "../../../../../lib/uuid";

export const runtime = "nodejs";

const BUCKET = "raw-reports";

function formatStorageUploadError(rawMessage: string): { message: string; status: number } {
  const m = rawMessage.toLowerCase();
  const bucketMissing =
    (m.includes("bucket") && (m.includes("not found") || m.includes("does not exist"))) ||
    m.includes("no such bucket") ||
    (m.includes("storage") && m.includes("not found")) ||
    m.includes("resource was not found");

  if (bucketMissing) {
    return {
      message: `Storage bucket "${BUCKET}" is missing or not accessible. In Supabase: Storage → create bucket "${BUCKET}" (private), then add policies so authenticated uploads can write objects under your org prefix.`,
      status: 503,
    };
  }

  return {
    message: `Could not store chunk in bucket "${BUCKET}": ${rawMessage}`,
    status: 500,
  };
}

/**
 * Chunked CSV upload: keeps each request body small so 400MB+ files never load into memory at once.
 * Client sends parts sequentially (recommended) or in parallel (progress uses max of index/bytes).
 */
export async function POST(req: Request): Promise<Response> {
  try {
    const form = await req.formData();
    const uploadId = form.get("upload_id");
    const partIndexRaw = form.get("part_index");
    const totalPartsRaw = form.get("total_parts");
    const totalBytesRaw = form.get("total_bytes");
    const actorUserId = form.get("actor_user_id");
    const fileExtensionRaw = form.get("file_extension");
    const file = form.get("file");

    if (typeof uploadId !== "string" || !isUuidString(uploadId)) {
      return NextResponse.json({ ok: false, error: "Invalid upload_id." }, { status: 400 });
    }
    const partIndex = Number(partIndexRaw);
    const totalParts = Number(totalPartsRaw);
    const totalBytes = Number(totalBytesRaw);
    if (!Number.isFinite(partIndex) || partIndex < 0 || !Number.isFinite(totalParts) || totalParts < 1) {
      return NextResponse.json({ ok: false, error: "Invalid part indices." }, { status: 400 });
    }
    if (!(file instanceof Blob) || file.size === 0) {
      return NextResponse.json({ ok: false, error: "Missing chunk." }, { status: 400 });
    }

    // Fetch by primary key only — service-role bypasses RLS so no org filter is needed.
    // The org is read from the stored row and used only for logging.
    const { data: row, error: fetchErr } = await supabaseServer
      .from("raw_report_uploads")
      .select("id, organization_id, metadata")
      .eq("id", uploadId)
      .maybeSingle();

    const prefix = parseRawReportMetadata(row?.metadata).storagePrefix?.trim() ?? "";
    if (fetchErr || !prefix) {
      const orgId = String((row as { organization_id?: unknown } | null)?.organization_id ?? "");
      console.error("[imports/chunk] session not found", {
        uploadId,
        orgId,
        fetchErr: fetchErr?.message,
      });
      return NextResponse.json({ ok: false, error: "Upload session not found." }, { status: 404 });
    }

    const orgId = String((row as { organization_id?: unknown }).organization_id ?? "");
    console.info("[imports/chunk] received", {
      uploadId,
      partIndex,
      totalParts,
      chunkBytes: file instanceof Blob ? file.size : 0,
      orgId,
    });

    const path = `${prefix}/part-${String(partIndex).padStart(6, "0")}`;
    const buf = Buffer.from(await file.arrayBuffer());

    const ext =
      typeof fileExtensionRaw === "string" ? fileExtensionRaw.trim().toLowerCase().replace(/^\./, "") : "";
    const contentType =
      ext === "xlsx"
        ? "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        : ext === "txt"
          ? "text/plain; charset=utf-8"
          : "text/csv; charset=utf-8";

    const { error: upErr } = await supabaseServer.storage.from(BUCKET).upload(path, buf, {
      upsert: true,
      contentType,
    });

    if (upErr) {
      const { message, status } = formatStorageUploadError(upErr.message);
      console.error("[imports/chunk] storage upload failed", {
        uploadId,
        path,
        bucket: BUCKET,
        message: upErr.message,
      });
      return NextResponse.json({ ok: false, error: message }, { status });
    }

    const actor =
      typeof actorUserId === "string" && actorUserId.trim().length > 0 ? actorUserId.trim() : null;

    const upd = await updateUploadAfterChunk({
      uploadId,
      partIndex,
      totalParts,
      bytesUploadedDelta: buf.length,
      totalBytes: Number.isFinite(totalBytes) && totalBytes > 0 ? totalBytes : totalParts * buf.length,
      actorUserId: actor,
    });

    if (!upd.ok) {
      console.error("[imports/chunk] progress update failed", { uploadId, error: upd.error });
      return NextResponse.json({ ok: false, error: upd.error ?? "Progress update failed." }, { status: 403 });
    }

    console.info("[imports/chunk] ok", { uploadId, partIndex, path });
    return NextResponse.json({ ok: true, path });
  } catch (e) {
    return NextResponse.json(
      { ok: false, error: e instanceof Error ? e.message : "Chunk upload failed." },
      { status: 500 },
    );
  }
}
