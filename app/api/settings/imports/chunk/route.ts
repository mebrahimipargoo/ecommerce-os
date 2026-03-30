import { NextResponse } from "next/server";

import { resolveOrganizationId } from "../../../../../lib/organization";
import { updateUploadAfterChunk } from "../../../../../lib/import-upload-progress";
import { supabaseServer } from "../../../../../lib/supabase-server";
import { isUuidString } from "../../../../../lib/uuid";

export const runtime = "nodejs";

const BUCKET = "raw-reports";

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

    const orgId = resolveOrganizationId();
    const { data: row, error: fetchErr } = await supabaseServer
      .from("raw_report_uploads")
      .select("id, storage_prefix, uploaded_by, organization_id")
      .eq("id", uploadId)
      .eq("organization_id", orgId)
      .maybeSingle();

    const prefix =
      typeof row?.storage_prefix === "string" ? row.storage_prefix.trim() : "";
    if (fetchErr || !prefix) {
      console.error("[imports/chunk] session not found", {
        uploadId,
        orgId,
        fetchErr: fetchErr?.message,
      });
      return NextResponse.json({ ok: false, error: "Upload session not found." }, { status: 404 });
    }

    console.info("[imports/chunk] received", {
      uploadId,
      partIndex,
      totalParts,
      chunkBytes: file instanceof Blob ? file.size : 0,
      orgId,
    });

    const path = `${prefix}/part-${String(partIndex).padStart(6, "0")}`;
    const buf = Buffer.from(await file.arrayBuffer());

    const { error: upErr } = await supabaseServer.storage.from(BUCKET).upload(path, buf, {
      upsert: true,
      contentType: "text/csv",
    });

    if (upErr) {
      console.error("[imports/chunk] storage upload failed", { uploadId, path, message: upErr.message });
      return NextResponse.json({ ok: false, error: upErr.message }, { status: 500 });
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
