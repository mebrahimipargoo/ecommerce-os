import { NextResponse } from "next/server";

import { setUploadByteProgress } from "../../../../../lib/import-upload-progress";
import { isUuidString } from "../../../../../lib/uuid";

export const runtime = "nodejs";

/**
 * POST { upload_id, uploaded_bytes, total_bytes }
 * Persists real upload progress (XHR byte counts) to raw_report_uploads + file_processing_status.
 */
export async function POST(req: Request): Promise<Response> {
  try {
    const body = (await req.json()) as {
      upload_id?: unknown;
      uploaded_bytes?: unknown;
      total_bytes?: unknown;
    };
    const uploadId = typeof body.upload_id === "string" ? body.upload_id.trim() : "";
    const uploadedBytes = Number(body.uploaded_bytes);
    const totalBytes = Number(body.total_bytes);
    if (!isUuidString(uploadId)) {
      return NextResponse.json({ ok: false, error: "Invalid upload_id." }, { status: 400 });
    }
    if (!Number.isFinite(uploadedBytes) || uploadedBytes < 0) {
      return NextResponse.json({ ok: false, error: "Invalid uploaded_bytes." }, { status: 400 });
    }
    if (!Number.isFinite(totalBytes) || totalBytes <= 0) {
      return NextResponse.json({ ok: false, error: "Invalid total_bytes." }, { status: 400 });
    }

    const r = await setUploadByteProgress({
      uploadId,
      uploadedBytes,
      totalBytes,
    });
    if (!r.ok) {
      return NextResponse.json({ ok: false, error: r.error ?? "Update failed." }, { status: 400 });
    }
    return NextResponse.json({ ok: true });
  } catch (e) {
    const message = e instanceof Error ? e.message : "Invalid request.";
    return NextResponse.json({ ok: false, error: message }, { status: 400 });
  }
}
