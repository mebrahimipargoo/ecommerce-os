import { NextRequest, NextResponse } from "next/server";
import { supabaseServer } from "../../../../../lib/supabase-server";

const BUCKET = "raw-reports";

/**
 * POST /api/admin/ledger/upload
 * Accepts a multipart FormData body with:
 *   - file: the raw CSV File
 *   - org_id: the target organization UUID
 * Uploads to `raw-reports` Supabase Storage bucket and returns the storage path.
 * The AbortController signal on the client fetch will cancel this request mid-transfer.
 */
export async function POST(req: NextRequest) {
  try {
    const formData = await req.formData();
    const file = formData.get("file") as File | null;
    if (!file) {
      return NextResponse.json({ error: "No file provided." }, { status: 400 });
    }

    const orgId = (formData.get("org_id") as string | null)?.trim() || "unknown";
    const ext = file.name.split(".").pop() ?? "csv";
    const ts = Date.now();
    const path = `amazon-ledger/${orgId}/${ts}.${ext}`;

    const buffer = Buffer.from(await file.arrayBuffer());

    const { error } = await supabaseServer.storage
      .from(BUCKET)
      .upload(path, buffer, {
        contentType: file.type || "text/csv",
        upsert: true,
      });

    if (error) {
      return NextResponse.json({ error: error.message }, { status: 500 });
    }

    return NextResponse.json({ ok: true, path });
  } catch (e) {
    return NextResponse.json(
      { error: e instanceof Error ? e.message : "Upload failed." },
      { status: 500 },
    );
  }
}
