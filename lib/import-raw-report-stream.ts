import "server-only";

import { PassThrough, Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { SupabaseClient } from "@supabase/supabase-js";

const BUCKET = "raw-reports";

/**
 * Streams concatenated chunk parts from `raw-reports` without loading the full file into memory.
 * Each part is fetched as a byte stream (typically 4MB) and written sequentially to the output.
 */
export function createConcatenatedPartsReadable(
  supabase: SupabaseClient,
  storagePrefix: string,
  totalParts: number,
): Readable {
  const out = new PassThrough();

  void (async () => {
    try {
      for (let i = 0; i < totalParts; i++) {
        const path = `${storagePrefix}/part-${String(i).padStart(6, "0")}`;
        const { data: sig, error } = await supabase.storage.from(BUCKET).createSignedUrl(path, 3600);
        if (error || !sig?.signedUrl) {
          throw new Error(error?.message ?? `Could not sign storage path: ${path}`);
        }
        const res = await fetch(sig.signedUrl);
        if (!res.ok) {
          throw new Error(`Storage fetch failed for part ${i} (${res.status})`);
        }
        if (!res.body) {
          throw new Error(`No response body for part ${i}`);
        }
        const partReadable = Readable.fromWeb(res.body as import("stream/web").ReadableStream);
        await pipeline(partReadable, out, { end: false });
      }
      out.end();
    } catch (e) {
      out.destroy(e instanceof Error ? e : new Error(String(e)));
    }
  })();

  return out;
}
