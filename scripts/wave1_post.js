const fs = require("fs");
const p = "app/api/settings/imports/sync/route.ts";
let s = fs.readFileSync(p, "utf8").replace(/\r\n/g, "\n");

if (!s.includes("const importStoreId = resolveImportStoreId")) {
  s = s.replace(
    `    const meta = (row as { metadata?: unknown }).metadata;

    // ── Optimistic lock — prevents concurrent clicks from double-syncing ───────`,
    `    const meta = (row as { metadata?: unknown }).metadata;

    const importStoreId = resolveImportStoreId(meta);
    if ((kind === "REMOVAL_ORDER" || kind === "REMOVAL_SHIPMENT") && !importStoreId) {
      return NextResponse.json(
        {
          ok: false,
          error:
            "Imports Target Store is required for removal reports. Choose a target store in the importer, save classification, then run Sync again.",
        },
        { status: 422 },
      );
    }

    // ── Optimistic lock — prevents concurrent clicks from double-syncing ───────`,
  );
}

s = s.replace(
  `      const r = await runRemovalShipmentSync({
        uploadId,
        orgId,
        totalStagingRows,
        columnMapping,
        syncUpserted,
      });`,
  `      const r = await runRemovalShipmentSync({
        uploadId,
        orgId,
        storeId: importStoreId!,
        totalStagingRows,
        columnMapping,
        syncUpserted,
      });`,
);

s = s.replace(
  `            insertRow = mapRowToAmazonRemoval(mappedRow, orgId, uploadId) as Record<string, unknown> | null;
            if (insertRow) insertRow.source_staging_id = sr.id;`,
  `            insertRow = mapRowToAmazonRemoval(mappedRow, orgId, uploadId, importStoreId!) as Record<string, unknown> | null;
            if (insertRow) insertRow.source_staging_id = sr.id;`,
);

const markOld = `    const syncCollapsedByDedupe =
      kind !== "REMOVAL_SHIPMENT"
        ? Math.max(0, totalStagingRows - synced - mapperNullCount)
        : 0;

    const { error: markErr } = await supabaseServer
      .from("raw_report_uploads")
      .update({
        status: "synced",
        metadata: mergeUploadMetadata(
          (prevRow as { metadata?: unknown } | null)?.metadata,
          {
            row_count: synced,
            staging_row_count: totalStagingRows,
            sync_row_count: synced,
            sync_mapper_null_count: mapperNullCount,
            sync_collapsed_by_dedupe: syncCollapsedByDedupe,
            process_progress: 100,
            sync_progress: 100,
            etl_phase: "sync",
            error_message: undefined,
          },
        ),`;

const markNew = `    const syncCollapsedByDedupe =
      kind !== "REMOVAL_SHIPMENT"
        ? Math.max(0, totalStagingRows - synced - mapperNullCount)
        : 0;

    const wave1Extra =
      kind === "REMOVAL_ORDER" || kind === "REMOVAL_SHIPMENT"
        ? {
            wave1_import_store_id: importStoreId,
            wave1_sync_reconciliation: {
              kind,
              staging_row_count: totalStagingRows,
              domain_rows_written: synced,
              mapper_null: mapperNullCount,
              collapsed_by_business_dedupe: syncCollapsedByDedupe,
            },
          }
        : {};

    const { error: markErr } = await supabaseServer
      .from("raw_report_uploads")
      .update({
        status: "synced",
        metadata: mergeUploadMetadata(
          (prevRow as { metadata?: unknown } | null)?.metadata,
          {
            row_count: synced,
            staging_row_count: totalStagingRows,
            sync_row_count: synced,
            sync_mapper_null_count: mapperNullCount,
            sync_collapsed_by_dedupe: syncCollapsedByDedupe,
            process_progress: 100,
            sync_progress: 100,
            etl_phase: "sync",
            error_message: undefined,
            ...wave1Extra,
          },
        ),`;

if (s.includes(markOld)) s = s.replace(markOld, markNew);
else console.warn("markOld pattern missing — skip wave1 metadata");

fs.writeFileSync(p, s.replace(/\n/g, "\r\n"));
console.log("post ok");
