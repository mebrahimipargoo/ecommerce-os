const fs = require("fs");
const p = "app/api/settings/imports/sync/route.ts";
let s = fs.readFileSync(p, "utf8").replace(/\r\n/g, "\n");

const oldArchive = `    const archiveRows: Record<string, unknown>[] = [];
    for (const sr of stagingRows) {
      const rawObj = parseStagingRawRow(sr.raw_row);
      archiveRows.push({
        organization_id: orgId,
        upload_id: uploadId,
        amazon_staging_id: sr.id,
        raw_row: rawObj,
      });
    }

    for (let i = 0; i < archiveRows.length; i += UPSERT_CHUNK_SIZE) {
      const chunk = archiveRows.slice(i, i + UPSERT_CHUNK_SIZE);
      const { error: insErr } = await supabaseServer.from("amazon_removal_shipments").insert(chunk);
      if (insErr) {
        throw new Error(
          \`[REMOVAL_SHIPMENT] insert into amazon_removal_shipments failed: \${insErr.message}\`,
        );
      }
      archived += chunk.length;
      await bumpSyncProgressMetadata(
        { uploadId, orgId, totalStagingRows, upserted: syncUpserted },
        chunk.length,
      );
    }

    const pendingUpdates: { id: string; payload: Record<string, unknown> }[] = [];

    for (const sr of stagingRows) {
      const rawObj = parseStagingRawRow(sr.raw_row);
      const mappedRow = applyColumnMappingToRow(rawObj, columnMapping);
      const insertRow = mapRowToAmazonRemoval(mappedRow, orgId, uploadId) as Record<string, unknown> | null;`;

const newArchive = `    const archiveRows: Record<string, unknown>[] = [];
    for (const sr of stagingRows) {
      const rawObj = parseStagingRawRow(sr.raw_row);
      const mappedRow = applyColumnMappingToRow(rawObj, columnMapping);
      const mappedRemoval = mapRowToAmazonRemoval(mappedRow, orgId, uploadId, storeId) as Record<string, unknown> | null;
      const baseArchive: Record<string, unknown> = {
        organization_id: orgId,
        upload_id: uploadId,
        amazon_staging_id: sr.id,
        store_id: storeId,
        raw_row: rawObj,
      };
      if (mappedRemoval) {
        Object.assign(baseArchive, {
          order_id: mappedRemoval.order_id ?? null,
          sku: mappedRemoval.sku ?? null,
          fnsku: mappedRemoval.fnsku ?? null,
          disposition: mappedRemoval.disposition ?? null,
          tracking_number: mappedRemoval.tracking_number ?? null,
          carrier: mappedRemoval.carrier ?? null,
          shipment_date: mappedRemoval.shipment_date ?? null,
          order_date: mappedRemoval.order_date ?? null,
          order_type: mappedRemoval.order_type ?? null,
          requested_quantity: mappedRemoval.requested_quantity ?? null,
          shipped_quantity: mappedRemoval.shipped_quantity ?? null,
          disposed_quantity: mappedRemoval.disposed_quantity ?? null,
          cancelled_quantity: mappedRemoval.cancelled_quantity ?? null,
        });
      }
      archiveRows.push(baseArchive);
    }

    for (let i = 0; i < archiveRows.length; i += UPSERT_CHUNK_SIZE) {
      const chunk = archiveRows.slice(i, i + UPSERT_CHUNK_SIZE);
      const { error: insErr } = await supabaseServer.from("amazon_removal_shipments").upsert(chunk, {
        onConflict: "organization_id,upload_id,amazon_staging_id",
        ignoreDuplicates: false,
      });
      if (insErr) {
        throw new Error(
          \`[REMOVAL_SHIPMENT] upsert into amazon_removal_shipments failed: \${insErr.message}\`,
        );
      }
      archived += chunk.length;
      await bumpSyncProgressMetadata(
        { uploadId, orgId, totalStagingRows, upserted: syncUpserted },
        chunk.length,
      );
    }

    const pendingUpdates: { id: string; payload: Record<string, unknown> }[] = [];

    for (const sr of stagingRows) {
      const rawObj = parseStagingRawRow(sr.raw_row);
      const mappedRow = applyColumnMappingToRow(rawObj, columnMapping);
      const insertRow = mapRowToAmazonRemoval(mappedRow, orgId, uploadId, storeId) as Record<string, unknown> | null;`;

if (!s.includes(oldArchive)) {
  console.error("oldArchive block not found");
  process.exit(1);
}
s = s.replace(oldArchive, newArchive);

fs.writeFileSync(p, s.replace(/\n/g, "\r\n"));
console.log("archive+upsert ok");
