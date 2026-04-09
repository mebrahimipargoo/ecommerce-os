const fs = require("fs");
const p = "app/api/settings/imports/sync/route.ts";
let raw = fs.readFileSync(p, "utf8");
let s = raw.replace(/\r\n/g, "\n");

s = s.replace(
  `type RemovalShipmentSyncOpts = {
  uploadId: string;
  orgId: string;
  totalStagingRows: number;
  columnMapping: Record<string, string> | null;
  syncUpserted: { value: number };
};`,
  `type RemovalShipmentSyncOpts = {
  uploadId: string;
  orgId: string;
  storeId: string;
  totalStagingRows: number;
  columnMapping: Record<string, string> | null;
  syncUpserted: { value: number };
};`,
);

s = s.replace(
  `  const { uploadId, orgId, totalStagingRows, columnMapping, syncUpserted } = opts;`,
  `  const { uploadId, orgId, storeId, totalStagingRows, columnMapping, syncUpserted } = opts;`,
);

s = s.replace(
  `    .select(
      "id, order_id, sku, fnsku, disposition, requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity, order_date, order_type",
    )
    .eq("organization_id", orgId)
    .is("tracking_number", null);`,
  `    .select(
      "id, store_id, carrier, shipment_date, order_id, sku, fnsku, disposition, requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity, order_date, order_type",
    )
    .eq("organization_id", orgId)
    .eq("store_id", storeId)
    .is("tracking_number", null);`,
);

s = s.replace(
  `  const nullQueues = new Map<string, string[]>();
  for (const r of nullRows ?? []) {
    const k = removalLineKeyFromMapped(r as Record<string, unknown>);
    const arr = nullQueues.get(k);
    if (arr) arr.push(String((r as { id: unknown }).id));
    else nullQueues.set(k, [String((r as { id: unknown }).id)]);
  }`,
  `  const nullQueues = new Map<string, string[]>();
  const nullRowDetail: Record<string, Record<string, unknown>> = {};
  for (const r of nullRows ?? []) {
    const rid = String((r as { id: unknown }).id);
    nullRowDetail[rid] = r as Record<string, unknown>;
    const k = removalLineKeyFromMapped(r as Record<string, unknown>);
    const arr = nullQueues.get(k);
    if (arr) arr.push(rid);
    else nullQueues.set(k, [rid]);
  }`,
);

fs.writeFileSync(p, s.replace(/\n/g, "\r\n"));
console.log("shipment opts+null ok");
