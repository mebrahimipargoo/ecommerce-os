const fs = require("fs");
const p = "app/api/settings/imports/sync/route.ts";
let s = fs.readFileSync(p, "utf8").replace(/\r\n/g, "\n");

s = s.replace(
  `        const rid = q.shift()!;
        const payload: Record<string, unknown> = { tracking_number: tn };
        if ("carrier" in insertRow) payload.carrier = insertRow.carrier;
        if ("shipment_date" in insertRow) payload.shipment_date = insertRow.shipment_date;
        pendingUpdates.push({ id: rid, payload });`,
  `        const rid = q.shift()!;
        const existing = nullRowDetail[rid] ?? {};
        const payload: Record<string, unknown> = { tracking_number: tn };
        const incC = insertRow.carrier;
        if (incC !== undefined && incC !== null && String(incC).trim() !== "") {
          if (!pgTextUniqueField(existing.carrier as string | null)) payload.carrier = incC;
        }
        const incSd = insertRow.shipment_date;
        if (incSd !== undefined && incSd !== null && String(incSd).trim() !== "") {
          if (existing.shipment_date == null || String(existing.shipment_date).trim() === "") {
            payload.shipment_date = incSd;
          }
        }
        pendingUpdates.push({ id: rid, payload });`,
);

s = s.replace(
  `        const { data: candidates, error: hitErr } = await supabaseServer
          .from("amazon_removals")
          .select(
            "id, sku, fnsku, disposition, requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity, order_date, order_type",
          )
          .eq("organization_id", orgId)
          .eq("order_id", oid)
          .eq("tracking_number", tn)
          .limit(50);`,
  `        const { data: candidates, error: hitErr } = await supabaseServer
          .from("amazon_removals")
          .select(
            "id, carrier, shipment_date, sku, fnsku, disposition, requested_quantity, shipped_quantity, disposed_quantity, cancelled_quantity, order_date, order_type",
          )
          .eq("organization_id", orgId)
          .eq("store_id", storeId)
          .eq("order_id", oid)
          .eq("tracking_number", tn)
          .limit(50);`,
);

s = s.replace(
  `        if (hit?.id) {
          const payload: Record<string, unknown> = {};
          if ("carrier" in insertRow) payload.carrier = insertRow.carrier;
          if ("shipment_date" in insertRow) payload.shipment_date = insertRow.shipment_date;
          if (Object.keys(payload).length > 0) {
            pendingUpdates.push({ id: hit.id, payload });
          }
        }`,
  `        if (hit?.id) {
          const existing = hit as Record<string, unknown>;
          const payload: Record<string, unknown> = {};
          const incC = insertRow.carrier;
          if (incC !== undefined && incC !== null && String(incC).trim() !== "") {
            if (!pgTextUniqueField(existing.carrier as string | null)) payload.carrier = incC;
          }
          const incSd = insertRow.shipment_date;
          if (incSd !== undefined && incSd !== null && String(incSd).trim() !== "") {
            if (existing.shipment_date == null || String(existing.shipment_date).trim() === "") {
              payload.shipment_date = incSd;
            }
          }
          if (Object.keys(payload).length > 0) {
            pendingUpdates.push({ id: hit.id, payload });
          }
        }`,
);

s = s.replace(
  `  console.log(
    \`[REMOVAL_SHIPMENT] Done: archived=\${archived} removals_tracking_updates=\${removalsUpdated} mapper_null=\${mapperNull}\`,
  );`,
  `  console.log(
    JSON.stringify({
      phase: "REMOVAL_SHIPMENT_wave1_reconciliation",
      store_id: storeId,
      shipment_lines_archived: archived,
      removals_tracking_updates: removalsUpdated,
      mapper_null: mapperNull,
    }),
  );
  console.log(
    \`[REMOVAL_SHIPMENT] Done: archived=\${archived} removals_tracking_updates=\${removalsUpdated} mapper_null=\${mapperNull}\`,
  );`,
);

fs.writeFileSync(p, s.replace(/\n/g, "\r\n"));
console.log("payloads+json ok");
