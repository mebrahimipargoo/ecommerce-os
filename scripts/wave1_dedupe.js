const fs = require("fs");
const p = "app/api/settings/imports/sync/route.ts";
let s = fs.readFileSync(p, "utf8");
const old = `      case "REMOVAL_ORDER":
      case "REMOVAL_SHIPMENT":
        key =
          row.source_staging_id != null && String(row.source_staging_id).trim() !== ""
            ? \`sid|\${String(row.organization_id)}|\${String(row.upload_id)}|\${String(row.source_staging_id)}\`
            : removalLogicalLineDedupKey(row);
        break;`;
const neu = `      case "REMOVAL_ORDER":
        key = removalBusinessDedupKey(row);
        break;
      case "REMOVAL_SHIPMENT":
        key =
          row.source_staging_id != null && String(row.source_staging_id).trim() !== ""
            ? \`sid|\${String(row.organization_id)}|\${String(row.upload_id)}|\${String(row.source_staging_id)}\`
            : removalLogicalLineDedupKey(row);
        break;`;
if (!s.includes("case \"REMOVAL_ORDER\":\n        key = removalBusinessDedupKey")) {
  if (!s.includes(old)) { console.error("dedupe pattern missing"); process.exit(1); }
  s = s.replace(old, neu);
}
s = s.replace(
  ` * REMOVAL_ORDER: keyed by source_staging_id — one row per staging line (no within-batch merge).`,
  ` * REMOVAL_ORDER: Wave 1 business key (store + logical line); last-wins within batch.`,
);
fs.writeFileSync(p, s);
console.log("dedupe ok");
