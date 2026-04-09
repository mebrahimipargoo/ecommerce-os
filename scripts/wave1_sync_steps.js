const fs = require("fs");
const p = "app/api/settings/imports/sync/route.ts";
let s = fs.readFileSync(p, "utf8");
s = s.replace(
  `  REMOVAL_ORDER:      "organization_id,upload_id,source_staging_id"`,
  `  REMOVAL_ORDER:      "organization_id,store_id,order_id,sku,fnsku,disposition,requested_quantity,shipped_quantity,disposed_quantity,cancelled_quantity,order_date,order_type"`,
);
s = s.replace(
  `  // One row per amazon_staging line — matches uq_amazon_removals_source_staging_line.`,
  `  // Wave 1: business-line dedupe (uq_amazon_removals_business_line); upload/staging idempotency via same upsert updating source_staging_id.`,
);
fs.writeFileSync(p, s);
console.log("step2 conflict");
