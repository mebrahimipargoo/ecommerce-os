const fs = require("fs");
const p = "lib/import-sync-mappers.ts";
let s = fs.readFileSync(p, "utf8");
s = s.replace(
  '"id", "organization_id", "upload_id", "source_staging_id", "order_date",',
  '"id", "organization_id", "store_id", "upload_id", "source_staging_id", "order_date",'
);
if (!s.includes("store_id: string;\n  upload_id: string;")) {
  s = s.replace(
    "export type AmazonRemovalInsert = {\n  organization_id: string;\n  upload_id: string;",
    "export type AmazonRemovalInsert = {\n  organization_id: string;\n  store_id: string;\n  upload_id: string;"
  );
}
s = s.replace(
  "export function mapRowToAmazonRemoval(\n  row: Record<string, string>,\n  orgId: string,\n  uploadId: string,\n): AmazonRemovalInsert | null {",
  "export function mapRowToAmazonRemoval(\n  row: Record<string, string>,\n  orgId: string,\n  uploadId: string,\n  storeId: string,\n): AmazonRemovalInsert | null {"
);
s = s.replace(
  "  return {\n    organization_id: orgId,\n    upload_id: uploadId,\n    order_id,",
  "  return {\n    organization_id: orgId,\n    store_id: storeId,\n    upload_id: uploadId,\n    order_id,"
);
fs.writeFileSync(p, s);
console.log("mappers ok");
