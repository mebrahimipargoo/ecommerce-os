const fs = require("fs");
const p = "app/api/settings/imports/sync/route.ts";
let s = fs.readFileSync(p, "utf8");
const marker = '      case "REMOVAL_ORDER":\r\n      case "REMOVAL_SHIPMENT":';
const start = s.indexOf(marker);
if (start < 0) { console.error("start miss"); process.exit(1); }
const inv = '      case "INVENTORY_LEDGER":';
const end = s.indexOf(inv, start);
const neu =
  '      case "REMOVAL_ORDER":\r\n' +
  '        key = removalBusinessDedupKey(row);\r\n' +
  '        break;\r\n' +
  '      case "REMOVAL_SHIPMENT":\r\n' +
  '        key =\r\n' +
  '          row.source_staging_id != null && String(row.source_staging_id).trim() !== ""\r\n' +
  '            ? `sid|${String(row.organization_id)}|${String(row.upload_id)}|${String(row.source_staging_id)}`\r\n' +
  '            : removalLogicalLineDedupKey(row);\r\n' +
  '        break;\r\n';
s = s.slice(0, start) + neu + s.slice(end);
fs.writeFileSync(p, s);
console.log("dedupe crlf ok");
