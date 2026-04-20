# Wave 4 — FBA Inventory Engine Stabilisation

Surgical patch that stabilises four Amazon report families without touching any
previously-working importer:

1. `MANAGE_FBA_INVENTORY`
2. `FBA_INVENTORY` (Inventory Health)
3. `INBOUND_PERFORMANCE` *(new domain table)*
4. `AMAZON_FULFILLED_INVENTORY` *(new domain table)*

The five-phase pipeline contract is preserved end-to-end:

```
1) Upload  →  2) Map & classify  →  3) Process / staging  →  4) Sync  →  5) Generic
```

For these four families Generic is an explicit no-op. Sync now marks
`phase4_generic_pct = 100` and `phase4_status = 'complete'` so the UI never
sits in "pending forever".

---

## Files changed

### Library / pipeline

| File | Change |
| --- | --- |
| `lib/raw-report-types.ts` | Added `INBOUND_PERFORMANCE`, `AMAZON_FULFILLED_INVENTORY` to the `RawReportType` union. |
| `lib/csv-import-detected-type.ts` | Added the 2 new types to `CLASSIFIED_REPORT_TYPES`; added `CANONICAL_FIELDS_PER_TYPE` entries for all 4; added 4 hard-rule fingerprints **before** the listing rules; added 2 cases to `parseGptReportType`. |
| `lib/csv-import-mapping.ts` | Added `REPORT_TYPE_SPECS` aliases for `INBOUND_PERFORMANCE` and `AMAZON_FULFILLED_INVENTORY`; expanded aliases on existing `MANAGE_FBA_INVENTORY` and `FBA_INVENTORY`. |
| `lib/import-sync-mappers.ts` | Expanded `NATIVE_COLUMNS_MANAGE_FBA_INVENTORY` and `NATIVE_COLUMNS_FBA_INVENTORY` to the full per-spec set; added `NATIVE_COLUMNS_INBOUND_PERFORMANCE` and `NATIVE_COLUMNS_AMAZON_FULFILLED_INVENTORY`; added typed mappers `mapRowToAmazonManageFbaInventory`, `mapRowToAmazonFbaInventory`, `mapRowToAmazonInboundPerformance`, `mapRowToAmazonAmazonFulfilledInventory`. |
| `lib/pipeline/amazon-report-registry.ts` | Added `INBOUND_PERFORMANCE` + `AMAZON_FULFILLED_INVENTORY` to `AmazonSyncKind`, registry entries (`supports_generic = false`), and `resolveAmazonImportSyncKind`. |
| `lib/pipeline/amazon-phase2-staging.ts` | Added 2 new types to `KNOWN_TYPES`. |
| `lib/pipeline/amazon-sync-batch-metrics.ts` | Added 2 new kinds to `byPhysicalLine` switch. |
| `lib/classify-import-headers-openai.ts` | Updated GPT prompt: stricter signals for `MANAGE_FBA_INVENTORY` / `FBA_INVENTORY`; added `INBOUND_PERFORMANCE` + `AMAZON_FULFILLED_INVENTORY` (with explicit "do NOT classify as listing" guard). |
| `app/api/settings/imports/sync/route.ts` | Imported the 4 typed mappers + 2 new native-column sets; added them to `NATIVE_COLUMNS_MAP`; routed each kind through its typed mapper instead of `mapRowToAmazonRawArchive`; added the 2 new kinds to `deduplicateByConflictKey`; **fixed Phase 5 contract** so `phase4_generic_pct: needsPhase4 ? 0 : 100`. |
| `app/(admin)/imports/ColumnMappingModal.tsx` | Added 2 new types to the modal dropdown. |

### Migrations (new)

| File | Purpose |
| --- | --- |
| `supabase/migrations/20260622_fba_inventory_engine_wave4.sql` | (a) Adds the full native column set to `amazon_manage_fba_inventory` and `amazon_fba_inventory` (additive `ADD COLUMN IF NOT EXISTS`). (b) Creates `amazon_inbound_performance` and `amazon_amazon_fulfilled_inventory` with `(organization_id, source_file_sha256, source_physical_row_number)` unique key and service-role RLS bypass. (c) Extends the `raw_report_uploads.report_type` CHECK constraint with the 2 new types. |

### Untouched (intentionally)

* All previously-working importers (`FBA_RETURNS`, `REMOVAL_ORDER`,
  `REMOVAL_SHIPMENT`, `INVENTORY_LEDGER`, `REIMBURSEMENTS`, `SETTLEMENT`,
  `SAFET_CLAIMS`, `TRANSACTIONS`, `REPORTS_REPOSITORY`, `ALL_LISTINGS`,
  `ACTIVE_LISTINGS`, `CATEGORY_LISTINGS`).
* `INVENTORY_LEDGER`'s inline Generic enrichment in
  `lib/inventory-ledger-generic-completion.ts` and the `/generic` route
  handler. IL keeps `supports_generic: true` and continues to auto-complete
  Phase 5 inline at the end of Sync; the only behavioural change to IL is the
  fixed Phase 5 progress metadata when `supports_generic` happens to be false
  (which IL is not).
* No tables / views / indexes deleted or renamed.

---

## Detection contract

Hard rules run **before** GPT fallback and **before** the listing rules so
none of the four files can ever be misclassified as a listing export:

| Family | Required headers (normalised) |
| --- | --- |
| `MANAGE_FBA_INVENTORY` | `fnsku` + `afn-fulfillable-quantity` + (`afn-warehouse-quantity` OR `afn-inbound-working-quantity` OR `afn-inbound-receiving-quantity`) |
| `FBA_INVENTORY` | `snapshot-date` + `fnsku` + `available` + (`inbound-quantity` OR `inbound-working` OR `inbound-received` OR `inventory-supply-at-fba` OR `total-reserved-quantity`) |
| `INBOUND_PERFORMANCE` | `fba-shipment-id` + `problem-type` + (`expected-quantity` OR `received-quantity` OR `problem-quantity` OR `fba-carton-id`) |
| `AMAZON_FULFILLED_INVENTORY` | `seller-sku` + `fulfillment-channel-sku` + `asin` + `quantity-available` |

Header normalisation flattens hyphens, underscores, and whitespace, so
`afn-fulfillable-quantity`, `AFN Fulfillable Quantity`, and
`AFN_Fulfillable_Quantity` all match.

---

## Sync registry summary (4 families)

```
dedupeMode                = source_line_hash
conflictColumns           = organization_id,source_file_sha256,source_physical_row_number
postSyncEnrichment        = none
generateWorklistAfterSync = false
supports_generic          = false   (Generic = no-op; Sync marks Phase 5 done)
```

---

## Manual test steps

Run these for each of the 4 sample files. Replace `<UPLOAD_ID>` with the
returned upload row id from step 2.

1. **Apply migration**
   ```bash
   supabase db push
   # or: psql -f supabase/migrations/20260622_fba_inventory_engine_wave4.sql
   ```

2. **Upload the CSV** through the existing Imports UI (Settings → Imports →
   Upload). Confirm:
   * `raw_report_uploads.report_type` is the expected canonical value
     (`MANAGE_FBA_INVENTORY` / `FBA_INVENTORY` / `INBOUND_PERFORMANCE` /
     `AMAZON_FULFILLED_INVENTORY`) — set by the rule engine, not GPT.
   * `column_mapping` is auto-populated with the canonical fields shown in
     the mapping modal.

3. **Process** (Phase 2):
   ```
   POST /api/settings/imports/process { "upload_id": "<UPLOAD_ID>" }
   ```
   Expect `200 ok:true`. `raw_report_uploads.status = 'staged'`.

4. **Sync** (Phase 3):
   ```
   POST /api/settings/imports/sync { "upload_id": "<UPLOAD_ID>" }
   ```
   Expect `200 ok:true` with `kind` matching the family. After completion:
   * `raw_report_uploads.status = 'synced'`.
   * `file_processing_status.phase3_status = 'complete'`,
     `phase3_raw_sync_pct = 100`.
   * `file_processing_status.phase4_status = 'complete'`,
     `phase4_generic_pct = 100` (this is the no-op contract for these 4
     families — UI shows the 5-phase pipeline as done).

5. **Re-upload the same bytes** to verify idempotent re-import:
   * `(organization_id, source_file_sha256, source_physical_row_number)`
     keys collide → `rows_synced_unchanged` increments,
     `rows_synced_new = 0`.

6. **Listing-export regression check** — re-upload a known
   `ALL_LISTINGS`/`ACTIVE_LISTINGS` file and confirm classification still
   resolves to the listing kind (the new rules sit before the listing rules
   but only fire when the inventory anchors match).

---

## SQL verification queries

Replace `:org` and `:upload` with the values for the sync you just ran.

```sql
-- 1. Per-family row count for the upload (should equal data rows in the file)
SELECT 'MANAGE_FBA_INVENTORY' AS report, COUNT(*) AS rows
  FROM public.amazon_manage_fba_inventory
  WHERE organization_id = :org AND source_upload_id = :upload
UNION ALL
SELECT 'FBA_INVENTORY', COUNT(*)
  FROM public.amazon_fba_inventory
  WHERE organization_id = :org AND source_upload_id = :upload
UNION ALL
SELECT 'INBOUND_PERFORMANCE', COUNT(*)
  FROM public.amazon_inbound_performance
  WHERE organization_id = :org AND source_upload_id = :upload
UNION ALL
SELECT 'AMAZON_FULFILLED_INVENTORY', COUNT(*)
  FROM public.amazon_amazon_fulfilled_inventory
  WHERE organization_id = :org AND source_upload_id = :upload;
```

```sql
-- 2. Sample row + raw_data overflow check (raw_data must be null only when
--    every CSV column was modeled; for these reports raw_data should typically
--    contain whatever Amazon adds beyond the spec).
SELECT id, sku, fnsku, asin, afn_fulfillable_quantity, afn_warehouse_quantity,
       afn_inbound_working_quantity, raw_data
  FROM public.amazon_manage_fba_inventory
  WHERE organization_id = :org AND source_upload_id = :upload
  ORDER BY source_physical_row_number ASC
  LIMIT 5;

SELECT id, snapshot_date, sku, fnsku, available, inbound_quantity,
       total_reserved_quantity, raw_data
  FROM public.amazon_fba_inventory
  WHERE organization_id = :org AND source_upload_id = :upload
  ORDER BY source_physical_row_number ASC
  LIMIT 5;

SELECT id, fba_shipment_id, problem_type, expected_quantity, received_quantity,
       problem_quantity, fba_carton_id, raw_data
  FROM public.amazon_inbound_performance
  WHERE organization_id = :org AND source_upload_id = :upload
  ORDER BY source_physical_row_number ASC
  LIMIT 5;

SELECT id, seller_sku, fulfillment_channel_sku, asin, quantity_available,
       condition_type, raw_data
  FROM public.amazon_amazon_fulfilled_inventory
  WHERE organization_id = :org AND source_upload_id = :upload
  ORDER BY source_physical_row_number ASC
  LIMIT 5;
```

```sql
-- 3. Physical-row identity uniqueness — must be 0
SELECT 'manage_fba_inventory' AS tbl, COUNT(*) AS dup_keys FROM (
  SELECT 1 FROM public.amazon_manage_fba_inventory
   WHERE organization_id = :org
   GROUP BY organization_id, source_file_sha256, source_physical_row_number
   HAVING COUNT(*) > 1
) x
UNION ALL
SELECT 'fba_inventory', COUNT(*) FROM (
  SELECT 1 FROM public.amazon_fba_inventory
   WHERE organization_id = :org
   GROUP BY organization_id, source_file_sha256, source_physical_row_number
   HAVING COUNT(*) > 1
) x
UNION ALL
SELECT 'inbound_performance', COUNT(*) FROM (
  SELECT 1 FROM public.amazon_inbound_performance
   WHERE organization_id = :org
   GROUP BY organization_id, source_file_sha256, source_physical_row_number
   HAVING COUNT(*) > 1
) x
UNION ALL
SELECT 'amazon_fulfilled_inventory', COUNT(*) FROM (
  SELECT 1 FROM public.amazon_amazon_fulfilled_inventory
   WHERE organization_id = :org
   GROUP BY organization_id, source_file_sha256, source_physical_row_number
   HAVING COUNT(*) > 1
) x;
```

```sql
-- 4. Pipeline status / 5-phase contract
SELECT upload_id, status, phase_key,
       phase1_upload_pct, phase2_stage_pct, phase3_raw_sync_pct,
       phase4_generic_pct, phase4_status, phase4_completed_at
  FROM public.file_processing_status
 WHERE upload_id = :upload;
-- Expect: status = 'complete', phase4_status = 'complete',
--         phase4_generic_pct = 100, phase4_completed_at IS NOT NULL.
```

```sql
-- 5. Regression — confirm the listing kinds still classify a known listings
--    file (run after re-importing one):
SELECT id, file_name, report_type, status
  FROM public.raw_report_uploads
 WHERE id = :listings_upload;
-- Expect report_type IN ('ALL_LISTINGS','ACTIVE_LISTINGS','CATEGORY_LISTINGS').
```
