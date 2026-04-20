# Wave 5 — Inventory-family identity enrichment

Adds **safe FNSKU coverage growth** for `product_identifier_map` from the four
Wave-4 inventory tables. Mirrors the existing inventory-ledger enricher
pattern exactly. Touches no other importer, no claim flow, no UI, no listing
logic, and **no migration is required**.

---

## 1) Inspection summary (recap)

Reuses, never duplicates:

* `public.product_identifier_map` — every column we need is already there
  (`fnsku, seller_sku, asin, msku, source_upload_id, match_source,
  inventory_source, confidence_score, linked_from_*`). Indexes
  `idx_product_identifier_map_org_fnsku`, `idx_product_identifier_map_org_sku_store_asin`,
  `idx_product_identifier_map_org_asin` already cover our lookups.
* `lib/product-identifier-match.ts` — `pickBestProductIdentifierMatch`,
  `prefetchIdentifierMapCandidatesForBatch` already implement the four-tier
  priority (FNSKU → seller_sku+ASIN → seller_sku/msku → ASIN).
* `lib/inventory-ledger-identifier-enrich.ts` — reference pattern (untouched).
* `lib/amazon-raw-payload-pick.ts` — fallback raw_data picker.

Verified absent in the codebase (the user named these — none exist; we did
NOT invent them):

* `public.catalog_identity_unresolved_backlog`
* `public.v_unified_product_identity`
* `public.v_catalog_identifier_matches`
* `public.v_catalog_fnsku_gap_analysis`
* `public.v_product_relation_resolver`
* `public.v_product_identifier_priority_matches`

The only existing identity helper view is
`public.v_inventory_ledger_identifier_candidates` — ledger-specific. We do
not modify it.

AFI FNSKU equivalence: `amazon_amazon_fulfilled_inventory.fulfillment_channel_sku`
IS the FNSKU per Amazon's Fulfilled Inventory report definition, and the
Wave-4 mapper already accepts both header spellings on the same column.
Treating it as FNSKU here is consistent with the established convention.

**No migration required in Wave 5.**

---

## 2) Changed files

| File | Status | Notes |
|---|---|---|
| `lib/inventory-family-identifier-enrich.ts` | **NEW** | Shared per-family enricher. One source spec per inventory family (table, fnsku/sku/asin column names, raw_data fallback keys, confidence weight, provenance slugs). Reuses `pickBestProductIdentifierMatch`, `prefetchIdentifierMapCandidatesForBatch`, `pickRawPayloadFields`. |
| `app/api/settings/imports/identity-enrich/route.ts` | **NEW** | `POST /api/settings/imports/identity-enrich` — opt-in trigger that takes `{ upload_id }`, looks up the inventory family from `raw_report_uploads.report_type`, and runs the shared enricher. Does NOT mutate `raw_report_uploads.status`, `file_processing_status`, the importer pipeline, or any operational row. |

No other files changed. No migration created.

---

## 3) Source-aware confidence (live values)

| Source | Insert confidence | match_source slug | inventory_source |
|---|---|---|---|
| `inventory_ledger` (existing — untouched) | `0.95` | `inventory_ledger_fnsku` | `inventory_ledger` |
| `manage_fba_inventory` | `0.92` | `manage_fba_inventory_fnsku` | `manage_fba_inventory` |
| `inbound_performance` | `0.90` | `inbound_performance_fnsku` | `inbound_performance` |
| `fba_inventory` | `0.90` | `fba_inventory_fnsku` | `fba_inventory` |
| `amazon_fulfilled_inventory` | `0.88` | `amazon_fulfilled_inventory_fnsku` | `amazon_fulfilled_inventory` |
| `listing_catalog` (existing — untouched) | `1.00` | `listing_catalog` | `null` |

Update rule (mirrors existing IL): `confidence_score = max(existing, source_weight)`.
Stronger sources (listing_catalog, inventory_ledger) are never weakened.
`inventory_source` is preserved when the existing value is stronger
(`inventory_ledger` is never demoted).

Safety invariants enforced inside the helper:

1. Candidate FNSKU must be non-null — rows without FNSKU are counted in
   `rows_skipped_no_fnsku` and never written.
2. If the matched bridge row already has a different non-null FNSKU, the
   candidate FNSKU is **not** overwritten (`conflictFnsku` branch).
3. `seller_sku`, `msku`, `asin` are filled only when the existing column is
   null — a stronger upstream value is never replaced.
4. `linked_from_report_family` / `linked_from_target_table` are stamped only
   when previously null — `listing` and `inventory_ledger` provenance is
   preserved.
5. Unique-key 23505 races fall through to a re-fetch + merge, never a hard
   failure.

---

## 4) SQL verify queries

Replace `:org` and `:upload` with the IDs you ran the enrichment against.

### A. Coverage delta — bridge rows for the upload's FNSKU set

```sql
-- All bridge rows whose source_upload_id matches this upload OR whose FNSKU
-- appears in the upload's source table.
WITH src AS (
  SELECT DISTINCT
    nullif(btrim(t.fnsku::text), '') AS fnsku
  FROM public.amazon_manage_fba_inventory t   -- swap table per family
  WHERE t.organization_id = :org
    AND t.source_upload_id = :upload
    AND nullif(btrim(t.fnsku::text), '') IS NOT NULL
)
SELECT
  COUNT(*) FILTER (WHERE pim.source_upload_id = :upload)            AS bridge_rows_attributed_to_upload,
  COUNT(*) FILTER (WHERE pim.fnsku IS NOT NULL)                     AS bridge_rows_with_fnsku,
  COUNT(*) FILTER (WHERE pim.inventory_source = 'manage_fba_inventory') AS bridge_rows_inventory_source_match,
  COUNT(*) FILTER (
    WHERE pim.match_source = 'manage_fba_inventory_fnsku'
  ) AS bridge_rows_match_source_match
FROM public.product_identifier_map pim
JOIN src ON src.fnsku = btrim(pim.fnsku)
WHERE pim.organization_id = :org;
```

(Replace `amazon_manage_fba_inventory` and the slug strings with whichever
family you triggered: `amazon_fba_inventory` /
`amazon_inbound_performance` / `amazon_amazon_fulfilled_inventory`. For AFI
the FNSKU column is `fulfillment_channel_sku`.)

### B. Confidence safety — no row weakened

```sql
SELECT
  COUNT(*) AS rows_with_fnsku,
  MIN(confidence_score) FILTER (WHERE inventory_source = 'inventory_ledger')         AS min_ledger_conf,
  MIN(confidence_score) FILTER (WHERE match_source = 'listing_catalog')              AS min_listing_conf,
  MAX(confidence_score) FILTER (WHERE inventory_source = 'manage_fba_inventory')     AS max_mfi_conf,
  MAX(confidence_score) FILTER (WHERE inventory_source = 'fba_inventory')            AS max_fbi_conf,
  MAX(confidence_score) FILTER (WHERE inventory_source = 'inbound_performance')      AS max_ip_conf,
  MAX(confidence_score) FILTER (WHERE inventory_source = 'amazon_fulfilled_inventory') AS max_afi_conf
FROM public.product_identifier_map
WHERE organization_id = :org
  AND fnsku IS NOT NULL;
-- Expectation:
--   min_listing_conf  = 1.00
--   min_ledger_conf   >= 0.95
--   max_mfi_conf      <= max(0.92, listing/ledger if upgraded by them)
--   max_fbi_conf      <= 0.90 unless upgraded by stronger source (then >= 0.90)
--   max_ip_conf       <= 0.90 unless upgraded
--   max_afi_conf      <= 0.88 unless upgraded
```

### C. No FNSKU overwrite collision

```sql
-- Detect any bridge row whose FNSKU does not match the FNSKU of any source
-- row claiming the same (seller_sku, asin) tuple — should be 0 unless the
-- legitimate priority matcher returned a tier 2/3/4 fallback.
SELECT COUNT(*) AS suspicious_fnsku_rewrites
FROM public.product_identifier_map pim
JOIN public.amazon_manage_fba_inventory src
  ON src.organization_id = pim.organization_id
 AND nullif(btrim(src.sku),  '') = nullif(btrim(pim.seller_sku), '')
 AND nullif(btrim(src.asin), '') = nullif(btrim(pim.asin), '')
WHERE pim.organization_id = :org
  AND pim.fnsku IS NOT NULL
  AND nullif(btrim(src.fnsku), '') IS NOT NULL
  AND btrim(pim.fnsku) <> btrim(src.fnsku);
```

### D. Unresolved backlog should decrease (when safe candidates exist)

```sql
-- Bridge rows still missing FNSKU after the run.
SELECT
  COUNT(*) FILTER (WHERE fnsku IS NULL) AS bridge_rows_missing_fnsku,
  COUNT(*)                              AS bridge_rows_total
FROM public.product_identifier_map
WHERE organization_id = :org;
-- Re-run before/after the enrichment call to see the delta.
```

### E. Wave-4 operational tables remain untouched

```sql
SELECT
  (SELECT COUNT(*) FROM public.amazon_manage_fba_inventory      WHERE organization_id = :org) AS mfi_rows,
  (SELECT COUNT(*) FROM public.amazon_fba_inventory             WHERE organization_id = :org) AS fbi_rows,
  (SELECT COUNT(*) FROM public.amazon_inbound_performance       WHERE organization_id = :org) AS ip_rows,
  (SELECT COUNT(*) FROM public.amazon_amazon_fulfilled_inventory WHERE organization_id = :org) AS afi_rows;
-- Run before and after the enrichment call — counts must be identical.
```

---

## 5) Manual test steps

Pre-condition: at least one upload per family (from Wave 4 sample files) is
already in `synced` / `complete` / `raw_synced` state.

1. **Baseline snapshot** — run query (D) and (E) above; record both totals.

2. **Trigger enrichment** for each Wave-4 inventory upload:
   ```bash
   curl -sS -X POST http://localhost:3000/api/settings/imports/identity-enrich \
     -H 'content-type: application/json' \
     -d '{ "upload_id": "<UPLOAD_ID>" }' | jq
   ```
   Expected response shape:
   ```json
   {
     "ok": true,
     "kind": "MANAGE_FBA_INVENTORY",
     "source_table": "amazon_manage_fba_inventory",
     "metrics": {
       "rows_scanned": 1234,
       "rows_skipped_no_fnsku": 12,
       "bridge_rows_inserted": 50,
       "bridge_rows_enriched": 200,
       "resolved_by_fnsku": 220,
       "resolved_by_sku_asin": 30,
       "resolved_sku_only": 0,
       "resolved_asin_only": 0,
       "unresolved_ambiguous": 0,
       "unresolved_insert_failed": 0
     }
   }
   ```

3. **Idempotency check** — run the same `curl` again. Response must show
   `bridge_rows_inserted = 0` (or much lower) and `bridge_rows_enriched`
   reflecting `last_seen_at` refresh writes only. **No errors**.

4. **Coverage delta** — re-run query (D). `bridge_rows_missing_fnsku` should
   be **strictly ≤** the baseline. `bridge_rows_total` should be **≥**
   baseline by at least `bridge_rows_inserted`.

5. **Operational rows untouched** — re-run query (E). All four counts
   must equal the baseline (no row deletes, no row inserts).

6. **Cross-family confidence sanity** — run query (B). Verify that
   `inventory_ledger` minimum confidence stays ≥ 0.95 and `listing_catalog`
   stays at 1.00. The four new sources stay capped at their respective
   weights unless stamped on top of an existing stronger row (in which case
   the row keeps the stronger weight).

7. **Report-type guard** — try the route on a non-inventory upload, e.g.
   `FBA_RETURNS`:
   ```bash
   curl -sS -X POST … -d '{ "upload_id": "<FBA_RETURNS_UPLOAD_ID>" }'
   ```
   Expect HTTP 422 with the message
   `Identity enrichment is only supported for the four Wave-4 inventory families…`.

8. **Status guard** — try the route on an upload with status
   `mapped` / `staged`:
   Expect HTTP 409 with the message
   `Upload is not yet synced (current status "...").`

9. **Importer + UI regression** —
   * Re-open the Universal Importer page; confirm the Import Pipeline
     card and history row for these uploads still show **Complete** with
     the Wave-4 5-phase contract (Generic = no-op done). The new route
     does **not** alter `file_processing_status`, so this must be a
     no-op visually.
   * Run query (E) once more after revisiting the page — counts unchanged.

10. **Inventory ledger regression** — upload or re-trigger an
    INVENTORY_LEDGER file via the existing pipeline. Verify the IL Generic
    phase (`completeInventoryLedgerProductIdentifierMapPhase`) still runs
    inline at end of Sync and reports the same `inventory_ledger_*`
    metrics in `raw_report_uploads.metadata`. Wave 5 did not change that
    code path.

---

## 6) Success criteria — mapped to this patch

| Criterion | How met |
|---|---|
| Imported inventory families remain untouched and working | The route only reads from `amazon_manage_fba_inventory`, `amazon_fba_inventory`, `amazon_inbound_performance`, `amazon_amazon_fulfilled_inventory`. No DELETEs, no UPDATEs on those tables. Verified by query (E). |
| `product_identifier_map` enrichment increases safe FNSKU coverage | Inserts new bridge rows from candidate FNSKUs, fills `fnsku` on existing rows when null and not in conflict. Verified by query (D). |
| Unresolved backlog decreases when safe candidates exist | Same — `bridge_rows_missing_fnsku` drops by `bridge_rows_inserted` + safe `enriched` fills. |
| No new duplicate diagnostic tables/views | None introduced. The user-listed views were verified absent and were not created. |
| Generic / importer / UI untouched | The new route is a separate endpoint. No changes to `app/api/settings/imports/{generic,sync,process}/route.ts`, `lib/pipeline/unified-import-pipeline.ts`, or any UI file. |
| No new tables / views / columns / migrations | Confirmed — no migration in this wave. |
| No raw data deletion | The route never issues DELETE. |
| No fake data | Every write is sourced from a non-null FNSKU candidate present on a real operational row. |
| Reused existing structures | `product_identifier_map`, `pickBestProductIdentifierMatch`, `prefetchIdentifierMapCandidatesForBatch`, `pickRawPayloadFields` all reused as-is. |
