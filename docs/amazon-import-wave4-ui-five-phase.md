# Wave 4 — Universal Importer UI (5-phase consistency)

UI-only patch that makes the **top pipeline card** (Universal Importer) and
the **Import History row pipeline** show the same five-phase view for the
inventory family:

```
1) Upload  →  2) Map & classify  →  3) Process — staging  →  4) Sync  →  5) Generic
```

Affected report families:

* `INVENTORY_LEDGER`
* `MANAGE_FBA_INVENTORY`
* `FBA_INVENTORY`
* `INBOUND_PERFORMANCE`
* `AMAZON_FULFILLED_INVENTORY`

For the four reports whose backend declares `supports_generic = false`,
Generic is rendered as **done · no-op** once Sync completes — never as
"pending forever" and never with a Generic button.

For `INVENTORY_LEDGER` (whose Generic does run inline at the end of Sync),
the existing real-row counters remain.

Other report families (`FBA_RETURNS`, `SAFET_CLAIMS`, `ALL_LISTINGS`,
`ACTIVE_LISTINGS`, `CATEGORY_LISTINGS`, `REMOVAL_*`, …) are deliberately
untouched by this pass.

---

## 1) Changed files

| File | Change |
| --- | --- |
| `lib/pipeline/unified-import-pipeline.ts` | (a) New exported set `FIVE_PHASE_INVENTORY_FAMILY` enumerating the five inventory kinds. (b) Generic step is **always pushed** to `steps[]` for those kinds, even when `supports_generic = false`. (c) Generic step now derives `tone`, `pct`, `subtitle`, and `rightLabel` from a `noOpGenericForInventory` branch — `pending → "auto-completes after Sync"` while Sync runs, then `done → "no-op"` once Phase 3 completes. |

No other UI files needed editing — both consumers already render every
non-`skipped` step from this central model:

* `app/(admin)/imports/UniversalImporter.tsx` — top pipeline card uses
  `topPipeline.steps.filter((s) => s.tone !== "skipped")`.
* `app/(admin)/imports/RawReportImportsPanel.tsx` — `PipelineCell` uses the
  identical filter on the same `pipeline.steps` array.

The `nextAction` logic is also untouched: because `needsP4 = false` for the
no-op families, no "Generic" button is ever surfaced for them in either UI
surface. Process / Sync / Worklist buttons continue to gate on the existing
backend signals.

---

## 2) Progress contract normalisation notes

Both surfaces consume the **same** `UnifiedPipelineModel` produced from the
same backend inputs:

| Source field | Drives |
| --- | --- |
| `raw_report_uploads.status` | high-level state (uploading / mapped / staged / processing / synced / failed). |
| `raw_report_uploads.metadata.*` | `failed_phase`, `etl_phase`, `worklist_progress`, `worklist_completed`, `catalog_listing_import_phase`, `total_bytes`, etc. |
| `file_processing_status.phase{1..4}_status` | per-step `done` / `running` / `pending` derivation. |
| `file_processing_status.phase{1..4}_*_pct` | real percentage when present (no fake interpolation). |
| `file_processing_status.staged_rows_written` / `raw_rows_written` / `raw_rows_skipped_existing` / `generic_rows_written` / `rows_eligible_for_generic` / `duplicate_rows_skipped` | row counters surfaced verbatim in `rightLabel` and `rowMetricsLine`. |
| `AMAZON_REPORT_REGISTRY[kind].supports_generic` | whether Generic is a real phase or a no-op. |
| `FIVE_PHASE_INVENTORY_FAMILY.has(kind)` *(new)* | forces the 5th step to render even when Generic is no-op. |

Per-step rules with this patch (Generic only — every other step is unchanged):

```
fivePhaseInventoryFamily = kind ∈ {INVENTORY_LEDGER, MANAGE_FBA_INVENTORY,
                                    FBA_INVENTORY, INBOUND_PERFORMANCE,
                                    AMAZON_FULFILLED_INVENTORY}
noOpGenericForInventory = fivePhaseInventoryFamily AND supports_generic === false

Generic step:
  if noOpGenericForInventory:
    tone        = phase3Complete ? "done"  : "pending"
    pct         = phase3Complete ? 100     : 0
    subtitle    = "Not required — no enrichment for this report"
    rightLabel  = phase3Complete ? "no-op" : "auto-completes after Sync"
  else if !supports_generic:
    tone="skipped", rightLabel="N/A"      ← unchanged for non-inventory families
  else:
    tone/pct/rightLabel from phase4_* + canonical row counts (unchanged)
```

Buttons: `nextAction` is **never** set to `"generic"` for the four no-op
families (the `needsP4` guard already prevents it). For `INVENTORY_LEDGER`
Generic auto-runs inline at end of Sync, so the standalone Generic button
also never surfaces.

The overall pipeline percentage is the average of all non-skipped steps,
so a 5-phase no-op pipeline averages 5 phases evenly (no phantom Phase 5
weight skewing the bar).

---

## 3) Manual test checklist

Use the four sample CSVs (one per family). For each upload, verify the
behaviours below in **both** the top pipeline card on the Universal
Importer page **and** the corresponding row in the Import History panel on
that same page. They must match step-for-step at every checkpoint.

### A. `MANAGE_FBA_INVENTORY` (no-op Generic)

1. Drop the file → top card shows step 1 active, history row shows row.
2. After upload + AI classify → steps 1–2 done; type column shows
   `MANAGE_FBA_INVENTORY`; **step 5 (Generic)** visible with subtitle
   "Not required — no enrichment for this report" and right label
   `auto-completes after Sync`. No Generic button.
3. Click **Process** → step 3 active with real `staged_rows_written /
   data_rows_total · n%`. No phantom 100%.
4. Click **Sync** → step 4 active with real
   `raw_rows_written / staged_rows_written · n%`.
5. Once Sync finishes → top card status badge = `Complete`; step 5 flips
   to **done**, right label `no-op`. No Generic button. Reset / Delete
   buttons remain usable. History row mirrors all five steps identically.

### B. `FBA_INVENTORY` (no-op Generic)
Same checklist as A.

### C. `INBOUND_PERFORMANCE` (no-op Generic)
Same checklist as A.

### D. `AMAZON_FULFILLED_INVENTORY` (no-op Generic)
Same checklist as A. Additionally confirm:

* Type column reads `AMAZON_FULFILLED_INVENTORY` (rule-based detection,
  not GPT, and **not** `ALL_LISTINGS` / `ACTIVE_LISTINGS`).
* No catalog products are touched (this is the fundamental "do not collapse
  into a listing export" guarantee).

### E. `INVENTORY_LEDGER` regression

1. Upload a known IL file.
2. Process and Sync as usual.
3. Step 5 (Generic) shows the existing `inventory_ledger_generic_map_upserts`
   counts — for example `1,234 rows`. The right-label string contains real
   counts, not "no-op" (because `supports_generic = true` for IL — we did
   not change its behaviour).
4. Status badge ends on `Complete`; both surfaces match step-for-step.

### F. Other-family regression spot-check

Re-upload one each of: `FBA_RETURNS`, `ALL_LISTINGS`, `REMOVAL_SHIPMENT`,
`SETTLEMENT`. Confirm:

* `FBA_RETURNS` / `SETTLEMENT` continue to show the original 4-step pipeline
  (no Generic step in either UI surface) — they are not in the inventory
  five-phase set.
* `ALL_LISTINGS` continues to render its existing Generic catalog step with
  real row counts.
* `REMOVAL_SHIPMENT` continues to show its existing Generic step + Generic
  button.

### G. Cross-surface consistency

For every test file above, a single visual diff between the top card and
the history row pipeline must show **identical**:

* number of visible steps,
* per-step tone (pending / active / done / failed),
* per-step right label (rows / percentage / no-op).

If they ever disagree the bug is in the central model
(`buildUnifiedPipeline`), not in either surface.
