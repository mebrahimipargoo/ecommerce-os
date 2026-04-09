from pathlib import Path
p = Path("backend-python/main.py")
s = p.read_text(encoding="utf8")

s = s.replace(
    """    except Exception as conn_err:
        log.error("[sync-removals] CONNECTION ERROR: %s", conn_err)
        _update_task(task_id, 0, f"Failed to connect to database: {conn_err!s}", status="failed")
        return

    enriched_count = 0""",
    """    except Exception as conn_err:
        log.error("[sync-removals] CONNECTION ERROR: %s", conn_err)
        _update_task(task_id, 0, f"Failed to connect to database: {conn_err!s}", status="failed")
        return

    store_id = _resolve_import_store_id_from_upload(db, organization_id, upload_id)
    if not store_id:
        log.error("[sync-removals] Missing Imports Target Store on raw_report_uploads metadata")
        _update_task(
            task_id,
            0,
            "Imports Target Store missing — set import_store_id or ledger_store_id on the upload metadata.",
            status="failed",
        )
        return
    log.info("[sync-removals] Using store_id=%s", store_id)

    enriched_count = 0""",
)

s = s.replace(
    """            staging_uuid = str(row["id"])
            extracted["source_staging_id"] = staging_uuid

            packed.append((staging_uuid, raw, extracted))""",
    """            staging_uuid = str(row["id"])
            extracted["source_staging_id"] = staging_uuid
            extracted["store_id"] = store_id

            packed.append((staging_uuid, raw, extracted))""",
)

old_hist = """        for staging_id, raw, ext in packed:
            shipment_history_rows.append(
                {
                    "organization_id": organization_id,
                    "upload_id": ext.get("upload_id"),
                    "amazon_staging_id": staging_id,
                    "raw_row": raw,
                },
            )
        if shipment_history_rows:
            _update_task(task_id, 20, f"Archiving {len(shipment_history_rows)} raw shipment rows...")
            for i in range(0, len(shipment_history_rows), STAGING_INSERT_BATCH):
                chunk = shipment_history_rows[i : i + STAGING_INSERT_BATCH]
                db.table("amazon_removal_shipments").insert(chunk).execute()"""

new_hist = """        for staging_id, raw, ext in packed:
            row_ship: dict[str, Any] = {
                "organization_id": organization_id,
                "upload_id": ext.get("upload_id"),
                "amazon_staging_id": staging_id,
                "store_id": store_id,
                "raw_row": raw,
                "order_id": ext.get("order_id"),
                "sku": ext.get("sku"),
                "fnsku": ext.get("fnsku"),
                "disposition": ext.get("disposition"),
                "tracking_number": ext.get("tracking_number"),
                "carrier": ext.get("carrier"),
                "shipment_date": ext.get("shipment_date"),
                "order_date": ext.get("order_date"),
                "order_type": ext.get("order_type"),
                "requested_quantity": ext.get("requested_quantity"),
                "shipped_quantity": ext.get("shipped_quantity"),
                "disposed_quantity": ext.get("disposed_quantity"),
                "cancelled_quantity": ext.get("cancelled_quantity"),
            }
            shipment_history_rows.append(row_ship)
        if shipment_history_rows:
            _update_task(task_id, 20, f"Archiving {len(shipment_history_rows)} raw shipment rows...")
            for i in range(0, len(shipment_history_rows), STAGING_INSERT_BATCH):
                chunk = shipment_history_rows[i : i + STAGING_INSERT_BATCH]
                db.table("amazon_removal_shipments").upsert(
                    chunk,
                    on_conflict="organization_id,upload_id,amazon_staging_id",
                ).execute()"""

if old_hist in s:
    s = s.replace(old_hist, new_hist)
else:
    raise SystemExit("hist block not found")

s = s.replace(
    """                existing_null_rows: list[dict[str, Any]] = (
                    db.table("amazon_removals")
                    .select("*")
                    .eq("organization_id", organization_id)
                    .is_("tracking_number", "null")
                    .execute()
                    .data or []
                )""",
    """                existing_null_rows: list[dict[str, Any]] = (
                    db.table("amazon_removals")
                    .select("*")
                    .eq("organization_id", organization_id)
                    .eq("store_id", store_id)
                    .is_("tracking_number", "null")
                    .execute()
                    .data or []
                )""",
)

s = s.replace(
    """def _merge_shipment_into_null_slot(existing: dict[str, Any], shipment: dict[str, Any]) -> dict[str, Any]:
    \"\"\"Backfill tracking/carrier/shipment_date only — never overwrite removal order quantities.\"\"\"
    out = dict(existing)
    tn = _pg_text_unique_field(shipment.get("tracking_number"))
    if tn:
        out["tracking_number"] = tn
    for col in ("carrier", "shipment_date"):
        if col in shipment and shipment.get(col) is not None:
            out[col] = shipment[col]
    return out""",
    """def _merge_shipment_into_null_slot(existing: dict[str, Any], shipment: dict[str, Any]) -> dict[str, Any]:
    \"\"\"Backfill tracking; fill carrier/shipment_date only when existing is empty (Wave 1).\"\"\"
    out = dict(existing)
    tn = _pg_text_unique_field(shipment.get("tracking_number"))
    if tn:
        out["tracking_number"] = tn
    inc_c = shipment.get("carrier")
    if inc_c is not None and str(inc_c).strip() != "":
        if not _pg_text_unique_field(existing.get("carrier")):
            out["carrier"] = inc_c
    inc_sd = shipment.get("shipment_date")
    if inc_sd is not None and str(inc_sd).strip() != "":
        if existing.get("shipment_date") is None or str(existing.get("shipment_date")).strip() == "":
            out["shipment_date"] = inc_sd
    return out""",
)

p.write_text(s, encoding="utf8")
print("main pass2 ok")
