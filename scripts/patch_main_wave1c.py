from pathlib import Path
p = Path("backend-python/main.py")
s = p.read_text(encoding="utf8")

s = s.replace(
    '''def _expected_pkg_identity_key(row: dict[str, Any]) -> tuple[Any, ...]:
    """Matches uq_expected_packages_source_staging_line when source_staging_id is set."""
    sid = row.get("source_staging_id")
    if sid is not None and str(sid).strip() != "":
        return (
            str(row.get("organization_id", "")),
            str(row.get("upload_id", "")),
            str(sid).strip(),
        )
    # Legacy rows without source_staging_id: in-memory merge key only (no DB uniqueness).
    return (
        str(row.get("organization_id", "")),
        _pg_text_unique_field(row.get("order_id")),
        _pg_text_unique_field(row.get("sku")),
        _pg_text_unique_field(row.get("fnsku")),
        _pg_text_unique_field(row.get("disposition")),
        _safe_int(row.get("requested_quantity")),
        _safe_int(row.get("shipped_quantity")),
        _safe_int(row.get("disposed_quantity")),
        _safe_int(row.get("cancelled_quantity")),
        _removal_order_date_key(row.get("order_date")),
    )''',
    '''def _expected_pkg_identity_key(row: dict[str, Any]) -> tuple[Any, ...]:
    """Wave 1: prefer business key when store_id is set (matches uq_expected_packages_business_line)."""
    st = row.get("store_id")
    if st is not None and str(st).strip() != "":
        return (
            str(row.get("organization_id", "")),
            str(st).strip(),
            _pg_text_unique_field(row.get("order_id")),
            _pg_text_unique_field(row.get("sku")),
            _pg_text_unique_field(row.get("fnsku")),
            _pg_text_unique_field(row.get("disposition")),
            _safe_int(row.get("requested_quantity")),
            _safe_int(row.get("shipped_quantity")),
            _safe_int(row.get("disposed_quantity")),
            _safe_int(row.get("cancelled_quantity")),
            _removal_order_date_key(row.get("order_date")),
        )
    sid = row.get("source_staging_id")
    if sid is not None and str(sid).strip() != "":
        return (
            str(row.get("organization_id", "")),
            str(row.get("upload_id", "")),
            str(sid).strip(),
        )
    return (
        str(row.get("organization_id", "")),
        _pg_text_unique_field(row.get("order_id")),
        _pg_text_unique_field(row.get("sku")),
        _pg_text_unique_field(row.get("fnsku")),
        _pg_text_unique_field(row.get("disposition")),
        _safe_int(row.get("requested_quantity")),
        _safe_int(row.get("shipped_quantity")),
        _safe_int(row.get("disposed_quantity")),
        _safe_int(row.get("cancelled_quantity")),
        _removal_order_date_key(row.get("order_date")),
    )''',
)

anchor = '        log.info(\n            "[sync-removals] DONE — upserted=%d updated_in_place=%d staging_skipped=%d",'
if "wave1_reconciliation" not in s:
    s = s.replace(
        anchor,
        '''        log.info(
            "[sync-removals] wave1_reconciliation %s",
            json.dumps(
                {
                    "store_id": store_id,
                    "staging_lines": staged_line_count,
                    "upsert_chunks_rows": upserted_total,
                    "enriched_in_place": enriched_count,
                    "skipped_staging": skipped,
                },
                default=str,
            ),
        )
''' + anchor,
    )

p.write_text(s, encoding="utf8")
print("main pass3")
