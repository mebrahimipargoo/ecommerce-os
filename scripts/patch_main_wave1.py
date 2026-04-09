"""Apply Wave 1 patches to backend-python/main.py"""
from pathlib import Path
p = Path("backend-python/main.py")
s = p.read_text(encoding="utf8")
if "def _resolve_import_store_id_from_upload" not in s:
    anchor = "def _extract_removal_row(\n"
    helper = '''def _resolve_import_store_id_from_upload(db: Any, organization_id: str, upload_id: str | None) -> str | None:
    """Reads import_store_id / ledger_store_id from raw_report_uploads.metadata."""
    if not upload_id:
        return None
    try:
        res = (
            db.table("raw_report_uploads")
            .select("metadata")
            .eq("id", upload_id)
            .eq("organization_id", organization_id)
            .limit(1)
            .execute()
        )
        row = (res.data or [None])[0] if isinstance(res.data, list) else res.data
        meta = row.get("metadata") if isinstance(row, dict) else None
        if not isinstance(meta, dict):
            return None
        raw = meta.get("import_store_id") or meta.get("ledger_store_id")
        if not raw:
            return None
        sid = str(raw).strip()
        uuid.UUID(sid)
        return sid
    except Exception:
        return None


'''
    s = s.replace(anchor, helper + anchor)

s = s.replace(
    '_REMOVALS_CONFLICT = "organization_id,upload_id,source_staging_id"',
    '_REMOVALS_CONFLICT = "organization_id,store_id,order_id,sku,fnsku,disposition,requested_quantity,shipped_quantity,disposed_quantity,cancelled_quantity,order_date,order_type"',
)
s = s.replace(
    '"source_staging_id",\n}',
    '"source_staging_id",\n    "store_id",\n}',
)

s = s.replace(
    '_EXPECTED_PKG_CONFLICT = "organization_id,upload_id,source_staging_id"',
    '_EXPECTED_PKG_CONFLICT = "organization_id,store_id,order_id,sku,fnsku,disposition,requested_quantity,shipped_quantity,disposed_quantity,cancelled_quantity,order_date"',
)

if '"store_id"' not in s.split("EXPECTED_PKG_AMAZON_COLS", 1)[1][:400]:
    s = s.replace(
        'EXPECTED_PKG_AMAZON_COLS = frozenset({\n    "organization_id",\n    "upload_id",\n    "source_staging_id",',
        'EXPECTED_PKG_AMAZON_COLS = frozenset({\n    "organization_id",\n    "store_id",\n    "upload_id",\n    "source_staging_id",\n    "order_type",',
    )

p.write_text(s, encoding="utf8")
print("main.py pass1")
