# One-off patch: UniversalImporter Phase 2 listing labels + FPS-backed counts
from pathlib import Path

CHECK = "\u2713"
path = Path(__file__).resolve().parents[1] / "app/(admin)/imports/UniversalImporter.tsx"
s = path.read_text(encoding="utf-8")

old1 = (
 "                <span\n"
    "                  title={removalShipmentUi ? REMOVAL_SHIPMENT_UI_LABELS.phase2Subtitle : undefined}\n"
    "                >\n"
    "                  {removalShipmentUi\n"
    "                    ? REMOVAL_SHIPMENT_UI_LABELS.phase2Title\n"
    "                    : listingImportUi\n"
    "                      ? phase === \"processing\"\n"
    "                        ? \"Phase 2 — Stage to amazon_staging\"\n"
    f"                        : \"{CHECK} Phase 2 — amazon_staging\"\n"
    "                      : phaseLabel\n"
    "                        ? `${phaseLabel} — staging`\n"
    "                        : \"Phase 2 — Stage to amazon_staging\"}\n"
    "                </span>"
)

new1 = (
    "                <span\n"
    "                  title={\n"
    "                    removalShipmentUi\n"
    "                      ? REMOVAL_SHIPMENT_UI_LABELS.phase2Subtitle\n"
    "                      : listingImportUi\n"
    "                        ? LISTING_IMPORT_UI_LABELS.phase2Subtitle\n"
    "                        : undefined\n"
    "                  }\n"
    "                >\n"
    "                  {removalShipmentUi\n"
    "                    ? REMOVAL_SHIPMENT_UI_LABELS.phase2Title\n"
    "                    : listingImportUi\n"
    "                      ? phase === \"processing\"\n"
    "                        ? LISTING_IMPORT_UI_LABELS.phase2Title\n"
    f"                        : `{CHECK} ${'{'}LISTING_IMPORT_UI_LABELS.phase2Title{'}'}`\n"
    "                      : phaseLabel\n"
    "                        ? `${phaseLabel} — staging`\n"
    "                        : \"Phase 2 — Stage to amazon_staging\"}\n"
    "                </span>"
)

# Fix the f-string line - template literal in TS should be `� ${LISTING_IMPORT_UI_LABELS.phase2Title}`
new1 = (
    "                <span\n"
    "                  title={\n"
    "                    removalShipmentUi\n"
    "                      ? REMOVAL_SHIPMENT_UI_LABELS.phase2Subtitle\n"
    "                      : listingImportUi\n"
    "                        ? LISTING_IMPORT_UI_LABELS.phase2Subtitle\n"
    "                        : undefined\n"
    "                  }\n"
    "                >\n"
    "                  {removalShipmentUi\n"
    "                    ? REMOVAL_SHIPMENT_UI_LABELS.phase2Title\n"
    "                    : listingImportUi\n"
    "                      ? phase === \"processing\"\n"
    "                        ? LISTING_IMPORT_UI_LABELS.phase2Title\n"
    f"                        : `{CHECK} ${{LISTING_IMPORT_UI_LABELS.phase2Title}}`\n"
    "                      : phaseLabel\n"
    "                        ? `${phaseLabel} — staging`\n"
    "                        : \"Phase 2 — Stage to amazon_staging\"}\n"
    "                </span>"
)

old2 = (
    f"                      : `{CHECK} ${{removalProgress.stagedRowsWritten.toLocaleString()}} / "
    f"${{removalProgress.dataRowsTotal.toLocaleString()}} row(s)`\n"
    "                    : phase === \"processing\"\n"
    "                      ? totalRows > 0"
)

new2 = (
    f"                      : `{CHECK} ${{removalProgress.stagedRowsWritten.toLocaleString()}} / "
    f"${{removalProgress.dataRowsTotal.toLocaleString()}} row(s)`\n"
    "                    : listingImportUi && !removalShipmentUi && listingProgress\n"
    "                      ? phase === \"processing\"\n"
    "                        ? listingProgress.dataRowsTotal > 0 || listingProgress.stagedRowsWritten > 0\n"
    "                          ? `${listingProgress.stagedRowsWritten.toLocaleString()} / "
    "${listingProgress.dataRowsTotal.toLocaleString()} (${listingProgress.phase2Pct}%)`\n"
    "                          : totalRows > 0\n"
    "                            ? `${processedRows.toLocaleString()} / ${totalRows.toLocaleString()} "
    "rows (${processPct}%)`\n"
    "                            : processPct > 0\n"
    "                              ? `${processPct}%`\n"
    "                              : \"running…\"\n"
    f"                        : `{CHECK} ${{listingProgress.stagedRowsWritten.toLocaleString()}} / "
    "${Math.max(listingProgress.dataRowsTotal, 1).toLocaleString()} row(s)`\n"
    "                    : phase === \"processing\"\n"
    "                      ? totalRows > 0"
)

if old1 not in s:
    raise SystemExit("block1 not found")
if old2 not in s:
    raise SystemExit("block2 not found")
s = s.replace(old1, new1, 1).replace(old2, new2, 1)
path.write_text(s, encoding="utf-8")
print("patched", path)
