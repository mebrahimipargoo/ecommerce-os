from pathlib import Path

CHECK = "\u2713"
path = Path(__file__).resolve().parents[1] / "app/(admin)/imports/UniversalImporter.tsx"
s = path.read_text(encoding="utf-8")
old = (
    "                  {phase === \"syncing\"\n"
    "                    ? `${Math.max(0, Math.min(100, syncPct))}%`\n"
    "                    : removalShipmentUi && removalProgress\n"
    "                      ? `" + CHECK + " archived ${removalProgress.rawRowsWritten.toLocaleString()} shipment line(s), skipped ${removalProgress.rawRowsSkippedExisting.toLocaleString()} already-archived line(s)`\n"
    "                      : \"" + CHECK + " complete\"}"
)
new = (
    "                  {phase === \"syncing\"\n"
    "                    ? `${Math.max(0, Math.min(100, syncPct))}%`\n"
    "                    : removalShipmentUi && removalProgress\n"
    "                      ? `" + CHECK + " archived ${removalProgress.rawRowsWritten.toLocaleString()} shipment line(s), skipped ${removalProgress.rawRowsSkippedExisting.toLocaleString()} already-archived line(s)`\n"
    "                      : listingImportUi && !removalShipmentUi && listingProgress\n"
    "                        ? `" + CHECK + " raw ${listingProgress.phase3Numerator.toLocaleString()} / ${listingProgress.syncDenominator.toLocaleString()} (${listingProgress.rawRowsWritten.toLocaleString()} written, ${listingProgress.rawRowsSkippedExisting.toLocaleString()} skipped)`\n"
    "                      : \"" + CHECK + " complete\"}"
)
if old not in s:
    raise SystemExit("phase3 span not found")
path.write_text(s.replace(old, new, 1), encoding="utf-8")
print("ok")
