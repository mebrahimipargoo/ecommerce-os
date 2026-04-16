from pathlib import Path

CHECK = "\u2713"
path = Path(__file__).resolve().parents[1] / "app/(admin)/imports/UniversalImporter.tsx"
s = path.read_text(encoding="utf-8")

old = """            <div>
              <div className=\"mb-1 flex justify-between text-[11px] font-medium text-muted-foreground\">
                <span title={REMOVAL_SHIPMENT_UI_LABELS.phase4Subtitle}>
                  {REMOVAL_SHIPMENT_UI_LABELS.phase4Title}
                </span>
                <span
                  className={
                    phase === \"genericing\"
                      ? \"tabular-nums\"
                      : topUi && !topUi.phase4Complete
                        ? \"text-muted-foreground\"
                        : \"text-emerald-500\"
                  }
                >
                  {removalProgress
                    ? phase === \"genericing\"
                      ? `${removalProgress.genericRowsWritten.toLocaleString()} / ${removalProgress.genericEligible.toLocaleString()} (${removalProgress.phase4Pct}%)`
                      : topUi?.phase4Complete
                        ? `" + CHECK + """ ${removalProgress.genericRowsWritten.toLocaleString()} / ${removalProgress.genericEligible.toLocaleString()} enriched`
                        : `pending — ${removalProgress.genericRowsWritten.toLocaleString()} / ${removalProgress.genericEligible.toLocaleString()}`
                    : phase === \"genericing\"
                      ? \"…\"
                      : topUi?.phase4Complete
                        ? \"""" + CHECK + """ complete\"
                        : \"pending — run Generic\"}
                </span>
              </div>
              <div className=\"relative h-2 overflow-hidden rounded-full bg-muted\">
                {phase === \"genericing\" && (
                  <div className=\"absolute inset-0 animate-pulse rounded-full bg-violet-400/50\" />
                )}
                <div
                  className=\"h-full rounded-full bg-violet-500 transition-all duration-500\"
                  style={{
                    width: `${
                      removalProgress
                        ? phase === \"genericing\"
                          ? Math.max(5, removalProgress.phase4Pct)
                          : topUi?.phase4Complete
                            ? 100
                            : Math.max(10, removalProgress.phase4Pct)
                        : phase === \"genericing\"
                          ? 45
                          : topUi?.phase4Complete
                            ? 100
                            : 12
                    }%`,
                  }}
                />
              </div>
            </div>"""

new = """            <div>
              <div className=\"mb-1 flex justify-between text-[11px] font-medium text-muted-foreground\">
                <span
                  title={
                    removalShipmentUi
                      ? REMOVAL_SHIPMENT_UI_LABELS.phase4Subtitle
                      : LISTING_IMPORT_UI_LABELS.phase4Subtitle
                  }
                >
                  {removalShipmentUi
                    ? REMOVAL_SHIPMENT_UI_LABELS.phase4Title
                    : LISTING_IMPORT_UI_LABELS.phase4Title}
                </span>
                <span
                  className={
                    phase === \"genericing\"
                      ? \"tabular-nums\"
                      : topUi && !topUi.phase4Complete
                        ? \"text-muted-foreground\"
                        : \"text-emerald-500\"
                  }
                >
                  {removalShipmentUi && removalProgress
                    ? phase === \"genericing\"
                      ? `${removalProgress.genericRowsWritten.toLocaleString()} / ${removalProgress.genericEligible.toLocaleString()} (${removalProgress.phase4Pct}%)`
                      : topUi?.phase4Complete
                        ? `" + CHECK + """ ${removalProgress.genericRowsWritten.toLocaleString()} / ${removalProgress.genericEligible.toLocaleString()} enriched`
                        : `pending — ${removalProgress.genericRowsWritten.toLocaleString()} / ${removalProgress.genericEligible.toLocaleString()}`
                    : listingImportUi && !removalShipmentUi && listingProgress
                      ? phase === \"genericing\"
                        ? `${listingProgress.phase4Numerator.toLocaleString()} / ${listingProgress.catalogEligible.toLocaleString()} (${listingProgress.phase4Pct}%)`
                        : topUi?.phase4Complete
                          ? `" + CHECK + """ ${listingProgress.catalogRowsNew.toLocaleString()} new · ${listingProgress.catalogRowsUpdated.toLocaleString()} updated · ${listingProgress.catalogRowsUnchanged.toLocaleString()} unchanged / ${listingProgress.catalogEligible.toLocaleString()} eligible`
                          : `pending — run Generic (catalog) — ${listingProgress.phase4Numerator.toLocaleString()} / ${listingProgress.catalogEligible.toLocaleString()}`
                    : phase === \"genericing\"
                      ? \"…\"
                      : topUi?.phase4Complete
                        ? \"""" + CHECK + """ complete\"
                        : \"pending — run Generic\"}
                </span>
              </div>
              <div className=\"relative h-2 overflow-hidden rounded-full bg-muted\">
                {phase === \"genericing\" && (
                  <div className=\"absolute inset-0 animate-pulse rounded-full bg-violet-400/50\" />
                )}
                <div
                  className=\"h-full rounded-full bg-violet-500 transition-all duration-500\"
                  style={{
                    width: `${
                      removalShipmentUi && removalProgress
                        ? phase === \"genericing\"
                          ? Math.max(5, removalProgress.phase4Pct)
                          : topUi?.phase4Complete
                            ? 100
                            : Math.max(10, removalProgress.phase4Pct)
                        : listingImportUi && !removalShipmentUi && listingProgress
                          ? phase === \"genericing\"
                            ? Math.max(5, listingProgress.phase4Pct)
                            : topUi?.phase4Complete
                              ? 100
                              : Math.max(10, listingProgress.phase4Pct)
 : phase === \"genericing\"
                            ? 45
                            : topUi?.phase4Complete
                              ? 100
                              : 12
                    }%`,
                  }}
                />
              </div>
            </div>"""

if old not in s:
    raise SystemExit("phase4 block not found")
path.write_text(s.replace(old, new, 1), encoding="utf-8")
print("ok")
