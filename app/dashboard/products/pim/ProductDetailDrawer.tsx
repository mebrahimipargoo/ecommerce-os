"use client";

import React, { useCallback, useEffect, useState } from "react";
import { Loader2, X } from "lucide-react";
import { IdentifierValue } from "./IdentifierValue";
import { resolvePimDisplayImageUrl } from "../../../../lib/pim-display-image";

function JsonBlock({ title, value }: { title: string; value: unknown }) {
  const [open, setOpen] = useState(false);
  let text = "";
  try {
    text = JSON.stringify(value ?? null, null, 2);
  } catch {
    text = String(value);
  }
  return (
    <div className="rounded-lg border border-border/60">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="flex w-full items-center justify-between px-3 py-2 text-left text-sm font-medium text-foreground hover:bg-muted/40"
      >
        {title}
        <span className="text-xs text-muted-foreground">{open ? "Hide" : "Show"}</span>
      </button>
      {open ? (
        <pre className="max-h-64 overflow-auto border-t border-border/40 bg-muted/20 p-3 text-[11px] leading-relaxed">{text}</pre>
      ) : null}
    </div>
  );
}

export function ProductDetailDrawer({
  organizationId,
  storeId,
  productId,
  onClose,
  onEdit,
}: {
  organizationId: string;
  storeId: string;
  productId: string | null;
  onClose: () => void;
  onEdit?: (productId: string) => void;
}) {
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [payload, setPayload] = useState<{
    product: Record<string, unknown>;
    product_identifier_map: Record<string, unknown>[];
    product_prices: Record<string, unknown>[];
    catalog_products: Record<string, unknown>[];
  } | null>(null);

  const load = useCallback(async () => {
    if (!productId) return;
    setLoading(true);
    setErr(null);
    try {
      const u = new URL(`/api/dashboard/products/${encodeURIComponent(productId)}`, window.location.origin);
      u.searchParams.set("organization_id", organizationId);
      u.searchParams.set("store_id", storeId);
      const res = await fetch(u.toString());
      const data = (await res.json()) as { ok?: boolean; error?: string; product?: Record<string, unknown> };
      if (!res.ok || !data.ok || !data.product) {
        setErr(data.error ?? "Failed to load product.");
        setPayload(null);
        return;
      }
      const full = data as typeof data & {
        product_identifier_map?: Record<string, unknown>[];
        product_prices?: Record<string, unknown>[];
        catalog_products?: Record<string, unknown>[];
      };
      setPayload({
        product: full.product!,
        product_identifier_map: full.product_identifier_map ?? [],
        product_prices: full.product_prices ?? [],
        catalog_products: full.catalog_products ?? [],
      });
    } catch {
      setErr("Network error.");
      setPayload(null);
    } finally {
      setLoading(false);
    }
  }, [organizationId, storeId, productId]);

  useEffect(() => {
    void load();
  }, [load]);

  if (!productId) return null;

  const p = payload?.product;
  const img = p ? resolvePimDisplayImageUrl(p.main_image_url, p.amazon_raw) : null;

  return (
    <div className="fixed inset-0 z-[400] flex justify-end bg-black/40 p-2 sm:p-4" role="presentation" onClick={onClose}>
      <div
        className="flex h-full w-full max-w-xl flex-col overflow-hidden rounded-2xl border border-border bg-card shadow-2xl"
        role="dialog"
        aria-modal="true"
        aria-labelledby="pim-drawer-title"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between gap-2 border-b border-border px-4 py-3">
          <h2 id="pim-drawer-title" className="text-lg font-semibold text-foreground">
            Product detail
          </h2>
          <div className="flex shrink-0 items-center gap-1">
            {onEdit && productId ? (
              <button
                type="button"
                onClick={() => onEdit(productId)}
                className="rounded-lg border border-border bg-background px-3 py-1.5 text-sm font-medium text-foreground hover:bg-muted"
              >
                Edit
              </button>
            ) : null}
            <button type="button" onClick={onClose} className="rounded-lg p-2 text-muted-foreground hover:bg-muted" aria-label="Close">
              <X className="h-5 w-5" />
            </button>
          </div>
        </div>

        <div className="min-h-0 flex-1 overflow-y-auto p-4">
          {loading ? (
            <div className="flex justify-center py-12">
              <Loader2 className="h-8 w-8 animate-spin text-primary" />
            </div>
          ) : err ? (
            <p className="text-sm text-destructive">{err}</p>
          ) : p ? (
            <div className="space-y-6">
              <div className="flex gap-4">
                <div className="h-24 w-24 shrink-0 overflow-hidden rounded-xl border border-border bg-muted">
                  {img ? (
                    // eslint-disable-next-line @next/next/no-img-element
                    <img src={img} alt="" className="h-full w-full object-cover" />
                  ) : null}
                </div>
                <div className="min-w-0">
                  <p className="text-base font-semibold text-foreground">{String(p.product_name ?? "—")}</p>
                  <p className="mt-1 text-xs text-muted-foreground">ID: {String(p.id)}</p>
                </div>
              </div>

              <section>
                <h3 className="text-sm font-semibold text-foreground">Identity</h3>
                <dl className="mt-2 grid gap-2 text-sm">
                  <div>
                    <dt className="text-muted-foreground">SKU</dt>
                    <dd>
                      <IdentifierValue value={String(p.sku ?? "")} kind="sku" />
                    </dd>
                  </div>
                  <div>
                    <dt className="text-muted-foreground">ASIN</dt>
                    <dd>
                      <IdentifierValue value={String(p.asin ?? "") || null} kind="asin" />
                    </dd>
                  </div>
                  <div>
                    <dt className="text-muted-foreground">FNSKU</dt>
                    <dd>
                      <IdentifierValue value={String(p.fnsku ?? "") || null} kind="fnsku" />
                    </dd>
                  </div>
                  <div>
                    <dt className="text-muted-foreground">UPC</dt>
                    <dd>
                      <IdentifierValue value={String(p.upc_code ?? "") || null} kind="upc" />
                    </dd>
                  </div>
                  <div>
                    <dt className="text-muted-foreground">MPN</dt>
                    <dd className="font-mono text-xs">{String(p.mfg_part_number ?? "—")}</dd>
                  </div>
                </dl>
              </section>

              <section>
                <h3 className="text-sm font-semibold text-foreground">Vendor &amp; category</h3>
                <dl className="mt-2 space-y-1 text-sm">
                  <div>
                    <dt className="text-muted-foreground">Vendor</dt>
                    <dd>{String(p.vendor_name ?? "—")}</dd>
                  </div>
                  <div>
                    <dt className="text-muted-foreground">Brand</dt>
                    <dd>{String(p.brand ?? "—")}</dd>
                  </div>
                </dl>
              </section>

              <section>
                <h3 className="text-sm font-semibold text-foreground">Pricing</h3>
                {(() => {
                  const prices = payload?.product_prices ?? [];
                  const latest = prices[0];
                  const amt = latest?.amount;
                  const cur = typeof latest?.currency === "string" ? latest.currency : "USD";
                  const n = typeof amt === "number" ? amt : typeof amt === "string" ? Number.parseFloat(amt) : Number.NaN;
                  const label = Number.isFinite(n)
                    ? new Intl.NumberFormat(undefined, { style: "currency", currency: cur.length === 3 ? cur : "USD" }).format(n)
                    : "—";
                  return (
                    <>
                      <p className="mt-1 text-sm text-muted-foreground">
                        Latest: <span className="font-medium text-foreground">{label}</span>
                        {latest?.observed_at ? (
                          <span className="text-xs"> ({String(latest.observed_at)})</span>
                        ) : null}
                      </p>
                      <div className="mt-2 max-h-40 overflow-auto rounded-lg border border-border/50">
                        <table className="w-full text-xs">
                          <thead>
                            <tr className="border-b border-border bg-muted/40 text-left">
                              <th className="px-2 py-1">Amount</th>
                              <th className="px-2 py-1">Observed</th>
                              <th className="px-2 py-1">Source</th>
                            </tr>
                          </thead>
                          <tbody>
                            {prices.map((row) => (
                              <tr key={String(row.id)} className="border-b border-border/30">
                                <td className="px-2 py-1 font-mono">{String(row.amount)}</td>
                                <td className="px-2 py-1">{String(row.observed_at ?? "")}</td>
                                <td className="px-2 py-1">{String(row.source ?? "")}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </>
                  );
                })()}
              </section>

              <section>
                <h3 className="text-sm font-semibold text-foreground">Amazon catalog</h3>
                <div className="mt-2 max-h-48 overflow-auto text-xs">
                  {(payload?.catalog_products ?? []).length === 0 ? (
                    <p className="text-muted-foreground">No matching catalog_products rows.</p>
                  ) : (
                    <table className="w-full border-collapse">
                      <thead>
                        <tr className="border-b border-border bg-muted/40 text-left">
                          <th className="px-2 py-1">SKU</th>
                          <th className="px-2 py-1">ASIN</th>
                          <th className="px-2 py-1">Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(payload?.catalog_products ?? []).map((cp) => (
                          <tr key={String(cp.id)} className="border-b border-border/30">
                            <td className="px-2 py-1 font-mono">{String(cp.seller_sku ?? "")}</td>
                            <td className="px-2 py-1 font-mono">{String(cp.asin ?? "")}</td>
                            <td className="px-2 py-1">{String(cp.listing_status ?? "")}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>
              </section>

              <section>
                <h3 className="text-sm font-semibold text-foreground">Source / provenance</h3>
                <ul className="mt-2 space-y-2 text-xs text-muted-foreground">
                  {(payload?.product_identifier_map ?? []).slice(0, 5).map((m) => (
                    <li key={String(m.id)} className="rounded border border-border/40 bg-muted/20 p-2">
                      <div>match_source: {String(m.match_source ?? "—")}</div>
                      <div>source_report_type: {String(m.source_report_type ?? "—")}</div>
                      <div>source_upload_id: {String(m.source_upload_id ?? "—")}</div>
                    </li>
                  ))}
                </ul>
              </section>

              <section className="space-y-2">
                <h3 className="text-sm font-semibold text-foreground">Raw JSON</h3>
                <JsonBlock title="metadata" value={p.metadata} />
                <JsonBlock title="amazon_raw" value={p.amazon_raw} />
                <JsonBlock title="field_provenance" value={p.field_provenance} />
              </section>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
