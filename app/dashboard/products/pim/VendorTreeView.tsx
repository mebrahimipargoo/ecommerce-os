"use client";

import React, { useMemo, useState } from "react";
import { ChevronDown, ChevronRight, Loader2 } from "lucide-react";
import { IdentifierValue } from "./IdentifierValue";
import { resolvePimDisplayImageUrl } from "../../../../lib/pim-display-image";
import type { PimCatalogRow } from "./CatalogDataGrid";

export type VendorAggRow = {
  id: string;
  name: string;
  product_count: number;
  missing_asin: number;
  missing_image: number;
  missing_category: number;
  active_status_count: number;
};

export function VendorTreeView({
  vendors,
  productsByVendor,
  loading = false,
  treeSearch,
  onTreeSearchChange,
  onOpenProduct,
  onViewVendorInGrid,
  onVendorOpened,
}: {
  vendors: VendorAggRow[];
  productsByVendor: Map<string, PimCatalogRow[]>;
  loading?: boolean;
  treeSearch: string;
  onTreeSearchChange: (v: string) => void;
  onOpenProduct: (id: string) => void;
  onViewVendorInGrid: (vendorId: string) => void;
  onVendorOpened?: (vendorId: string) => void;
}) {
  const [open, setOpen] = useState<Set<string>>(() => new Set());
  const q = treeSearch.trim().toLowerCase();

  const filteredVendors = useMemo(() => {
    if (!q) return vendors;
    return vendors.filter((v) => {
      if (v.name.toLowerCase().includes(q)) return true;
      const prows = productsByVendor.get(v.id) ?? [];
      return prows.some((p) => String(p.product_name ?? "").toLowerCase().includes(q) || displaySku(p).toLowerCase().includes(q));
    });
  }, [vendors, productsByVendor, q]);

  function displaySku(row: PimCatalogRow): string {
    const s = typeof row.sku === "string" ? row.sku.trim() : "";
    if (s) return s;
    return typeof row.map_seller_sku === "string" ? row.map_seller_sku.trim() : "";
  }

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-end gap-3">
        <label className="block text-sm">
          <span className="text-muted-foreground">Search in tree</span>
          <input
            value={treeSearch}
            onChange={(e) => onTreeSearchChange(e.target.value)}
            placeholder="Vendor or product…"
            className="mt-1 h-10 w-72 max-w-full rounded-lg border border-border bg-background px-3 text-sm"
          />
        </label>
      </div>

      {loading ? (
        <div className="flex justify-center py-16 text-muted-foreground">
          <Loader2 className="h-8 w-8 animate-spin" />
        </div>
      ) : (
        <ul className="space-y-1 rounded-xl border border-border/60 bg-card/40 p-2">
          {filteredVendors.map((v) => {
            const expanded = open.has(v.id);
            const prows = (productsByVendor.get(v.id) ?? []).filter((p) => {
              if (!q) return true;
              return (
                String(p.product_name ?? "").toLowerCase().includes(q) ||
                displaySku(p).toLowerCase().includes(q) ||
                v.name.toLowerCase().includes(q)
              );
            });
            return (
              <li key={v.id} className="rounded-lg border border-border/40 bg-background/60">
                <div className="flex flex-wrap items-center gap-2 px-2 py-2">
                  <button
                    type="button"
                    onClick={() => {
                      setOpen((prev) => {
                        const n = new Set(prev);
                        const willOpen = !n.has(v.id);
                        if (willOpen) {
                          n.add(v.id);
                          window.setTimeout(() => onVendorOpened?.(v.id), 0);
                        } else {
                          n.delete(v.id);
                        }
                        return n;
                      });
                    }}
                    className="inline-flex items-center gap-1 text-left font-medium text-foreground"
                  >
                    {expanded ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
                    {v.name}
                  </button>
                  <span className="text-xs text-muted-foreground">
                    {v.product_count} total · {v.missing_asin} no ASIN · {v.missing_image} no image · {v.missing_category} no category ·{" "}
                    {v.active_status_count} active status
                  </span>
                  <button
                    type="button"
                    onClick={() => onViewVendorInGrid(v.id)}
                    className="ml-auto text-xs font-medium text-primary underline-offset-2 hover:underline"
                  >
                    Filter grid
                  </button>
                </div>
                {expanded ? (
                  <ul className="border-t border-border/40 px-2 py-2">
                    {prows.length === 0 ? (
                      <li className="px-2 py-2 text-sm text-muted-foreground">No products loaded for this vendor.</li>
                    ) : (
                      prows.map((p) => {
                        const pid = String(p.id ?? "");
                        const img = resolvePimDisplayImageUrl(p.main_image_url, p.amazon_raw);
                        const name = String(p.product_name ?? "—");
                        return (
                          <li key={pid} className="flex flex-wrap items-center gap-2 border-b border-border/30 py-2 last:border-0">
                            <div className="h-8 w-8 shrink-0 overflow-hidden rounded border border-border bg-muted">
                              {img ? (
                                // eslint-disable-next-line @next/next/no-img-element
                                <img src={img} alt="" className="h-full w-full object-cover" />
                              ) : null}
                            </div>
                            <span className="min-w-0 flex-1 truncate text-sm font-medium">{name}</span>
                            <span className="min-w-[120px]">
                              <IdentifierValue value={displaySku(p)} kind="sku" />
                            </span>
                            <button
                              type="button"
                              onClick={() => onOpenProduct(pid)}
                              className="rounded border border-border px-2 py-0.5 text-xs hover:bg-muted"
                            >
                              Details
                            </button>
                          </li>
                        );
                      })
                    )}
                  </ul>
                ) : null}
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}
