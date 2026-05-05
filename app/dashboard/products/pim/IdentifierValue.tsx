"use client";

import React, { useCallback, useState } from "react";
import { Check, Copy, ExternalLink, Search } from "lucide-react";

type IdentifierKind = "asin" | "sku" | "fnsku" | "upc";

function amazonDpUrl(asin: string): string {
  return `https://www.amazon.com/dp/${encodeURIComponent(asin)}`;
}

function amazonSearchUrl(value: string): string {
  return `https://www.amazon.com/s?k=${encodeURIComponent(value)}`;
}

function sellerCentralSkuUrl(sku: string): string | null {
  const tpl = (process.env.NEXT_PUBLIC_AMAZON_SELLER_CENTRAL_SKU_URL ?? "").trim();
  if (!tpl.includes("{value}")) return null;
  return tpl.split("{value}").join(encodeURIComponent(sku));
}

export function IdentifierValue({
  value,
  kind,
  className = "",
}: {
  value: string | null | undefined;
  kind: IdentifierKind;
  className?: string;
}) {
  const v = typeof value === "string" ? value.trim() : "";
  const [copied, setCopied] = useState(false);

  const copy = useCallback(async () => {
    if (!v) return;
    try {
      await navigator.clipboard.writeText(v);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch {
      /* ignore */
    }
  }, [v]);

  if (!v) {
    return <span className={`text-muted-foreground ${className}`}>—</span>;
  }

  const openPrimary =
    kind === "asin"
      ? () => window.open(amazonDpUrl(v), "_blank", "noopener,noreferrer")
      : () => window.open(amazonSearchUrl(v), "_blank", "noopener,noreferrer");

  const scUrl = kind === "sku" || kind === "fnsku" ? sellerCentralSkuUrl(v) : null;

  return (
    <span className={`inline-flex max-w-full items-center gap-0.5 font-mono text-xs ${className}`}>
      <span className="min-w-0 truncate" title={v}>
        {v}
      </span>
      <button
        type="button"
        onClick={() => void copy()}
        className="shrink-0 rounded p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
        title="Copy"
        aria-label="Copy"
      >
        {copied ? <Check className="h-3.5 w-3.5 text-emerald-600" /> : <Copy className="h-3.5 w-3.5" />}
      </button>
      <button
        type="button"
        onClick={openPrimary}
        className="shrink-0 rounded p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
        title={kind === "asin" ? "Open product on Amazon" : "Search on Amazon"}
        aria-label="Open on Amazon"
      >
        {kind === "asin" ? <ExternalLink className="h-3.5 w-3.5" /> : <Search className="h-3.5 w-3.5" />}
      </button>
      {scUrl ? (
        <button
          type="button"
          onClick={() => window.open(scUrl, "_blank", "noopener,noreferrer")}
          className="shrink-0 rounded p-0.5 text-muted-foreground hover:bg-muted hover:text-foreground"
          title="Open in Seller Central (configured URL)"
          aria-label="Seller Central"
        >
          <span className="text-[10px] font-semibold">SC</span>
        </button>
      ) : null}
    </span>
  );
}
