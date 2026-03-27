/**
 * Marketplace product search URL from store platform (amazon / walmart / ebay).
 * Used by Returns & Claim Engine identifier cells and PDFs.
 */
export function marketplaceSearchUrl(
  platformOrMarketplace: string | null | undefined,
  query: string,
): string | null {
  const q = query.trim();
  if (!q) return null;
  const raw = (platformOrMarketplace ?? "").toLowerCase();
  if (raw.includes("walmart")) {
    return `https://www.walmart.com/search?q=${encodeURIComponent(q)}`;
  }
  if (raw.includes("ebay")) {
    return `https://www.ebay.com/sch/i.html?_nkw=${encodeURIComponent(q)}`;
  }
  return `https://www.amazon.com/s?k=${encodeURIComponent(q)}`;
}
