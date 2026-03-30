/**
 * Ensures tenant logos load in the browser: relative `/storage/...` paths become absolute
 * using `NEXT_PUBLIC_SUPABASE_URL` when needed.
 */
export function normalizeTenantLogoUrl(raw: string | null | undefined): string {
  const u = (raw ?? "").trim();
  if (!u) return "";
  if (/^https?:\/\//i.test(u)) return u;
  if (u.startsWith("//")) return `https:${u}`;
  if (u.startsWith("/")) {
    const base =
      typeof process !== "undefined" && process.env?.NEXT_PUBLIC_SUPABASE_URL
        ? String(process.env.NEXT_PUBLIC_SUPABASE_URL).replace(/\/$/, "")
        : "";
    if (base && (u.includes("/storage/") || u.includes("/object/"))) {
      return `${base}${u}`;
    }
    return u;
  }
  return u;
}
