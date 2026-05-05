/**
 * Whether Amazon SP-API credentials on a `marketplaces.credentials` row look complete
 * for seller LWA refresh (see {@link ../lib/adapters/amazon.ts} `normalizeLwa`).
 */
export function amazonSpCredentialsLookComplete(credentials: unknown): boolean {
  if (!credentials || typeof credentials !== "object") return false;
  const c = credentials as Record<string, string>;
  const lwaId = String(c.lwa_client_id ?? (c as { lwaClientId?: string }).lwaClientId ?? "").trim();
  const lwaSecret = String(
    c.lwa_client_secret ?? (c as { lwaClientSecret?: string }).lwaClientSecret ?? "",
  ).trim();
  const refresh = String(c.refresh_token ?? (c as { refreshToken?: string }).refreshToken ?? "").trim();
  return Boolean(lwaId && lwaSecret && refresh);
}
