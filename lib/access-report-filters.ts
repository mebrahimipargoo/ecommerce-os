/**
 * Report tab only: high-level “functional areas” (not every `modules` row or sidebar leaf).
 * Access Management User/Role/Group/Settings tabs keep the full feature tree; this is for reporting only.
 */

export const REPORT_MODULE_GROUP_CHOICES: { value: string; label: string; hint: string }[] = [
  { value: "operations", label: "Operations", hint: "Catalog `operations` module (e.g. inventory and other ops features)." },
  { value: "warehouse", label: "Warehouse", hint: "WMS / scanner (`wms` module)." },
  { value: "claims", label: "Claims", hint: "`claims` module (claim engine, report history, …)." },
  { value: "returns", label: "Returns", hint: "Returns flow: `operations` + returns feature, or a `returns` module if present." },
  { value: "finance", label: "Finance", hint: "`finance` module (e.g. settlements)." },
  { value: "settings", label: "Settings", hint: "Tenant / system `settings` module." },
  { value: "platform", label: "Platform", hint: "Platform admin `platform` module." },
  { value: "account", label: "Account & admin", hint: "Tenant admin, imports, tech debug." },
];

/** `modules.key` OR-sets for report groups (except `operations` / `returns`, which are special-cased). */
const REPORT_GROUP_TO_MODULE_KEYS: Record<string, string[]> = {
  warehouse: ["wms"],
  claims: ["claims"],
  finance: ["finance"],
  settings: ["settings"],
  platform: ["platform"],
  account: ["tenant_admin", "tech_debug"],
};

/**
 * @returns `true` = row is included. Empty / null `groupId` = no filter (all groups).
 * Unknown `groupId` → no rows match.
 */
/**
 * Access Management: filter which top-level module nodes appear (same coarse areas as the report).
 * Empty / null `scope` = show all modules.
 */
export function accessTreeModuleMatchesScope(scope: string | null | undefined, moduleKey: string): boolean {
  if (!scope?.trim()) return true;
  const g = scope.trim().toLowerCase();
  const mk = (moduleKey ?? "").trim().toLowerCase();
  if (g === "operations") return mk === "operations";
  if (g === "returns") return mk === "returns" || mk === "operations";
  if (g === "warehouse") return mk === "wms";
  if (g === "claims") return mk === "claims";
  if (g === "finance") return mk === "finance";
  if (g === "settings") return mk === "settings";
  if (g === "platform") return mk === "platform";
  if (g === "account") return mk === "tenant_admin" || mk === "tech_debug";
  return true;
}

export function rowMatchesReportModuleGroup(
  groupId: string | null | undefined,
  moduleKey: string,
  featureKey: string,
  _featureName: string,
): boolean {
  if (!groupId?.trim()) return true;
  const g = groupId.trim().toLowerCase();
  const mk = (moduleKey ?? "").trim().toLowerCase();
  const fk = (featureKey ?? "").trim().toLowerCase();

  if (g === "operations") {
    return mk === "operations";
  }
  if (g === "returns") {
    if (mk === "returns") return true;
    return mk === "operations" && (fk === "returns" || fk.startsWith("returns."));
  }

  const keys = REPORT_GROUP_TO_MODULE_KEYS[g];
  if (!keys || keys.length === 0) return false;
  return keys.some((k) => k.toLowerCase() === mk);
}
