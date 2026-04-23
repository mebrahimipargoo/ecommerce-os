import type { ModuleFeatureTreeNode } from "./access-entitlements";

/**
 * UI-only slices for the Access Management "Settings" tab (same catalog, no new permission model).
 * Keys align with public.modules + public.module_features (see 20260630120000 migration).
 */
export function filterToSingleModuleFeature(
  tree: ModuleFeatureTreeNode[],
  moduleKey: string,
  featureKey: string,
): ModuleFeatureTreeNode[] {
  const mk = String(moduleKey ?? "").trim().toLowerCase();
  const fk = String(featureKey ?? "").trim().toLowerCase();
  if (!mk || !fk) return [];
  for (const node of tree) {
    if (String(node.module.key).trim().toLowerCase() !== mk) continue;
    const features = node.features.filter((b) => String(b.featureKey).trim().toLowerCase() === fk);
    if (features.length === 0) return [];
    return [{ ...node, features }];
  }
  return [];
}

export const SYSTEM_SETTINGS_SLICES: { title: string; moduleKey: string; featureKey: string }[] = [
  { title: "Stores & Adapters", moduleKey: "settings", featureKey: "stores" },
  { title: "Users", moduleKey: "settings", featureKey: "users" },
];

export const PLATFORM_SETTINGS_SLICES: { title: string; moduleKey: string; featureKey: string }[] = [
  { title: "Product Branding", moduleKey: "platform", featureKey: "branding" },
  { title: "Organizations", moduleKey: "platform", featureKey: "organizations" },
  { title: "Platform Users", moduleKey: "platform", featureKey: "users" },
  { title: "Access Management", moduleKey: "platform", featureKey: "access" },
];

/** Report / Access Management: filter rows by the known “Settings Access” feature pairs. */
export type AccessReportSettingsScope =
  | "all"
  | "settings_only"
  | "system_settings"
  | "platform_settings"
  | "non_settings";

/**
 * `system` = System Settings tab pair; `platform` = Platform Settings tab pair; `null` = not a listed settings feature.
 */
export function matchSettingsFeatureScope(moduleKey: string, featureKey: string): "system" | "platform" | null {
  const mk = String(moduleKey ?? "").trim().toLowerCase();
  const fk = String(featureKey ?? "").trim().toLowerCase();
  for (const s of SYSTEM_SETTINGS_SLICES) {
    if (s.moduleKey.toLowerCase() === mk && s.featureKey.toLowerCase() === fk) return "system";
  }
  for (const s of PLATFORM_SETTINGS_SLICES) {
    if (s.moduleKey.toLowerCase() === mk && s.featureKey.toLowerCase() === fk) return "platform";
  }
  return null;
}
