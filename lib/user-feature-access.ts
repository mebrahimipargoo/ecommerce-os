import type { UiAccessLevel } from "./access-level";

export type UserOverrideDbLevel = "none" | "read" | "write";

/** UI + wire: `inherit` = no row in `user_feature_access_overrides` */
export type UserOverrideChoice = "inherit" | UserOverrideDbLevel;

export type UserFeatureAccessRow = {
  module_feature_id: string;
  baseline: UiAccessLevel;
  override: UserOverrideChoice;
  effective: UiAccessLevel;
};

const RANK: Record<UiAccessLevel, number> = {
  none: 0,
  read: 1,
  write: 2,
  manage: 3,
};

/** Max of two levels (for baseline = max(role, group)). */
export function maxAccessLevel(a: UiAccessLevel, b: UiAccessLevel): UiAccessLevel {
  return RANK[a] >= RANK[b] ? a : b;
}

/**
 * Per-feature user effective access: org gates first, then user override replaces baseline when a row exists.
 * Override may only be none | read | write (not manage at user level).
 */
export function effectiveUserFeatureAfterOverride(args: {
  orgEntitled: boolean;
  baseline: UiAccessLevel;
  override: UserOverrideChoice;
}): UiAccessLevel {
  if (!args.orgEntitled) {
    return "none";
  }
  if (args.override === "inherit") {
    return args.baseline;
  }
  if (args.override === "none") {
    return "none";
  }
  if (args.override === "read") {
    return "read";
  }
  return "write";
}

export function isUserOverrideLevel(v: string): v is UserOverrideDbLevel {
  return v === "none" || v === "read" || v === "write";
}
