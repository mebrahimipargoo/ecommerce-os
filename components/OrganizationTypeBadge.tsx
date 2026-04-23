export type OrganizationTypeBadgeProps = {
  type: "internal" | "tenant";
};

/**
 * Compact pill label for `organizations.type` in platform / admin tables.
 */
export function OrganizationTypeBadge({ type }: OrganizationTypeBadgeProps) {
  if (type === "internal") {
    return (
      <span className="inline-flex shrink-0 rounded-full bg-purple-500/10 px-2 py-0.5 text-xs font-medium text-purple-400">
        Internal
      </span>
    );
  }
  return (
    <span className="inline-flex shrink-0 rounded-full bg-green-500/10 px-2 py-0.5 text-xs font-medium text-green-400">
      Tenant
    </span>
  );
}
