/** One `user_groups` row joined to `groups` for display on the Users admin page. */
export type UserGroupAssignment = {
  user_group_id: string;
  group_id: string;
  key: string;
  name: string;
};

/** One row from `public.groups` for pickers. */
export type OrgGroupRow = {
  id: string;
  key: string;
  name: string;
  description: string | null;
};

/** One row from the `profiles` table (email from `auth.users` when available). */
export type ProfileRow = {
  id: string;
  organization_id: string | null;
  /** Resolved via a single Supabase join — no secondary fetch needed. */
  company_name: string | null;
  full_name: string | null;
  email: string;
  /** Canonical `roles.key` (preferred) or legacy `profiles.role` text. */
  role: string | null;
  /** `roles.name` from the catalog join when available. */
  role_display_name: string | null;
  /** From `roles.scope` when `role_id` join resolves; null for legacy rows. */
  role_scope: "system" | "tenant" | null;
  photo_url: string | null;
  created_at: string;
  updated_at: string | null;
  /** Filled by the Users page from `public.user_groups` + `public.groups` after listing profiles. */
  assigned_groups?: UserGroupAssignment[];
};
