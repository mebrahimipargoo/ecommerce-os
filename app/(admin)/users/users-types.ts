/** One row from the `profiles` table (email from `auth.users` when available). */
export type ProfileRow = {
  id: string;
  organization_id: string | null;
  /** Resolved via a single Supabase join — no secondary fetch needed. */
  company_name: string | null;
  full_name: string | null;
  email: string;
  role: string | null;
  photo_url: string | null;
  created_at: string;
  updated_at: string | null;
};
