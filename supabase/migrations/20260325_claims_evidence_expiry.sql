-- Claims Evidence Photos & Expiry Tracking
-- Adds dedicated photo-URL columns for damage-claim evidence on both packages
-- and items, plus expiry_date at the item level for FEFO tracking.
--
-- Run in Supabase SQL Editor — idempotent (ADD COLUMN IF NOT EXISTS).

-- ── Packages: closed-box and opened-box claim evidence ────────────────────────
alter table public.packages
  add column if not exists photo_closed_url text default null,
  add column if not exists photo_opened_url text default null;

comment on column public.packages.photo_closed_url is
  'Claim evidence: photo of the box in its original closed/sealed state.
Used to document the condition at receipt before opening.';

comment on column public.packages.photo_opened_url is
  'Claim evidence: photo of the box after opening.
Used to document contents, damage, or discrepancies found inside.';

-- ── Returns / Items: per-item evidence and FEFO expiry tracking ───────────────
alter table public.returns
  add column if not exists photo_item_url   text default null,
  add column if not exists photo_expiry_url text default null;

-- NOTE: expiry_date for FEFO is stored in the existing `expiration_date` (date) column.
-- No new column is needed — expiration_date already exists on the returns table.

comment on column public.returns.photo_item_url is
  'Claim evidence: general item condition photo.
Documents the physical state of the returned item.';

comment on column public.returns.photo_expiry_url is
  'Claim evidence: close-up photo of the expiry date label on the item.
Required when expiration_date is set, to verify the printed date.';

-- ── Optional: fast lookup for expiry-based FEFO picking ─────────────────────
create index if not exists idx_returns_expiry
  on public.returns (expiration_date)
  where expiration_date is not null;
