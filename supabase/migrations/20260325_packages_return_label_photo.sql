-- Add return-label photo URL to the packages claim-evidence set.
-- Idempotent — safe to run multiple times.

alter table public.packages
  add column if not exists photo_return_label_url text default null;

comment on column public.packages.photo_return_label_url is
  'Claim evidence: photo of the return shipping label attached to the box.
Documents the carrier label, tracking number, and RMA printed on the package.';

-- Confirm LPN (lpn column on returns) is strictly nullable / optional.
-- No schema change needed — the column was created with DEFAULT NULL.
-- This comment documents the intent explicitly:
comment on column public.returns.lpn is
  'Return label / LPN barcode (strictly OPTIONAL).
Populated only for orphaned items not assigned to a tracked package.
Operators must never be blocked from saving an item due to a missing LPN.';
