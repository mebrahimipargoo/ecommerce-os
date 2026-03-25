-- Adds photo_evidence JSONB column to returns table
-- Stores a map of category_id → photo_count, e.g.:
--   { "shipping_label": 2, "outer_box": 1, "damage_closeup": 3 }
-- Run in Supabase SQL Editor: Dashboard > SQL Editor > New query

alter table public.returns
  add column if not exists photo_evidence jsonb default null;

-- Update status column default to reflect the new status lifecycle:
--   'received'         – sellable returns, no claim needed
--   'pending_evidence' – claim issues without complete photo evidence
--   'ready_for_claim'  – claim issues with all required photo evidence captured
--   'processing'       – claim submitted to Amazon
--   'completed'        – claim resolved/reimbursed
--   'flagged'          – requires manual review
alter table public.returns
  alter column status set default 'pending_evidence';

-- Back-fill existing rows that have no photo_evidence as 'received'
update public.returns
  set status = 'received'
  where photo_evidence is null and status = 'received';

comment on column public.returns.photo_evidence is
  'JSONB map of photo category id to count of uploaded photos.
   Populated by the warehouse returns wizard on submission.
   Null for sellable returns that require no evidence.';
