-- Adds expiration date and batch/lot tracking to the returns table.
-- Required for Amazon FBA FIFO violation claims (groceries, supplements, etc.)
-- Run in Supabase SQL Editor: Dashboard > SQL Editor > New query

-- expiration_date: the date printed on the product packaging
alter table public.returns
  add column if not exists expiration_date date default null;

-- batch_number: manufacturer lot / batch / production code
alter table public.returns
  add column if not exists batch_number text default null;

-- Index for expiry-date range queries (e.g. "all returns expiring in Q1 2024")
create index if not exists idx_returns_expiry
  on public.returns (organization_id, expiration_date)
  where expiration_date is not null;

comment on column public.returns.expiration_date is
  'Expiration date printed on the product. Populated only for FIFO violation returns.
   Amazon FBA is liable for reimbursement when it allows older stock to expire
   in-fulfillment-center instead of shipping it first (First-In First-Out rule).';

comment on column public.returns.batch_number is
  'Manufacturer batch / lot number. Paired with expiration_date to build
   bulletproof FIFO violation claims against Amazon.';
