-- Multi-condition support: replaces the single "condition" text column with a
-- "conditions" text[] column that can store multiple simultaneous issue types.
-- e.g. ['empty_box', 'customer_damaged'] or ['fifo_expired', 'wrong_item']
--
-- Run in Supabase SQL Editor: Dashboard > SQL Editor > New query

-- 1. Add the new array column
alter table public.returns
  add column if not exists conditions text[] not null default '{}';

-- 2. Migrate existing single-condition data into the array (idempotent)
update public.returns
  set conditions = array[condition]
  where condition is not null
    and condition <> ''
    and conditions = '{}';

-- 3. GIN index for fast "does this row contain issue X?" queries
--    e.g. WHERE 'empty_box' = ANY(conditions)
create index if not exists idx_returns_conditions
  on public.returns using gin (conditions);

-- 4. (Optional) keep the old condition column for backward compat,
--    or drop it after verifying the migration:
-- alter table public.returns drop column if exists condition;

comment on column public.returns.conditions is
  'Array of issue types present on this return.
   Valid values: sellable | wrong_item | empty_box | customer_damaged | fifo_expired
   A single return may have multiple simultaneous conditions
   (e.g. empty_box + customer_damaged).
   Admin-configurable SLA rules per condition are planned for a future sprint.';
