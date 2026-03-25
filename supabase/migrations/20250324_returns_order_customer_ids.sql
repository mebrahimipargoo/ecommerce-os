-- Traceability fields for marketplace API / customer return scoring (nullable)
alter table public.returns add column if not exists order_id text null;
alter table public.returns add column if not exists customer_id text null;

comment on column public.returns.order_id is 'External marketplace order id — lifecycle & API sync.';
comment on column public.returns.customer_id is 'External customer id — return scoring & CRM.';
