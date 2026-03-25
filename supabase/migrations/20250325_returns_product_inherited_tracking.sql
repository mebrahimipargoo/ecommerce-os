-- Product identity (ASIN/UPC/FNSKU) — primary identifier at item level
alter table public.returns add column if not exists product_identifier text null;
comment on column public.returns.product_identifier is 'ASIN / UPC / FNSKU — product barcode. LPN lives on package flow.';

-- Denormalized snapshot when item is linked to a package (inherit package tracking for reporting/API)
alter table public.returns add column if not exists inherited_tracking_number text null;
alter table public.returns add column if not exists inherited_carrier text null;
comment on column public.returns.inherited_tracking_number is 'Copied from linked package at insert/update for traceability.';
comment on column public.returns.inherited_carrier is 'Copied from linked package at insert/update.';

-- Optional BoL image on pallets (Bill of Lading)
alter table public.pallets add column if not exists bol_photo_url text null;
comment on column public.pallets.bol_photo_url is 'Optional Bill of Lading scan.';
